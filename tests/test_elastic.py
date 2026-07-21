"""Tests for the elastic worker loop (leat.elastic).

The elastic model: anonymous workers claim contiguous offset-range buckets from a
shared LocalClaimStore, process exactly-once (offset rides the sink commit),
survive death (reclaim from the SINK offset), and self-balance by process count.

We use threads for speed/determinism, but each worker thread opens its OWN
LocalClaimStore on the SAME sqlite file — identical to separate processes (SQLite
connections are per-thread anyway), so this genuinely exercises cross-connection
CAS coordination. The full multiprocess proof lives in examples/elastic_demo.py.
"""
import os
import shutil
import tempfile
import threading
import time

import numpy as np
import pyarrow as pa
import polars as pl
import pytest

import leat
from leat import run_worker, LocalClaimStore
from leat.elastic import _buckets


N_ROWS = 60_000
NUM_BUCKETS = 6


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("value") > 100)


@pytest.fixture
def env(tmp_path):
    wh = os.path.join(tempfile.gettempdir(),
                      f"pxleat_el_{os.getpid()}_{int(time.time()*1000)}")
    shutil.rmtree(wh, ignore_errors=True)
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    events = lt.create("db.events", schema)
    lt.create("db.silver", schema)
    events.append(pa.table({
        "_offset": np.arange(N_ROWS, dtype=np.int64),
        "value": np.random.default_rng(0).integers(0, 200, N_ROWS, dtype=np.int64),
    }))
    claims_db = os.path.join(wh, "claims.db")
    yield lt, claims_db
    shutil.rmtree(wh, ignore_errors=True)


def _expected(lt) -> pl.DataFrame:
    whole = pl.from_arrow(lt.source("db.events").read_all())
    return transform(whole).sort("_offset")


def _assert_exactly_once(lt):
    snk = lt.source("db.silver")
    got = pl.from_arrow(snk.read_all()).sort("_offset")
    exp = _expected(lt)
    assert got.height == exp.height, f"row count {got.height} != {exp.height}"
    assert got["_offset"].to_list() == exp["_offset"].to_list()
    # no duplicates -> exactly-once
    assert got["_offset"].n_unique() == got.height
    return got


# --------------------------------------------------------------------------- #
def test_bucket_plan_is_contiguous_and_covers_range():
    b = _buckets(until=99, num_buckets=4, name="t")
    # contiguous, adjacent, covers (-1 .. 99] with no gap/overlap
    assert b[0][1] == -1
    assert b[-1][2] == 99
    for i in range(1, len(b)):
        assert b[i][1] == b[i - 1][2]  # lo_i == hi_{i-1}
    keys = [k for (k, _, _) in b]
    assert keys == ["t.bucket0", "t.bucket1", "t.bucket2", "t.bucket3"]


def test_single_worker_exactly_once(env):
    lt, claims_db = env
    src, snk = lt.source("db.events"), lt.source("db.silver")
    store = LocalClaimStore(claims_db)
    stats = run_worker(src, snk, transform, name="single",
                       num_buckets=NUM_BUCKETS, claim_store=store,
                       worker="solo", batch_rows=5_000)
    store.close()
    got = _assert_exactly_once(lt)
    assert len(stats["buckets_completed"]) == NUM_BUCKETS
    assert got.height > 0


def test_multiple_workers_partition_and_exactly_once(env):
    lt, claims_db = env
    results = {}

    def work(wid):
        # each worker opens its OWN store on the shared file (like a process)
        store = LocalClaimStore(claims_db)
        src = leat.IcebergFormat(lt.catalog, "db.events")
        snk = leat.IcebergFormat(lt.catalog, "db.silver")
        try:
            results[wid] = run_worker(src, snk, transform, name="multi",
                                      num_buckets=NUM_BUCKETS, claim_store=store,
                                      worker=wid, batch_rows=4_000, ttl=30.0)
        finally:
            store.close()

    threads = [threading.Thread(target=work, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)

    _assert_exactly_once(lt)

    # buckets were partitioned across workers with no double-completion
    completed = [b for r in results.values() for b in r["buckets_completed"]]
    assert sorted(completed) == sorted(set(completed))  # each completed once
    assert set(completed) == {f"multi.bucket{i}" for i in range(NUM_BUCKETS)}
    # more than one worker actually did work (self-balancing)
    active = [w for w, r in results.items() if r["buckets_worked"]]
    assert len(active) >= 2


def test_scale_up(env):
    """Start 2 workers, add 3 more mid-flight; all buckets complete exactly-once."""
    lt, claims_db = env
    results = {}
    threads = []

    def work(wid):
        store = LocalClaimStore(claims_db)
        src = leat.IcebergFormat(lt.catalog, "db.events")
        snk = leat.IcebergFormat(lt.catalog, "db.silver")
        try:
            results[wid] = run_worker(src, snk, transform, name="scale",
                                      num_buckets=NUM_BUCKETS, claim_store=store,
                                      worker=wid, batch_rows=2_000, ttl=30.0)
        finally:
            store.close()

    for i in range(2):
        t = threading.Thread(target=work, args=(f"early{i}",))
        t.start()
        threads.append(t)
    time.sleep(0.05)  # let the first two grab buckets
    for i in range(3):
        t = threading.Thread(target=work, args=(f"late{i}",))
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=120)

    _assert_exactly_once(lt)
    completed = [b for r in results.values() for b in r["buckets_completed"]]
    assert set(completed) == {f"scale.bucket{i}" for i in range(NUM_BUCKETS)}


def test_failover_reclaims_from_sink_offset(env):
    """A worker dies after committing one chunk of a bucket; a second worker
    reclaims the bucket (expired lease) and finishes from the SINK offset — no
    double-count."""
    lt, claims_db = env
    src, snk = lt.source("db.events"), lt.source("db.silver")

    # A short TTL so the dead worker's lease lapses quickly for the reclaimer.
    TTL = 1.0
    NB = 3

    # Worker A: a sink wrapper that lets the FIRST chunk commit atomically, then
    # dies BEFORE the second chunk's commit -> a genuine mid-bucket process death
    # (no half-write: chunk 2 never lands, its offset never advances). We must NOT
    # raise *after* a successful inner.append, else the runner's legitimate
    # concurrent-commit retry would re-append the same (already committed) chunk.
    class DyingSink:
        def __init__(self, inner):
            self.inner = inner
            self.appends = 0
        def latest_offset(self):
            return self.inner.latest_offset()
        def read_offsets(self):
            return self.inner.read_offsets()
        def read_all(self):
            return self.inner.read_all()
        def append(self, data, offsets=None):
            self.appends += 1
            if self.appends >= 2:
                # crash before committing chunk 2 -> exactly-once boundary
                raise RuntimeError("worker A crashed mid-bucket")
            self.inner.append(data, offsets=offsets)

    store_a = LocalClaimStore(claims_db)
    dying = DyingSink(leat.IcebergFormat(lt.catalog, "db.silver"))
    with pytest.raises(RuntimeError):
        run_worker(src, dying, transform, name="fo", num_buckets=NB,
                   claim_store=store_a, worker="A", batch_rows=3_000, ttl=TTL)

    # A committed exactly one chunk (offset advanced) but did not complete a bucket.
    off_after_a = snk.read_offsets()
    assert any(v >= 0 for v in off_after_a.values()), "A should have advanced one offset"
    # A holds a lease that must lapse; wait past TTL.
    time.sleep(TTL + 0.5)
    store_a.close()

    # Worker B: healthy, finishes everything, reclaiming A's partial bucket from
    # the SINK offset.
    store_b = LocalClaimStore(claims_db)
    statsb = run_worker(src, snk, transform, name="fo", num_buckets=NB,
                        claim_store=store_b, worker="B", batch_rows=3_000, ttl=30.0)
    store_b.close()

    _assert_exactly_once(lt)
    assert len(statsb["buckets_completed"]) >= 1
    # B should have reclaimed the bucket A partially filled (resume > lo)
    assert statsb["reclaimed"], "B should have detected a reclaim from the sink offset"
