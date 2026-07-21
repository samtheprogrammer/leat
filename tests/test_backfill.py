"""Tests for backfill mode (static sharding).

- Union of all shards == whole-table transform (no loss, no dupes); shards disjoint.
- Failover via an inline _MemoryClaimStore: a worker "dies" mid-shard, a second
  run reclaims from the bookmark and finishes exactly-once.
"""
import shutil
import tempfile
import time
import os

import numpy as np
import pyarrow as pa
import polars as pl
import pytest

import leat
from leat.backfill import Backfill


N_ROWS = 100_000
NUM_SHARDS = 4


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("value") > 100)


@pytest.fixture
def session(tmp_path):
    # no-space warehouse path required on Windows
    wh = os.path.join(tempfile.gettempdir(), f"pxleat_bf_{os.getpid()}_{int(time.time()*1000)}")
    shutil.rmtree(wh, ignore_errors=True)
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    events = lt.create("db.events", schema)
    lt.create("db.silver", schema)
    events.append(pa.table({
        "_offset": np.arange(N_ROWS, dtype=np.int64),
        "value": np.random.default_rng(0).integers(0, 200, N_ROWS, dtype=np.int64),
    }))
    yield lt
    shutil.rmtree(wh, ignore_errors=True)


def _expected(lt) -> pl.DataFrame:
    whole = pl.from_arrow(lt.source("db.events").read_all())
    return transform(whole).sort("_offset")


def test_shards_union_equals_whole_and_disjoint(session):
    lt = session
    src = lt.source("db.events")
    snk = lt.source("db.silver")

    bf = Backfill(src, snk, transform, num_shards=NUM_SHARDS, name="t1")
    assert bf.until == N_ROWS - 1

    total = 0
    for s in range(NUM_SHARDS):
        total += bf.run_shard(s)
    # every source row within [0, until] is covered exactly once across shards
    assert total == N_ROWS

    got = pl.from_arrow(snk.read_all()).sort("_offset")
    exp = _expected(lt)

    # no rows lost, no dupes
    assert got.height == exp.height
    assert got["_offset"].to_list() == exp["_offset"].to_list()
    # offsets unique -> disjoint shards
    assert got["_offset"].n_unique() == got.height

    # shards partition the offset space by modulo
    for s in range(NUM_SHARDS):
        assert bf.status()[s]["complete"] is True


def test_run_all_in_one_process(session):
    lt = session
    snk = lt.source("db.silver")
    bf = Backfill(lt.source("db.events"), snk, transform, num_shards=NUM_SHARDS, name="t2")
    bf.run("all")
    got = pl.from_arrow(snk.read_all()).sort("_offset")
    exp = _expected(lt)
    assert got["_offset"].to_list() == exp["_offset"].to_list()


def test_rerun_is_idempotent(session):
    lt = session
    snk = lt.source("db.silver")
    bf = Backfill(lt.source("db.events"), snk, transform, num_shards=NUM_SHARDS, name="t3")
    bf.run("all")
    rows_after_first = snk.read_all().num_rows
    # rerun: everything complete -> no new rows appended
    bf.run("all")
    assert snk.read_all().num_rows == rows_after_first


# --------------------------------------------------------------------------- #
# Inline ClaimStore for failover tests (mirrors leat.coordination.ClaimStore)
# --------------------------------------------------------------------------- #
class _Claim:
    def __init__(self, shard, worker, lease_expiry_ms, bookmark_offset, status):
        self.shard = shard
        self.worker = worker
        self.lease_expiry_ms = lease_expiry_ms
        self.bookmark_offset = bookmark_offset
        self.status = status


class _MemoryClaimStore:
    def __init__(self):
        self._c: dict = {}  # shard -> _Claim
        self._now = 0.0     # controllable virtual clock (ms)

    def _ms(self):
        return self._now

    def advance(self, seconds: float):
        self._now += seconds * 1000.0

    def claim(self, shard, worker, ttl):
        c = self._c.get(shard)
        if c is None:
            self._c[shard] = _Claim(shard, worker, self._ms() + ttl * 1000, None, "leased")
            return True
        if c.status == "done":
            return False
        if c.worker == worker:
            c.lease_expiry_ms = self._ms() + ttl * 1000
            return True
        # someone else holds it: only claimable if expired
        if c.lease_expiry_ms <= self._ms():
            c.worker = worker
            c.lease_expiry_ms = self._ms() + ttl * 1000
            c.status = "leased"
            return True
        return False

    def renew(self, shard, worker, ttl):
        c = self._c.get(shard)
        if c and c.worker == worker:
            c.lease_expiry_ms = self._ms() + ttl * 1000
            return True
        return False

    def bookmark(self, shard, worker, offset):
        c = self._c.get(shard)
        if c and c.worker == worker:
            c.bookmark_offset = offset

    def get(self, shard):
        return self._c.get(shard)

    def complete(self, shard, worker):
        c = self._c.get(shard)
        if c:
            c.status = "done"

    def release(self, shard, worker):
        c = self._c.get(shard)
        if c and c.worker == worker and c.status != "done":
            c.lease_expiry_ms = self._ms()  # immediately expired -> reclaimable

    def list_claims(self):
        return dict(self._c)

    def close(self):
        pass


def test_claim_store_failover(session):
    lt = session
    snk = lt.source("db.silver")
    store = _MemoryClaimStore()

    # Worker A: dies partway through shard 0 (raise after the first committed batch).
    orig_append = snk.append
    calls = {"n": 0}

    def flaky_append(data):
        calls["n"] += 1
        # let the first batch commit (so a bookmark is recorded), then crash.
        if calls["n"] == 2:
            raise RuntimeError("worker A crashed after first batch")
        orig_append(data)

    class _FlakySink:
        def append(self, data):
            flaky_append(data)

    # small batch so a shard needs multiple batches -> a crash is truly mid-shard
    bf_a = Backfill(lt.source("db.events"), _FlakySink(), transform,
                    num_shards=NUM_SHARDS, name="t4", claim_store=store,
                    worker="A", batch_rows=5_000, ttl=30.0)

    with pytest.raises(RuntimeError):
        bf_a.run_shard(0)

    # A committed at least one batch and recorded a bookmark, but shard 0 isn't done.
    c0 = store.get("t4:shard0")
    assert c0 is not None and c0.bookmark_offset is not None
    assert c0.status != "done"

    # A's lease expires (A is dead) -> shard becomes reclaimable.
    store.advance(60.0)

    # Worker B: healthy sink, runs "all" -> should reclaim shard 0 from bookmark
    # and finish every shard.
    bf_b = Backfill(lt.source("db.events"), snk, transform,
                    num_shards=NUM_SHARDS, name="t4", claim_store=store,
                    worker="B", batch_rows=5_000, ttl=30.0)
    bf_b.run("all")

    got = pl.from_arrow(snk.read_all()).sort("_offset")
    exp = _expected(lt)

    # complete + exactly-once despite the crash + reclaim
    assert got["_offset"].to_list() == exp["_offset"].to_list()
    assert got["_offset"].n_unique() == got.height

    for s in range(NUM_SHARDS):
        assert store.get(f"t4:shard{s}").status == "done"
