"""Delta Lake hardening — prove the Delta path is a first-class format, at
feature parity with Iceberg.

This ADDS coverage on top of tests/test_delta.py, test_atomic_checkpoint.py and
test_ergonomics.py (which already cover the basic Consumer drive, the atomic
sink-checkpoint contrast, and low-level auto-mint). Here we harden:

  1. Incremental read semantics: multi-append, two-sided `read_since(offset, hi=)`
     range read, monotonic offset advance, empty/single-row/None edge cases.
  2. Exactly-once via atomic sink checkpoint ON DELTA: crash-between-append-and-
     checkpoint proven safe with SinkCheckpointStore (dup with a JSON store),
     multi-key newest-first history scan recovers per key.
  3. Auto-mint + invisible offset: mints when absent & schema has it; does NOT
     mint into a PRE-EXISTING dimension table lacking `_offset` (the delta guard
     `_t.schema().to_arrow().names`); explicit `_offset` honored.
  4. Pipeline / @lt.model end-to-end on Delta (format objects driven through the
     real Session/Pipeline, checkpoint_mode="sink") — silver filter runs
     incrementally exactly-once; read back business columns only.
  5. Elastic worker on Delta: run_worker + LocalClaimStore over a Delta src+sink,
     buckets processed exactly-once, resume-from-sink works.
  6. Iceberg<->Delta parity: same seeded data + same transform through BOTH
     formats -> identical row counts / aggregates.
  7. Edge cases: strings/nulls, multi-file appends, reopen in a fresh DeltaFormat
     (simulating a fresh process) and continue.

No-space temp paths only (delta-rs + Windows); paths use forward slashes.
"""
import os
import shutil
import tempfile

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from leat import (
    DeltaFormat, connect, session, run_worker, LocalClaimStore,
)
from leat.consumer import Consumer
from leat.checkpoint import JsonCheckpointStore, SinkCheckpointStore

deltalake = pytest.importorskip("deltalake")

THRESHOLD = 100
NAME = "silver_clean"


def _rows(start, n, seed):
    off = np.arange(start, start + n, dtype=np.int64)
    val = np.random.default_rng(seed).integers(0, 200, n, dtype=np.int64)
    return pa.table({"_offset": off, "value": val})


def _rows_no_off(n, seed):
    val = np.random.default_rng(seed).integers(0, 200, n, dtype=np.int64)
    return pa.table({"value": val})


def _keep(arrow):
    return pc.filter(arrow, pc.greater(arrow.column("value"), THRESHOLD))


def transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("value") > THRESHOLD)


@pytest.fixture
def base():
    d = tempfile.mkdtemp(prefix="leat_delta_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


# =========================================================================== #
# 1. Incremental read semantics
# =========================================================================== #
def test_read_since_empty_and_none_edge_cases(base):
    t = DeltaFormat(f"{base}/events")
    # Empty (no _delta_log yet): bounds are None, read returns empty, offset kept.
    assert t.earliest_offset() is None
    assert t.latest_offset() is None
    data, off = t.read_since(None)
    assert data.num_rows == 0 and off is None
    data, off = t.read_since(5)
    assert data.num_rows == 0 and off == 5           # offset passed through unchanged

    # Single row.
    t.append(_rows(0, 1, seed=0))
    assert t.earliest_offset() == 0 and t.latest_offset() == 0
    data, off = t.read_since(None)
    assert data.num_rows == 1 and off == 0
    # read_since(latest) -> nothing new, offset unchanged.
    data, off = t.read_since(0)
    assert data.num_rows == 0 and off == 0


def test_read_since_none_returns_all_and_monotonic(base):
    t = DeltaFormat(f"{base}/events")
    t.append(_rows(0, 100, seed=1))
    t.append(_rows(100, 100, seed=2))
    t.append(_rows(200, 50, seed=3))

    # read_since(None) returns ALL rows.
    allrows, off = t.read_since(None)
    assert allrows.num_rows == 250
    assert off == 249
    assert t.earliest_offset() == 0 and t.latest_offset() == 249

    # Monotonic incremental walk in bounded windows: each step reads a fresh,
    # strictly-advancing offset range, never re-reading prior rows.
    cur = None
    got = 0
    prev = -1
    for hi in (99, 199, 249):
        data, cur = t.read_since(cur, hi=hi)
        assert data.num_rows > 0
        mx = pc.max(data.column("_offset")).as_py()
        mn = pc.min(data.column("_offset")).as_py()
        assert mn > prev                              # strictly beyond last window
        assert mx == hi                               # bounded exactly to hi
        prev = mx
        got += data.num_rows
    assert got == 250


def test_read_since_two_sided_range(base):
    """The elastic two-sided range read: offset < _offset <= hi prunes both ends."""
    t = DeltaFormat(f"{base}/events")
    t.append(_rows(0, 300, seed=7))

    data, off = t.read_since(99, hi=199)
    offs = data.column("_offset").to_pylist()
    assert min(offs) == 100 and max(offs) == 199
    assert data.num_rows == 100
    assert off == 199

    # hi with no lower bound: everything <= hi.
    data, off = t.read_since(None, hi=49)
    assert data.column("_offset").to_pylist() == list(range(0, 50))
    assert off == 49

    # Empty window (lo == hi range with nothing) -> offset stays the input lo.
    data, off = t.read_since(199, hi=199)
    assert data.num_rows == 0
    assert off == 199


# =========================================================================== #
# 2. Exactly-once via atomic sink checkpoint ON DELTA
# =========================================================================== #
def test_delta_json_store_duplicates_on_crash(base):
    """Contrast: JSON store leaves a crash window -> duplicates on Delta."""
    source = DeltaFormat(f"{base}/events")
    sink = DeltaFormat(f"{base}/silver")
    source.append(_rows(0, 5_000, seed=0))
    ckpt = JsonCheckpointStore(f"{base}/offsets.json")

    c1 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    batch = c1.poll()
    kept = _keep(batch.arrow())
    sink.append(kept)
    del c1                                            # crash before ckpt.set()

    assert ckpt.get(NAME) is None
    c2 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    sink.append(_keep(c2.poll().arrow()))             # re-processes same rows
    assert sink.read_all().num_rows == 2 * kept.num_rows   # DUPLICATES


def test_delta_sink_store_no_duplicates_on_crash(base):
    """The fix: offset embedded in the Delta commit -> no window -> exactly-once."""
    source = DeltaFormat(f"{base}/events")
    sink = DeltaFormat(f"{base}/silver")
    source.append(_rows(0, 5_000, seed=0))
    ckpt = SinkCheckpointStore(sink)

    c1 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    batch = c1.poll()
    kept = _keep(batch.arrow())
    sink.append(kept, offsets={NAME: batch.offset})   # ONE atomic Delta commit
    del c1                                             # crash

    # Recover start from the Delta commit metadata (newest-first history scan).
    assert ckpt.get(NAME) == batch.offset == 4_999
    c2 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    assert c2.position() == 4_999
    assert c2.poll() is None                           # nothing to re-process

    src_all = source.read_all()
    truth = pc.sum(pc.greater(src_all.column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert sink.read_all().num_rows == truth == kept.num_rows


def test_delta_multi_key_history_scan(base):
    """Two consumer names -> one sink; each commit carries only its own key.
    Newest-first Delta history() scan recovers the latest per key."""
    sink = DeltaFormat(f"{base}/silver")
    sink.append(_rows(0, 10, seed=1), offsets={"a": 100})
    sink.append(_rows(10, 10, seed=2), offsets={"b": 200})
    sink.append(_rows(20, 10, seed=3), offsets={"a": 300})
    sink.append(_rows(30, 10, seed=4), offsets={"b": 400})
    sink.append(_rows(40, 10, seed=5), offsets={"a": 500})

    assert sink.read_offsets() == {"a": 500, "b": 400}
    ck = SinkCheckpointStore(sink)
    assert ck.get("a") == 500 and ck.get("b") == 400 and ck.get("missing") is None


def test_delta_read_offsets_empty_and_offsetless(base):
    sink = DeltaFormat(f"{base}/empty")
    assert sink.read_offsets() == {}                   # no _delta_log
    sink.append(_rows(0, 5, seed=0))                   # plain append, no offsets
    assert sink.read_offsets() == {}


# =========================================================================== #
# 3. Auto-mint + invisible offset
# =========================================================================== #
def test_delta_auto_mint_when_absent_and_continues(base):
    t = DeltaFormat(f"{base}/events")
    t.append(_rows_no_off(3, seed=0))                  # no _offset -> mint 0..2
    assert t.earliest_offset() == 0 and t.latest_offset() == 2
    t.append(_rows_no_off(2, seed=1))                  # continues 3..4
    assert t.latest_offset() == 4
    got = pl.from_arrow(t.read_all()).sort("_offset")
    assert got["_offset"].to_list() == [0, 1, 2, 3, 4]


def test_delta_explicit_offset_honored(base):
    t = DeltaFormat(f"{base}/events")
    t.append(pa.table({"_offset": pa.array([100, 101], pa.int64()), "value": [1, 2]}))
    assert t.earliest_offset() == 100 and t.latest_offset() == 101


def test_delta_dimension_table_not_minted(base):
    """A PRE-EXISTING Delta table created WITHOUT `_offset` (dimension table) must
    NOT get an `_offset` injected — the guard `_t.schema().to_arrow().names`
    (mirrors the Iceberg fix). This is the delta-rs analogue of Iceberg's
    `_off not in schema.column_names`."""
    from deltalake import write_deltalake, DeltaTable

    dim_uri = f"{base}/dim"
    # Pre-create WITHOUT _offset, exactly as lt.create would for a dim table.
    write_deltalake(dim_uri, pa.table({"country": ["US", "GB"], "rate": [1.0, 0.8]}),
                    mode="append")
    assert "_offset" not in DeltaTable(dim_uri).schema().to_arrow().names

    dim = DeltaFormat(dim_uri)
    dim.append(pa.table({"country": ["DE"], "rate": [0.9]}))   # guard must skip mint
    names = DeltaTable(dim_uri).schema().to_arrow().names
    assert "_offset" not in names                      # NOT injected
    assert dim.read_all().num_rows == 3


# =========================================================================== #
# 4. Pipeline / @lt.model end-to-end on Delta (format objects, sink checkpoint)
# =========================================================================== #
def test_delta_pipeline_end_to_end_exactly_once(base):
    """Drive the real Session/Pipeline with Delta format objects + sink checkpoint:
    incremental exactly-once silver filter, crash+resume, business-only read-back."""
    src = DeltaFormat(f"{base}/events")
    snk = DeltaFormat(f"{base}/silver")
    src.append(_rows(0, 20_000, seed=7))

    lt = session(catalog=None,
                 checkpoint=JsonCheckpointStore(f"{base}/unused.json"),
                 checkpoint_mode="sink")
    p1 = lt.pipeline(NAME, src, snk, transform, start="earliest")
    assert isinstance(p1.consumer._ckpt, SinkCheckpointStore)
    n1 = p1.step()
    assert n1 == 20_000
    del p1

    truth = pc.sum(pc.greater(src.read_all().column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert snk.read_all().num_rows == truth

    # Resume from the SINK commit metadata — no reprocessing.
    p2 = lt.pipeline(NAME, src, snk, transform, start="earliest")
    assert p2.consumer.position() == 19_999
    assert p2.step() == 0
    assert snk.read_all().num_rows == truth            # still exactly-once

    # New data -> resumed pipeline processes ONLY the delta.
    src.append(_rows(20_000, 5_000, seed=8))
    p3 = lt.pipeline(NAME, src, snk, transform, start="earliest")
    assert p3.step() == 5_000
    new_truth = pc.sum(pc.greater(src.read_all().column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert snk.read_all().num_rows == new_truth

    # Business-only read-back (strip _offset like TableHandle.read).
    df = pl.from_arrow(snk.read_all())
    assert "_offset" in df.columns                     # sink carries leat-owned offset
    business = df.drop("_offset")
    assert business.columns == ["value"]


def test_delta_model_decorator_end_to_end(base):
    """@lt.model over Delta format objects: transform sees business-only columns,
    silver filter runs exactly-once."""
    src = DeltaFormat(f"{base}/events")
    snk = DeltaFormat(f"{base}/silver")
    src.append(pa.table({"_offset": np.arange(4, dtype=np.int64),
                         "user_id": [1, 2, 3, 4],
                         "value": [50, 150, 250, 90]}))

    lt = session(catalog=None,
                 checkpoint=JsonCheckpointStore(f"{base}/unused.json"),
                 checkpoint_mode="sink")
    seen = {}

    @lt.model(source=src, sink=snk, start="earliest")
    def silver(df):
        seen["cols"] = set(df.columns)                 # NO _offset at the boundary
        return df.filter(pl.col("value") > THRESHOLD)

    silver.run(once=True)
    assert seen["cols"] == {"user_id", "value"}
    out = pl.from_arrow(snk.read_all()).drop("_offset")
    assert sorted(out["value"].to_list()) == [150, 250]


# =========================================================================== #
# 5. Elastic worker on Delta
# =========================================================================== #
def _expected(src) -> pl.DataFrame:
    return transform(pl.from_arrow(src.read_all())).sort("_offset")


def test_delta_elastic_single_worker_exactly_once(base):
    N_ROWS, NUM_BUCKETS = 30_000, 5
    src = DeltaFormat(f"{base}/events")
    snk = DeltaFormat(f"{base}/silver")
    src.append(pa.table({"_offset": np.arange(N_ROWS, dtype=np.int64),
                         "value": np.random.default_rng(0).integers(0, 200, N_ROWS, dtype=np.int64)}))

    store = LocalClaimStore(f"{base}/claims.db")
    stats = run_worker(src, snk, transform, name="single",
                       num_buckets=NUM_BUCKETS, claim_store=store,
                       worker="solo", batch_rows=4_000)
    store.close()

    got = pl.from_arrow(snk.read_all()).sort("_offset")
    exp = _expected(src)
    assert got.height == exp.height > 0
    assert got["_offset"].to_list() == exp["_offset"].to_list()
    assert got["_offset"].n_unique() == got.height     # no duplicates
    assert len(stats["buckets_completed"]) == NUM_BUCKETS


def test_delta_elastic_resume_from_sink(base):
    """Run a worker to completion, then a second worker over the SAME buckets
    resumes from the SINK offset and does zero extra work (no double-write)."""
    N_ROWS, NUM_BUCKETS = 20_000, 4
    src = DeltaFormat(f"{base}/events")
    snk = DeltaFormat(f"{base}/silver")
    src.append(pa.table({"_offset": np.arange(N_ROWS, dtype=np.int64),
                         "value": np.random.default_rng(1).integers(0, 200, N_ROWS, dtype=np.int64)}))

    store1 = LocalClaimStore(f"{base}/claims.db")
    run_worker(src, snk, transform, name="resume", num_buckets=NUM_BUCKETS,
               claim_store=store1, worker="w1", batch_rows=3_000)
    store1.close()
    rows_after_first = snk.read_all().num_rows
    offs_after_first = snk.read_offsets()

    # A second, brand-new store+worker over the same sink: every bucket already
    # complete (sink offset >= hi), so it appends nothing.
    store2 = LocalClaimStore(f"{base}/claims2.db")
    stats2 = run_worker(src, snk, transform, name="resume", num_buckets=NUM_BUCKETS,
                        claim_store=store2, worker="w2", batch_rows=3_000)
    store2.close()

    assert snk.read_all().num_rows == rows_after_first   # no double-write
    assert snk.read_offsets() == offs_after_first
    assert stats2["rows_out"] == 0                        # nothing to do
    got = pl.from_arrow(snk.read_all()).sort("_offset")
    assert got["_offset"].n_unique() == got.height        # still no duplicates


# =========================================================================== #
# 6. Iceberg <-> Delta parity
# =========================================================================== #
def test_iceberg_delta_parity_counts_and_aggregates(base):
    """Same seeded data + same transform through BOTH formats -> identical row
    counts AND aggregate sums (format neutrality — the differentiator)."""
    data = _rows(0, 40_000, seed=42)

    # --- Delta ---
    d_src = DeltaFormat(f"{base}/d_events")
    d_snk = DeltaFormat(f"{base}/d_silver")
    d_src.append(data)
    _drain(d_src, d_snk, JsonCheckpointStore(f"{base}/d_off.json"), "d_clean")

    # --- Iceberg ---
    lt = connect(f"{base}/ice")
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    i_src = lt.create("db.events", schema)
    i_snk = lt.create("db.silver", schema)
    i_src.append(data)
    _drain(i_src, i_snk, lt.checkpoint, "i_clean")

    d_all = d_snk.read_all()
    i_all = i_snk.read_all()
    assert d_all.num_rows == i_all.num_rows > 0
    assert d_src.read_all().num_rows == i_src.read_all().num_rows == 40_000
    # Aggregates match, not just counts.
    assert pc.sum(d_all.column("value")).as_py() == pc.sum(i_all.column("value")).as_py()
    assert pc.min(d_all.column("value")).as_py() == pc.min(i_all.column("value")).as_py()
    assert pc.max(d_all.column("value")).as_py() == pc.max(i_all.column("value")).as_py()


def _drain(src, snk, ckpt, name):
    c = Consumer(src, name=name, checkpoint=ckpt, start="earliest")
    while (batch := c.poll()) is not None:
        snk.append(_keep(batch.arrow()))
        c.commit()


# =========================================================================== #
# 7. Edge cases
# =========================================================================== #
def test_delta_strings_and_nulls(base):
    t = DeltaFormat(f"{base}/events")
    t.append(pa.table({
        "name": pa.array(["alice", None, "bob", None]),
        "value": pa.array([150, None, 90, 250], pa.int64()),
    }))
    assert t.earliest_offset() == 0 and t.latest_offset() == 3
    all_rows = t.read_all()
    assert all_rows.num_rows == 4
    assert "_offset" in all_rows.column_names
    # Nulls preserved; filter on non-null values works through a Consumer.
    ck = JsonCheckpointStore(f"{base}/off.json")
    snk = DeltaFormat(f"{base}/silver")
    c = Consumer(t, name="s", checkpoint=ck, start="earliest")
    batch = c.poll()
    kept = pc.filter(batch.arrow(),
                     pc.greater(batch.arrow().column("value"), THRESHOLD))
    snk.append(kept)
    c.commit()
    out = pl.from_arrow(snk.read_all()).sort("value")
    assert out["value"].to_list() == [150, 250]        # nulls dropped by > filter


def test_delta_multi_file_append_incremental(base):
    """Several appends (multiple data files) drained incrementally by a Consumer;
    exactly-once, offsets contiguous across file boundaries."""
    src = DeltaFormat(f"{base}/events")
    snk = DeltaFormat(f"{base}/silver")
    total = 0
    for i in range(5):
        src.append(_rows(i * 1_000, 1_000, seed=i))    # 5 separate commits/files
        total += 1_000
    assert src.latest_offset() == total - 1

    ck = JsonCheckpointStore(f"{base}/off.json")
    c = Consumer(src, name="s", checkpoint=ck, start="earliest")
    kept_total = 0
    last = -1
    while (batch := c.poll()) is not None:
        assert batch.offset > last
        last = batch.offset
        kept = _keep(batch.arrow())
        snk.append(kept)
        c.commit()
        kept_total += kept.num_rows

    truth = pc.sum(pc.greater(src.read_all().column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert snk.read_all().num_rows == truth == kept_total
    assert c.position() == total - 1


def test_delta_reopen_fresh_process_and_continue(base):
    """Simulate a fresh process: a brand-new DeltaFormat on the same path resumes
    from committed sink metadata and continues incrementally — no duplicates."""
    uri_src = f"{base}/events"
    uri_snk = f"{base}/silver"

    # "Process 1": append + consume half, offset in sink metadata.
    src1 = DeltaFormat(uri_src)
    snk1 = DeltaFormat(uri_snk)
    src1.append(_rows(0, 3_000, seed=0))
    ck1 = SinkCheckpointStore(snk1)
    c1 = Consumer(src1, name=NAME, checkpoint=ck1, start="earliest")
    b1 = c1.poll()
    snk1.append(_keep(b1.arrow()), offsets={NAME: b1.offset})
    committed = b1.offset
    del src1, snk1, ck1, c1                              # process exits

    # More data arrives while "process 1" is gone.
    DeltaFormat(uri_src).append(_rows(3_000, 2_000, seed=1))

    # "Process 2": brand-new objects on the same paths.
    src2 = DeltaFormat(uri_src)
    snk2 = DeltaFormat(uri_snk)
    ck2 = SinkCheckpointStore(snk2)
    assert ck2.get(NAME) == committed                   # recovered from Delta commit
    c2 = Consumer(src2, name=NAME, checkpoint=ck2, start="earliest")
    assert c2.position() == committed
    b2 = c2.poll()
    assert b2 is not None
    snk2.append(_keep(b2.arrow()), offsets={NAME: b2.offset})

    truth = pc.sum(pc.greater(src2.read_all().column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert snk2.read_all().num_rows == truth            # exactly-once across "processes"
