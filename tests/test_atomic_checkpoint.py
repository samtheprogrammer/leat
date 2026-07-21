"""Atomic sink-committed checkpointing — the linchpin of exactly-once.

Proves the fix BY CONTRAST:
  - JSON store (offset in a SEPARATE file): a crash between sink.append() and
    ckpt.set() → a fresh consumer re-reads the stale offset → DUPLICATE rows.
  - SinkCheckpointStore (offset embedded in the sink's OWN commit): the offset
    advances iff the append commits → no window → no duplicates, no gaps.

Also proves the newest→oldest history scan recovers the latest value per key
across many single-key commits (the per-partition multi-writer guarantee), and
drives the whole thing end-to-end through the real Pipeline with checkpoint="sink".

No-space warehouse/temp paths only (PyIceberg + delta-rs on Windows).
"""
import shutil
import tempfile

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from leat import DeltaFormat, connect
from leat.consumer import Consumer
from leat.checkpoint import JsonCheckpointStore, SinkCheckpointStore

deltalake = pytest.importorskip("deltalake")

N = 100_000
THRESHOLD = 100
NAME = "silver_clean"


def _rows(start, n, seed):
    off = np.arange(start, start + n, dtype=np.int64)
    val = np.random.default_rng(seed).integers(0, 200, n, dtype=np.int64)
    return pa.table({"_offset": off, "value": val})


def _keep(arrow):
    return pc.filter(arrow, pc.greater(arrow.column("value"), THRESHOLD))


@pytest.fixture
def base():
    d = tempfile.mkdtemp(prefix="pxleat_atomic_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


# --- format factories: return (source, sink) built the same way as production ---

def _delta_pair(base):
    return DeltaFormat(f"{base}/events"), DeltaFormat(f"{base}/silver")


def _iceberg_pair(base):
    lt = connect(f"{base}/ice")
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    return lt.create("db.events", schema), lt.create("db.silver", schema)


FORMATS = [("delta", _delta_pair), ("iceberg", _iceberg_pair)]


# ---------------------------------------------------------------------------
# 1. The bug: JSON store → crash between writes → duplicates.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fmt,make_pair", FORMATS, ids=[f[0] for f in FORMATS])
def test_json_store_duplicates_on_crash(base, fmt, make_pair):
    source, sink = make_pair(base)
    source.append(_rows(0, N, seed=0))
    ckpt = JsonCheckpointStore(f"{base}/offsets.json")

    # First worker: append the transformed batch, then CRASH before ckpt.set().
    c1 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    batch = c1.poll()
    kept = _keep(batch.arrow())
    sink.append(kept)
    # <-- crash here: ckpt.set() never happens, in-memory consumer lost.
    del c1

    # Restart: fresh consumer reads the STALE (never-written) JSON offset.
    assert ckpt.get(NAME) is None                      # offset was never persisted
    c2 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    batch2 = c2.poll()
    sink.append(_keep(batch2.arrow()))                 # re-processes the SAME rows

    expected = kept.num_rows
    assert sink.read_all().num_rows == 2 * expected    # DUPLICATES — at-least-once gap


# ---------------------------------------------------------------------------
# 2. The fix: SinkCheckpointStore → atomic commit → no duplicates, no gaps.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fmt,make_pair", FORMATS, ids=[f[0] for f in FORMATS])
def test_sink_store_no_duplicates_on_crash(base, fmt, make_pair):
    source, sink = make_pair(base)
    source.append(_rows(0, N, seed=0))
    ckpt = SinkCheckpointStore(sink)

    # First worker: the ONLY write embeds the offset in the sink's own commit.
    c1 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    batch = c1.poll()
    kept = _keep(batch.arrow())
    sink.append(kept, offsets={NAME: batch.offset})    # data + offset, ONE atomic commit
    # crash = just drop the in-memory consumer.
    del c1

    # Restart: fresh consumer resolves its start from the SINK's commit metadata.
    assert ckpt.get(NAME) == batch.offset == N - 1
    c2 = Consumer(source, name=NAME, checkpoint=ckpt, start="earliest")
    assert c2.position() == N - 1                       # resumed at the committed offset
    assert c2.poll() is None                            # nothing new → no re-processing

    # sink == exactly the source rows matching the transform: no dup, no gap.
    src_all = source.read_all()
    truth = pc.sum(pc.greater(src_all.column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert sink.read_all().num_rows == truth == kept.num_rows


# ---------------------------------------------------------------------------
# 3. History-scan / multi-key: two keys writing to the SAME sink, each commit
#    carrying only its own key. Newest→oldest scan recovers BOTH latest values.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("fmt,make_pair", FORMATS, ids=[f[0] for f in FORMATS])
def test_multi_key_history_scan(base, fmt, make_pair):
    _source, sink = make_pair(base)

    # Interleave two writers ("a", "b") to the same sink across several appends.
    # Each append carries ONLY its own key — the latest snapshot does not
    # accumulate the other key, so recovery must scan back through history.
    sink.append(_rows(0, 10, seed=1), offsets={"a": 100})
    sink.append(_rows(10, 10, seed=2), offsets={"b": 200})
    sink.append(_rows(20, 10, seed=3), offsets={"a": 300})    # newer "a"
    sink.append(_rows(30, 10, seed=4), offsets={"b": 400})    # newer "b"
    sink.append(_rows(40, 10, seed=5), offsets={"a": 500})    # newest "a"

    offs = sink.read_offsets()
    assert offs == {"a": 500, "b": 400}                # first-value-wins per key, newest→oldest

    # And via the store interface used by the Consumer.
    ckpt = SinkCheckpointStore(sink)
    assert ckpt.get("a") == 500
    assert ckpt.get("b") == 400
    assert ckpt.get("missing") is None


def test_read_offsets_empty(base):
    """Empty / offset-less table → empty dict (both formats)."""
    delta_sink = DeltaFormat(f"{base}/d_empty")
    assert delta_sink.read_offsets() == {}              # no _delta_log yet
    delta_sink.append(_rows(0, 5, seed=0))              # plain append, no offsets
    assert delta_sink.read_offsets() == {}

    lt = connect(f"{base}/ice_empty")
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    ice_sink = lt.create("db.silver", schema)
    ice_sink.append(_rows(0, 5, seed=0))
    assert ice_sink.read_offsets() == {}


# ---------------------------------------------------------------------------
# 5. End-to-end through the real Pipeline / @lt.model with checkpoint="sink":
#    run, kill, resume → exactly-once.
# ---------------------------------------------------------------------------
def test_pipeline_sink_checkpoint_exactly_once_iceberg(base):
    lt = connect(f"{base}/ice_e2e", checkpoint="sink")
    assert lt.checkpoint_mode == "sink"
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    src = lt.create("db.events", schema)
    lt.create("db.silver", schema)
    src.append(_rows(0, N, seed=7))

    def clean(df):
        return df.filter(pl.col("value") > THRESHOLD)

    # First run: process one batch, then simulate a kill by dropping the model.
    p1 = lt.pipeline(NAME, "db.events", "db.silver", clean, start="earliest")
    assert isinstance(p1.consumer._ckpt, SinkCheckpointStore)
    n1 = p1.step()
    assert n1 == N
    del p1

    truth = pc.sum(
        pc.greater(src.read_all().column("value"), THRESHOLD).cast(pa.int64())
    ).as_py()
    sink = lt.source("db.silver")
    assert sink.read_all().num_rows == truth

    # Resume: a brand-new pipeline resolves its offset from the sink's commit.
    p2 = lt.pipeline(NAME, "db.events", "db.silver", clean, start="earliest")
    assert p2.consumer.position() == N - 1              # recovered from sink metadata
    assert p2.step() == 0                               # nothing new → no reprocess
    assert sink.read_all().num_rows == truth            # still exactly-once, no duplicates

    # New data arrives → resumed pipeline processes ONLY the delta.
    src.append(_rows(N, 10_000, seed=8))
    p3 = lt.pipeline(NAME, "db.events", "db.silver", clean, start="earliest")
    assert p3.step() == 10_000
    new_truth = pc.sum(
        pc.greater(src.read_all().column("value"), THRESHOLD).cast(pa.int64())
    ).as_py()
    assert sink.read_all().num_rows == new_truth


def test_pipeline_json_path_unchanged_iceberg(base):
    """The default (JSON) path still commits offsets to the side file — no regression."""
    lt = connect(f"{base}/ice_json")               # default checkpoint mode
    assert lt.checkpoint_mode == "json"
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    src = lt.create("db.events", schema)
    lt.create("db.silver", schema)
    src.append(_rows(0, 1_000, seed=9))

    p = lt.pipeline(NAME, "db.events", "db.silver", lambda df: df, start="earliest")
    assert isinstance(p.consumer._ckpt, JsonCheckpointStore)
    assert p.step() == 1_000
    assert lt.checkpoint.get(NAME) == 999          # persisted to the JSON store
    # Sink carried no embedded offsets in the JSON path.
    assert lt.source("db.silver").read_offsets() == {}
