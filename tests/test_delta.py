"""Delta Lake adapter tests — drives the REAL leat.Consumer through a Delta
source/sink, and asserts DeltaFormat is a drop-in for IcebergFormat (same
incremental semantics, same row counts for the same logical data).

No-space temp paths only (delta-rs + Windows).
"""
import os
import shutil
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from leat import DeltaFormat
from leat.consumer import Consumer
from leat.checkpoint import JsonCheckpointStore

deltalake = pytest.importorskip("deltalake")

N = 50_000
MORE = 10_000
THRESHOLD = 100


def _rows(start, n, seed):
    off = np.arange(start, start + n, dtype=np.int64)
    val = np.random.default_rng(seed).integers(0, 200, n, dtype=np.int64)
    return pa.table({"_offset": off, "value": val})


@pytest.fixture
def base():
    # No-space temp dir on the same drive as the repo is fine; tempfile is safest.
    d = tempfile.mkdtemp(prefix="pxleat_delta_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


def test_delta_incremental_through_consumer(base):
    src_path = f"{base}/events"
    silver_path = f"{base}/silver"
    ckpt_path = f"{base}/offsets.json"

    source = DeltaFormat(src_path)
    silver = DeltaFormat(silver_path)
    ckpt = JsonCheckpointStore(ckpt_path)

    # 50k rows in the source.
    source.append(_rows(0, N, seed=0))
    assert source.earliest_offset() == 0
    assert source.latest_offset() == N - 1

    consumer = Consumer(source, name="silver_clean", checkpoint=ckpt, start="earliest")

    # Drain history: poll -> filter value>100 -> append to silver -> commit.
    total_polled = 0
    total_kept = 0
    last_offset = -1
    while (batch := consumer.poll()) is not None:
        # offsets advance monotonically
        assert batch.offset > last_offset
        last_offset = batch.offset
        kept = pc.filter(batch.arrow(), pc.greater(batch.arrow().column("value"), THRESHOLD))
        silver.append(kept)
        consumer.commit()
        total_polled += batch.num_rows
        total_kept += kept.num_rows

    assert total_polled == N
    # After consuming all, poll() returns None.
    assert consumer.poll() is None
    assert consumer.position() == N - 1

    # Committed offset persisted.
    assert ckpt.get("silver_clean") == N - 1

    # silver == every source row with value>THRESHOLD (exactly-once).
    src_all = source.read_all()
    truth = pc.sum(pc.greater(src_all.column("value"), THRESHOLD).cast(pa.int64())).as_py()
    assert silver.read_all().num_rows == truth == total_kept

    # --- new data arrives ---
    source.append(_rows(N, MORE, seed=1))
    assert source.latest_offset() == N + MORE - 1

    # A FRESH consumer resumes from the committed offset and sees only the new delta.
    consumer2 = Consumer(source, name="silver_clean", checkpoint=ckpt, start="latest")
    assert consumer2.position() == N - 1          # resumed at committed offset

    batch2 = consumer2.poll()
    assert batch2 is not None
    assert batch2.num_rows == MORE                # ONLY the new rows
    assert batch2.offset == N + MORE - 1
    assert batch2.offset > consumer2._offset      # advances monotonically
    consumer2.commit()
    assert consumer2.poll() is None               # nothing left after
    assert ckpt.get("silver_clean") == N + MORE - 1


def test_delta_iceberg_row_count_parity(base):
    """Format neutrality: DeltaFormat and IcebergFormat produce identical row
    counts for the same logical data, driven by identical Consumer code."""
    from leat import connect

    data = _rows(0, N, seed=42)

    # --- Delta side ---
    delta_src = DeltaFormat(f"{base}/d_events")
    delta_snk = DeltaFormat(f"{base}/d_silver")
    delta_src.append(data)
    delta_ck = JsonCheckpointStore(f"{base}/d_offsets.json")
    _drain(delta_src, delta_snk, delta_ck)

    # --- Iceberg side (tiny parallel table via leat.connect) ---
    wh = f"{base}/ice"
    lt = connect(wh)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    ice_src = lt.create("db.events", schema)
    ice_snk = lt.create("db.silver", schema)
    ice_src.append(data)
    _drain(ice_src, ice_snk, lt.checkpoint, name="ice_clean")

    assert delta_snk.read_all().num_rows == ice_snk.read_all().num_rows
    assert delta_src.read_all().num_rows == ice_src.read_all().num_rows == N


def _drain(src, snk, ckpt, name="parity_clean"):
    c = Consumer(src, name=name, checkpoint=ckpt, start="earliest")
    while (batch := c.poll()) is not None:
        arr = batch.arrow()
        kept = pc.filter(arr, pc.greater(arr.column("value"), THRESHOLD))
        snk.append(kept)
        c.commit()
