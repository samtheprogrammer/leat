"""leat quickstart: incremental silver over Iceberg with Kafka-style offsets.

Demonstrates: start='earliest', poll/commit, continue-from-offset, lag, and
exactly-once (silver == source filtered), all single-node, no cluster.
"""
import atexit, os, shutil, tempfile
import numpy as np
import pyarrow as pa
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog

from leat import IcebergFormat, Consumer, JsonCheckpointStore

# Cross-platform, no-space, unique warehouse dir; cleaned up on exit.
BASE = tempfile.mkdtemp(prefix="leat_silver_")
atexit.register(lambda: shutil.rmtree(BASE, ignore_errors=True))
os.makedirs(BASE + "/wh", exist_ok=True)
cat = SqlCatalog("leat", **{
    "uri": f"sqlite:///{BASE}/catalog.db",
    "warehouse": f"file:///{BASE}/wh",
    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
})
cat.create_namespace("s")
schema = pa.schema([("_offset", pa.int64()), ("id", pa.int64()), ("value", pa.int64())])
cat.create_table("s.events", schema=schema)   # source ("topic")
cat.create_table("s.silver", schema=schema)   # sink

events = IcebergFormat(cat, "s.events")
silver = IcebergFormat(cat, "s.silver")
ckpt = JsonCheckpointStore(BASE + "/offsets.json")

N = 100_000
def produce(batch_id):
    base = batch_id * N
    off = np.arange(base, base + N, dtype=np.int64)          # monotonic offset (the Kafka offset)
    val = np.random.default_rng(batch_id).integers(0, 200, N, dtype=np.int64)
    events.append(pa.table({"_offset": off, "id": off, "value": val}))

# --- 3 batches already in the table; consumer starts from the beginning ---
for b in range(3):
    produce(b)

consumer = Consumer(events, name="silver_clean", checkpoint=ckpt, start="earliest")
print("start=earliest -> draining history")
while (batch := consumer.poll()) is not None:
    kept = batch.pl().filter(pl.col("value") > 100)          # silver transform (Polars)
    silver.append(kept.to_arrow())
    consumer.commit()
    print(f"  polled {batch.num_rows:>6} rows, kept {kept.height:>5}, "
          f"offset -> {consumer.position()}, lag {consumer.lag()}")

# --- new data arrives; a fresh consumer CONTINUES from the committed offset ---
produce(3)
consumer2 = Consumer(events, name="silver_clean", checkpoint=ckpt, start="latest")
b = consumer2.poll()
print(f"\ncontinue: new consumer resumed at committed offset, polled "
      f"{b.num_rows if b else 0} rows (only the new batch), lag was {consumer2.lag()+ (b.num_rows if b else 0)}")
if b:
    silver.append(b.pl().filter(pl.col("value") > 100).to_arrow())
    consumer2.commit()

# --- exactly-once check: silver == every source row with value>100 ---
import pyarrow.compute as pc
src = cat.load_table("s.events").scan().to_arrow()
truth = pc.sum(pc.greater(src["value"], 100).cast(pa.int64())).as_py()
got = cat.load_table("s.silver").scan().to_arrow().num_rows
print(f"\nexactly-once: silver rows {got} == source(value>100) {truth} -> {got == truth}")
print("no broker, no cluster, no spark. 1 process.")
