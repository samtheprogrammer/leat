"""leat quickstart on Delta Lake: incremental silver over Delta with Kafka-style
offsets. Same Consumer code as the Iceberg example — only the format changes.

Demonstrates format neutrality: start='earliest', poll/commit,
continue-from-offset, lag, and exactly-once (silver == source filtered), all
single-node, no cluster, no JVM (delta-rs is Rust-native).
"""
import atexit, shutil, tempfile
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl

from leat import DeltaFormat, Consumer, JsonCheckpointStore

# Cross-platform, no-space, unique warehouse dir (delta-rs needs a no-space
# path); cleaned up on exit.
BASE = tempfile.mkdtemp(prefix="leat_delta_")
atexit.register(lambda: shutil.rmtree(BASE, ignore_errors=True))

events = DeltaFormat(f"{BASE}/events")   # source ("topic")
silver = DeltaFormat(f"{BASE}/silver")   # sink
ckpt = JsonCheckpointStore(f"{BASE}/offsets.json")

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
      f"{b.num_rows if b else 0} rows (only the new batch)")
if b:
    silver.append(b.pl().filter(pl.col("value") > 100).to_arrow())
    consumer2.commit()

# --- exactly-once check: silver == every source row with value>100 ---
src = events.read_all()
truth = pc.sum(pc.greater(src["value"], 100).cast(pa.int64())).as_py()
got = silver.read_all().num_rows
print(f"\nexactly-once: silver rows {got} == source(value>100) {truth} -> {got == truth}")
print("no broker, no cluster, no spark, no JVM. 1 process.")
