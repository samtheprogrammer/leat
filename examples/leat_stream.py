# leat streaming per-batch latency: a commit arrives -> consume only the delta -> commit.
# Matches the Spark Structured Streaming test (filter value>100, 200k rows/batch).
import atexit, os, shutil, tempfile, time
import numpy as np
import pyarrow as pa
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
from leat import IcebergFormat, Consumer, JsonCheckpointStore

# Cross-platform, no-space, unique warehouse dir; cleaned up on exit.
BASE = tempfile.mkdtemp(prefix="leat_stream_")
atexit.register(lambda: shutil.rmtree(BASE, ignore_errors=True))
os.makedirs(BASE + "/wh", exist_ok=True)
t0 = time.perf_counter()
cat = SqlCatalog("l", **{"uri": f"sqlite:///{BASE}/c.db", "warehouse": f"file:///{BASE}/wh",
                         "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
cat.create_namespace("s")
sch = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
cat.create_table("s.events", schema=sch); cat.create_table("s.silver", schema=sch)
events = IcebergFormat(cat, "s.events"); silver = IcebergFormat(cat, "s.silver")
ckpt = JsonCheckpointStore(BASE + "/o.json")
consumer = Consumer(events, name="s", checkpoint=ckpt, start="latest")
cold = time.perf_counter() - t0

N = 200_000
print("LEAT streaming (filter, 200k rows/batch)")
print(f"cold start         : {cold*1e3:8.0f} ms")
durs = []
for b in range(8):
    off = np.arange(b*N, (b+1)*N, dtype=np.int64)                 # a commit "arrives"
    val = np.random.default_rng(b).integers(0, 200, N, dtype=np.int64)
    events.append(pa.table({"_offset": off, "value": val}))
    t = time.perf_counter()
    batch = consumer.poll()                                       # consume only the delta
    silver.append(batch.pl().filter(pl.col("value") > 100).to_arrow())
    consumer.commit()
    dt = (time.perf_counter() - t) * 1e3
    durs.append(dt)
    print(f"  batch {b}: {dt:>6.0f} ms proc, {batch.num_rows:>7} rows")
warm = durs[1:]
print(f"per-batch warm proc: {sum(warm)/len(warm):8.0f} ms")
print("NOTE: runs as a triggered/scheduled task -> scales to ZERO between batches (no idle cost).")
