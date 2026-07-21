# Where does leat's per-batch time go? Break the loop into read / transform / sink / commit.
import os, shutil, tempfile, time
import numpy as np
import pyarrow as pa
import polars as pl
from pyiceberg.catalog.sql import SqlCatalog
from leat import IcebergFormat, Consumer, JsonCheckpointStore

BASE = tempfile.mkdtemp(prefix="leat_profile_").replace(os.sep, "/")   # cross-platform, no-space
shutil.rmtree(BASE, ignore_errors=True); os.makedirs(BASE + "/wh", exist_ok=True)
cat = SqlCatalog("l", **{"uri": f"sqlite:///{BASE}/c.db", "warehouse": f"file:///{BASE}/wh",
                         "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
cat.create_namespace("s")
sch = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
cat.create_table("s.events", schema=sch); cat.create_table("s.silver", schema=sch)
events = IcebergFormat(cat, "s.events"); silver = IcebergFormat(cat, "s.silver")
ckpt = JsonCheckpointStore(BASE + "/o.json")
consumer = Consumer(events, name="s", checkpoint=ckpt, start="latest")

N = 200_000
phases = {"read": [], "transform": [], "sink": [], "commit": []}
for b in range(8):
    off = np.arange(b*N, (b+1)*N, dtype=np.int64)
    val = np.random.default_rng(b).integers(0, 200, N, dtype=np.int64)
    events.append(pa.table({"_offset": off, "value": val}))          # (bronze producer, not timed)

    t = time.perf_counter(); batch = consumer.poll(); phases["read"].append(time.perf_counter()-t)
    t = time.perf_counter(); out = batch.pl().filter(pl.col("value") > 100).to_arrow(); phases["transform"].append(time.perf_counter()-t)
    t = time.perf_counter(); silver.append(out); phases["sink"].append(time.perf_counter()-t)
    t = time.perf_counter(); consumer.commit(); phases["commit"].append(time.perf_counter()-t)

print(f"leat per-batch breakdown (200k rows, filter, LOCAL fs)\n")
warm = lambda xs: xs[1:]
total = sum(sum(warm(v)) / len(warm(v)) for v in phases.values())
print(f"{'phase':<12}{'ms':>9}{'% of batch':>12}")
print("-" * 33)
for name, v in phases.items():
    ms = sum(warm(v)) / len(warm(v)) * 1e3
    print(f"{name:<12}{ms:>8.1f}{ms/(total*1e3)*100:>11.0f}%")
print("-" * 33)
print(f"{'TOTAL':<12}{total*1e3:>8.1f}{100:>11}%")
print("\n(on S3, read+sink dominate far more — object-storage latency; transform stays tiny)")
