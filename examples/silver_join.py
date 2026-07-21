"""Stream-table join: enrich each event delta against a dimension table via DuckDB.

The delta (new events) is joined to the customers dimension in DuckDB (native hash
join over Arrow) — no join engine to build, no cluster. Incremental + exactly-once.
"""
import atexit, os, shutil, tempfile, time
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from leat import IcebergFormat, Consumer, JsonCheckpointStore, sql

# Cross-platform, no-space, unique warehouse dir; cleaned up on exit.
BASE = tempfile.mkdtemp(prefix="leat_join_")
atexit.register(lambda: shutil.rmtree(BASE, ignore_errors=True))
os.makedirs(BASE + "/wh", exist_ok=True)
cat = SqlCatalog("l", **{"uri": f"sqlite:///{BASE}/c.db", "warehouse": f"file:///{BASE}/wh",
                         "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"})
cat.create_namespace("s")
cat.create_table("s.events", schema=pa.schema(
    [("_offset", pa.int64()), ("customer_id", pa.int64()), ("amount", pa.int64())]))
cat.create_table("s.customers", schema=pa.schema(
    [("customer_id", pa.int64()), ("region", pa.string())]))
SILVER = pa.schema([("_offset", pa.int64()), ("customer_id", pa.int64()),
                    ("amount", pa.int64()), ("region", pa.string())])
cat.create_table("s.silver", schema=SILVER)

events = IcebergFormat(cat, "s.events")
customers = IcebergFormat(cat, "s.customers")
silver = IcebergFormat(cat, "s.silver")
ckpt = JsonCheckpointStore(BASE + "/o.json")

# dimension table (small, read once)
REGIONS = ["NA", "EU", "APAC", "LATAM"]
customers.append(pa.table({"customer_id": np.arange(1000, dtype=np.int64),
                           "region": pa.array([REGIONS[i % 4] for i in range(1000)])}))
dim = customers.read_all()

consumer = Consumer(events, name="enrich", checkpoint=ckpt, start="latest")
N = 200_000
print("stream-table join: events delta JOIN customers (DuckDB)\n")
for b in range(3):
    off = np.arange(b*N, (b+1)*N, dtype=np.int64)
    cid = np.random.default_rng(b).integers(0, 1000, N, dtype=np.int64)
    amt = np.random.default_rng(b+9).integers(1, 500, N, dtype=np.int64)
    events.append(pa.table({"_offset": off, "customer_id": cid, "amount": amt}))

    batch = consumer.poll()
    t = time.perf_counter()
    enriched = sql("""
        SELECT b._offset, b.customer_id, b.amount, d.region
        FROM batch b LEFT JOIN dim d ON b.customer_id = d.customer_id
    """, batch=batch.arrow(), dim=dim).cast(SILVER)      # native hash join over Arrow
    dt = (time.perf_counter() - t) * 1e3
    silver.append(enriched)
    consumer.commit()
    print(f"  batch {b}: joined {batch.num_rows} events -> {enriched.num_rows} enriched, {dt:.0f} ms")

sv = cat.load_table("s.silver").scan().to_arrow()
with_region = pc.sum(pc.is_valid(sv["region"]).cast(pa.int64())).as_py()
print(f"\nenriched rows: {sv.num_rows}, with region: {with_region}, all matched: {sv.num_rows == with_region}")
reg = sv.group_by("region").aggregate([("region", "count")]).to_pylist()
print("region distribution:", {r["region"]: r["region_count"] for r in reg})
print("\nnative join, no engine, no cluster. Iceberg -> DuckDB -> Iceberg, all Arrow.")
