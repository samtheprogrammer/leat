"""leat 5-line quickstart — easy like Polars, offsets are leat's job.

connect -> write a Polars frame (no schema, no offset) -> declare a model
(your df has NO _offset, just your columns) -> read back Polars (no _offset).
leat mints the Kafka-style offset for you; it stays real & controllable at the
consumer level (start/position/lag/seek).
"""
import atexit
import io
import shutil
import sys
import tempfile
import polars as pl
import leat

# Windows consoles default to cp1252; Polars' table repr uses box-drawing chars.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Cross-platform, no-space, unique warehouse dir (respects the no-space
# requirement); cleaned up on exit.
TMP = tempfile.mkdtemp(prefix="leat_quickstart_")
atexit.register(lambda: shutil.rmtree(TMP, ignore_errors=True))

# 1. connect (zero-config local session)
lt = leat.connect(TMP)

# 2. write a Polars frame — no schema, no _offset. leat infers + auto-creates + mints.
lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],
                                          "value": [50, 150, 250]}))


# 3. a pipeline is a Polars function + source + sink. df has NO _offset — just your columns.
@lt.model(source="db.events", sink="db.silver", start="earliest")
def silver(df):
    return df.filter(pl.col("value") > 100)


silver.run(once=True)

# 4. read back — Polars DataFrame, no _offset column.
print("silver (no _offset column, just your columns):")
print(lt.table("db.silver").read())

# --- offsets are invisible in your data, but REAL and controllable at the
#     consumer level (Kafka-style). leat assigns them for you on write. ---
from leat import Consumer, JsonCheckpointStore
c = Consumer(lt.source("db.events"), name="peek",
             checkpoint=JsonCheckpointStore(f"{TMP}/peek.json"), start="earliest")
batch = c.poll()
print("\nKafka verbs still work — offsets are real:")
print("  consumer.position() before commit:", c.position())
print("  batch max offset (leat-minted)   :", batch.offset)
c.commit()
print("  consumer.position() after commit :", c.position())
print("  consumer.lag()                   :", c.lag())
print("\nthat's the whole thing — write, declare, read. leat owns the offset.")
