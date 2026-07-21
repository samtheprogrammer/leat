"""The "easy like Polars" API — define pipelines as decorated Polars functions.

A leat pipeline is just: a Polars function + where it reads from + where it writes to.
No catalog wiring, no loop, no commits. If you know Polars, you know leat.
"""
import atexit
import shutil
import tempfile
import numpy as np
import pyarrow as pa
import polars as pl
import leat

# Cross-platform, no-space, unique warehouse dir; cleaned up on exit.
WH = tempfile.mkdtemp(prefix="leat_models_")
atexit.register(lambda: shutil.rmtree(WH, ignore_errors=True))

lt = leat.connect(WH)
schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
events = lt.create("db.events", schema)
lt.create("db.silver", schema)
lt.create("db.big_values", schema)

# some bronze data
events.append(pa.table({"_offset": np.arange(200_000, dtype=np.int64),
                        "value": np.random.default_rng(0).integers(0, 200, 200_000, dtype=np.int64)}))


# --- define pipelines as decorated Polars functions. that's the whole thing. ---
@lt.model(source="db.events", sink="db.silver", start="earliest")
def silver_clean(df):
    return df.filter(pl.col("value") > 100)


@lt.model(source="db.events", sink="db.big_values", start="earliest")
def big_values(df):
    return df.filter(pl.col("value") > 180).with_columns(pl.col("value") * 10)


# run one, or run them all
silver_clean.run(once=True)
big_values.run(once=True)
# lt.run_all(once=True)   # <- or this

print("silver rows   :", lt.source("db.silver").read_all().num_rows)
print("big_values rows:", lt.source("db.big_values").read_all().num_rows)
print("\nthat's a pipeline: a Polars function + source + sink. no loop, no commits, no cluster.")
