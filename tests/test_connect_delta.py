"""connect(format="delta") — Delta as a first-class connect() citizen.

Proves the 5-line easy path (`connect` -> `table.write` -> `@lt.model` -> read)
works on Delta exactly like Iceberg, that the identifier->path convention holds,
that `checkpoint="sink"` gives exactly-once with crash/resume, that Kafka verbs
(position/lag) are visible on a Delta-backed source, and that Iceberg vs Delta
produce identical business results. Backward-compat smoke keeps the default
(Iceberg) connect() path unchanged.

No-space temp paths only (delta-rs + Windows); forward slashes.
"""
import shutil
import tempfile

import polars as pl
import pyarrow as pa
import pytest

import leat
from leat import Consumer, JsonCheckpointStore

pytest.importorskip("deltalake")

THRESHOLD = 100


@pytest.fixture
def wh():
    d = tempfile.mkdtemp(prefix="leat_cdelta_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


# --------------------------------------------------------------------------- #
# 1. table().write()/.read() easy path on Delta (Polars + pyarrow inputs)
# --------------------------------------------------------------------------- #
def test_delta_table_write_read_polars_input(wh):
    lt = leat.connect(wh, format="delta")
    lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],
                                              "value": [50, 150, 250]}))
    out = lt.table("db.events").read()
    assert isinstance(out, pl.DataFrame)
    assert "_offset" not in out.columns            # user-facing: no offset column
    assert set(out.columns) == {"user_id", "value"}
    assert out.sort("user_id")["value"].to_list() == [50, 150, 250]

    # underlying DeltaFormat still carries the minted _offset (low-level use)
    fmt = lt.table("db.events").format
    assert "_offset" in fmt.read_all().column_names
    assert fmt.latest_offset() == 2


def test_delta_table_write_pyarrow_input_and_path_convention(wh):
    import os
    lt = leat.connect(wh, format="delta")
    lt.write("db.events", pa.table({"user_id": [7, 8], "value": [1, 2]}))  # Session.write
    out = lt.table("db.events").read()
    assert "_offset" not in out.columns
    assert out.sort("user_id")["user_id"].to_list() == [7, 8]
    # identifier "db.events" maps to <warehouse>/db/events
    assert os.path.isdir(f"{wh}/db/events/_delta_log")


def test_delta_table_write_appends_to_existing(wh):
    lt = leat.connect(wh, format="delta")
    h = lt.table("db.events")
    h.write(pl.DataFrame({"value": [1, 2]}))
    h.write(pl.DataFrame({"value": [3, 4]}))       # existing table -> append + mint continues
    assert h.format.latest_offset() == 3
    assert sorted(h.read()["value"].to_list()) == [1, 2, 3, 4]


# --------------------------------------------------------------------------- #
# 2. @lt.model end-to-end on Delta: exactly-once + crash/resume (sink checkpoint)
# --------------------------------------------------------------------------- #
def test_delta_model_exactly_once_and_resume(wh):
    lt = leat.connect(wh, format="delta", checkpoint="sink")
    lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3, 4],
                                              "value": [50, 150, 250, 90]}))

    seen = {}

    @lt.model(source="db.events", sink="db.silver", start="earliest")
    def silver(df):
        seen["cols"] = set(df.columns)             # transform sees business-only
        return df.filter(pl.col("value") > THRESHOLD)

    silver.run(once=True)
    assert seen["cols"] == {"user_id", "value"}    # NO _offset at the boundary
    out = lt.table("db.silver").read()
    assert "_offset" not in out.columns
    assert sorted(out["value"].to_list()) == [150, 250]

    # Crash + resume: a FRESH Session over the same warehouse re-registers the
    # model. With checkpoint="sink" the start offset is recovered from the Delta
    # commit metadata -> zero extra rows processed, no duplicates.
    lt2 = leat.connect(wh, format="delta", checkpoint="sink")

    @lt2.model(source="db.events", sink="db.silver", start="earliest")
    def silver(df):
        return df.filter(pl.col("value") > THRESHOLD)

    assert silver.step() == 0                       # nothing new to process
    out2 = lt2.table("db.silver").read()
    assert sorted(out2["value"].to_list()) == [150, 250]   # still exactly-once

    # New source data -> only the delta is processed on the next run.
    lt2.table("db.events").write(pl.DataFrame({"user_id": [5, 6],
                                               "value": [500, 10]}))
    assert silver.step() == 2                       # 2 new source rows read
    out3 = lt2.table("db.silver").read()
    assert sorted(out3["value"].to_list()) == [150, 250, 500]


def test_delta_model_json_checkpoint(wh):
    """checkpoint="json" (opt-in) also works end-to-end on Delta."""
    lt = leat.connect(wh, format="delta", checkpoint="json")   # opt into JSON offsets
    assert lt.checkpoint_mode == "json"
    lt.table("db.events").write(pl.DataFrame({"value": [50, 150, 250]}))

    @lt.model(source="db.events", sink="db.silver", start="earliest")
    def silver(df):
        return df.filter(pl.col("value") > THRESHOLD)

    silver.run(once=True)
    assert sorted(lt.table("db.silver").read()["value"].to_list()) == [150, 250]


# --------------------------------------------------------------------------- #
# 3. Kafka verbs visible on the Delta-backed source
# --------------------------------------------------------------------------- #
def test_delta_kafka_verbs_position_and_lag(wh):
    lt = leat.connect(wh, format="delta")
    lt.table("db.events").write(pl.DataFrame({"value": [10, 20, 30, 40]}))

    c = Consumer(lt.source("db.events"), name="peek",
                 checkpoint=JsonCheckpointStore(f"{wh}/peek.json"), start="earliest")
    assert c.lag() == 4                             # 4 rows available from earliest
    batch = c.poll()
    assert batch.offset == 3                        # leat-minted max offset
    c.commit()
    assert c.position() == 3
    assert c.lag() == 0


# --------------------------------------------------------------------------- #
# 4. Parity: iceberg vs delta -> identical business results
# --------------------------------------------------------------------------- #
def test_iceberg_delta_connect_parity(wh):
    import numpy as np
    rng = np.random.default_rng(42)
    seeded = pl.DataFrame({"user_id": rng.integers(0, 1000, 5000),
                           "value": rng.integers(0, 200, 5000)})

    def run(fmt):
        lt = leat.connect(f"{wh}/{fmt}", format=fmt, checkpoint="sink")
        lt.table("db.events").write(seeded)

        @lt.model(source="db.events", sink="db.silver", start="earliest")
        def silver(df):
            return df.filter(pl.col("value") > THRESHOLD)

        silver.run(once=True)
        return lt.table("db.silver").read()

    d = run("delta").sort(["user_id", "value"])
    i = run("iceberg").sort(["user_id", "value"])
    assert d.height == i.height > 0
    assert d["value"].sum() == i["value"].sum()
    assert d["value"].to_list() == i["value"].to_list()
    assert d["user_id"].to_list() == i["user_id"].to_list()


# --------------------------------------------------------------------------- #
# 5. Backward-compat smoke: default connect() (iceberg) unchanged
# --------------------------------------------------------------------------- #
def test_connect_default_is_iceberg_unchanged(wh):
    lt = leat.connect(wh)                           # no format kwarg -> iceberg
    assert lt.format == "iceberg"
    assert lt.catalog is not None
    lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],
                                              "value": [50, 150, 250]}))
    out = lt.table("db.events").read()
    assert "_offset" not in out.columns
    assert out.sort("user_id")["value"].to_list() == [50, 150, 250]
