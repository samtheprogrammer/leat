"""Ergonomics: leat-owned, invisible `_offset` at the transform boundary — while
the Kafka-style consumer surface (start/position/lag/seek/commit) stays first-class.

Covers:
  - low-level auto-mint on append (Iceberg + Delta) when `_offset` is absent,
    continuation across appends, and explicit `_offset` honored when present.
  - Session.table(id).write(df)/.read(): schema inference, auto-create, mint,
    `_offset`-free reads; Polars AND pyarrow inputs.
  - @lt.model transform receives a df with NO `_offset` and returns business-only;
    end-to-end easy path (write -> model -> read); Consumer position()/lag() still work.
"""
import shutil
import tempfile

import polars as pl
import pyarrow as pa
import pytest

import leat
from leat import Consumer, JsonCheckpointStore


@pytest.fixture
def wh():
    d = tempfile.mkdtemp(prefix="pxleat_ergo_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


# --------------------------------------------------------------------------- #
# 1. Low-level auto-mint on IcebergFormat.append
# --------------------------------------------------------------------------- #
def test_iceberg_append_auto_mints_offset(wh):
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    tbl = lt.create("db.events", schema)

    # append WITHOUT _offset -> minted 0..n-1
    tbl.append(pa.table({"value": [10, 20, 30]}))
    assert tbl.earliest_offset() == 0
    assert tbl.latest_offset() == 2
    got = pl.from_arrow(tbl.read_all()).sort("_offset")
    assert got["_offset"].to_list() == [0, 1, 2]
    assert got["value"].to_list() == [10, 20, 30]

    # append more -> continues 3..4
    tbl.append(pa.table({"value": [40, 50]}))
    assert tbl.latest_offset() == 4
    assert sorted(pl.from_arrow(tbl.read_all())["_offset"].to_list()) == [0, 1, 2, 3, 4]


def test_iceberg_append_honors_explicit_offset(wh):
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    tbl = lt.create("db.events", schema)

    # explicit _offset present -> used as-is (backward compat)
    tbl.append(pa.table({"_offset": pa.array([100, 101], pa.int64()),
                         "value": [1, 2]}))
    assert tbl.earliest_offset() == 100
    assert tbl.latest_offset() == 101


# --------------------------------------------------------------------------- #
# 2. Session.table().write()/.read() — Polars-easy path
# --------------------------------------------------------------------------- #
def test_table_write_read_polars_input(wh):
    lt = leat.connect(wh)
    lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],
                                              "value": [50, 150, 250]}))
    out = lt.table("db.events").read()
    assert isinstance(out, pl.DataFrame)
    assert "_offset" not in out.columns            # user-facing: no offset column
    assert set(out.columns) == {"user_id", "value"}
    assert out.sort("user_id")["value"].to_list() == [50, 150, 250]

    # underlying format still carries the minted _offset (advanced/low-level use)
    fmt = lt.table("db.events").format
    assert "_offset" in fmt.read_all().column_names
    assert fmt.latest_offset() == 2


def test_table_write_pyarrow_input(wh):
    lt = leat.connect(wh)
    lt.write("db.events", pa.table({"user_id": [7, 8], "value": [1, 2]}))  # Session.write
    out = lt.table("db.events").read()
    assert "_offset" not in out.columns
    assert out.sort("user_id")["user_id"].to_list() == [7, 8]


def test_table_write_appends_to_existing(wh):
    lt = leat.connect(wh)
    h = lt.table("db.events")
    h.write(pl.DataFrame({"value": [1, 2]}))
    h.write(pl.DataFrame({"value": [3, 4]}))       # existing table -> append + mint continues
    assert h.format.latest_offset() == 3
    assert sorted(h.read()["value"].to_list()) == [1, 2, 3, 4]


# --------------------------------------------------------------------------- #
# 3. @lt.model transform sees NO _offset; end-to-end easy path; Kafka verbs work
# --------------------------------------------------------------------------- #
def test_model_transform_has_no_offset_and_end_to_end(wh):
    lt = leat.connect(wh)
    lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3, 4],
                                              "value": [50, 150, 250, 90]}))

    seen_cols = {}

    @lt.model(source="db.events", sink="db.silver", start="earliest")
    def silver(df):
        seen_cols["cols"] = set(df.columns)        # transform sees business-only
        return df.filter(pl.col("value") > 100)

    silver.run(once=True)

    assert seen_cols["cols"] == {"user_id", "value"}   # NO _offset at the boundary
    out = lt.table("db.silver").read()
    assert "_offset" not in out.columns
    assert sorted(out["value"].to_list()) == [150, 250]

    # Consumer position()/lag() still work — offset is visible at consumer level.
    c = Consumer(lt.source("db.events"), name="peek",
                 checkpoint=JsonCheckpointStore(f"{wh}/peek.json"), start="earliest")
    assert c.lag() == 4                                 # 4 rows available from earliest
    batch = c.poll()
    assert batch.offset == 3                            # leat-minted max offset
    c.commit()
    assert c.position() == 3
    assert c.lag() == 0


# --------------------------------------------------------------------------- #
# 4. Delta smoke: auto-mint when _offset absent
# --------------------------------------------------------------------------- #
def test_delta_append_auto_mints_offset(wh):
    pytest.importorskip("deltalake")
    from leat import DeltaFormat

    tbl = DeltaFormat(f"{wh}/dtable")
    tbl.append(pa.table({"value": [1, 2, 3]}))
    assert tbl.earliest_offset() == 0
    assert tbl.latest_offset() == 2

    tbl.append(pa.table({"value": [4, 5]}))
    assert tbl.latest_offset() == 4
    got = pl.from_arrow(tbl.read_all()).sort("_offset")
    assert got["_offset"].to_list() == [0, 1, 2, 3, 4]
    assert got["value"].to_list() == [1, 2, 3, 4, 5]
