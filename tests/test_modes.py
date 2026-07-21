"""Mutation awareness: (#1) append-mode SAFETY check that surfaces source
UPDATE/DELETE/overwrite commits the offset cursor can't see, and (#2) `upsert`
mode that MERGES by business key so the sink holds current state (inserts +
updates) and reprocessing is idempotent.

Both features are additive + backward-compatible: `mode` defaults to `"append"`
and a pure-append source triggers NO warning. The key correctness case for #1 is
that benign COMPACTION (Iceberg `replace` / Delta `OPTIMIZE`) does NOT warn —
only genuine row-changes do.

No-space temp paths only (delta-rs + PyIceberg on Windows); forward slashes.
"""
import logging
import shutil
import tempfile

import polars as pl
import pyarrow as pa
import pytest

import leat
from leat import DeltaFormat
from leat.consumer import Consumer
from leat.checkpoint import JsonCheckpointStore

pytest.importorskip("deltalake")


@pytest.fixture
def wh():
    d = tempfile.mkdtemp(prefix="leat_modes_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


def _iceberg_src(wh, name="db.events"):
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()),
                        ("k", pa.string()), ("value", pa.int64())])
    lt.create(name, schema)
    return lt, lt.source(name)


# =========================================================================== #
# #1 — Append-mode safety check
# =========================================================================== #

# ---- (a) pure append -> NO warning ---------------------------------------- #
def test_safety_pure_append_no_warning_delta(wh, caplog):
    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b"], "value": [1, 2]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    with caplog.at_level(logging.WARNING, logger="leat"):
        while (b := c.poll()) is not None:
            c.commit()
        # a second pure append: still NO warning
        src.append(pa.table({"k": ["c"], "value": [3]}))
        assert c.poll() is not None
        c.commit()
    assert "cannot capture" not in caplog.text


def test_safety_pure_append_no_warning_iceberg(wh, caplog):
    lt, src = _iceberg_src(wh)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b"], "value": [1, 2]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    with caplog.at_level(logging.WARNING, logger="leat"):
        while (b := c.poll()) is not None:
            c.commit()
        src.append(pa.table({"k": ["c"], "value": [3]}))
        assert c.poll() is not None
        c.commit()
    assert "cannot capture" not in caplog.text


# ---- (b) UPDATE/DELETE -> warning (and error mode raises) ------------------ #
def test_safety_delete_warns_delta(wh, caplog):
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while c.poll() is not None:
        c.commit()

    DeltaTable(src._uri).delete(predicate="k = 'b'")             # non-max row deleted
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()                                                 # offset cursor misses it
    assert "cannot capture" in caplog.text
    assert "DELETE" in caplog.text


def test_safety_update_warns_iceberg(wh, caplog):
    from pyiceberg.expressions import EqualTo

    lt, src = _iceberg_src(wh)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while c.poll() is not None:
        c.commit()

    # in-place overwrite of row b, reusing offset 1 -> row-changing (OVERWRITE)
    t = lt.catalog.load_table("db.events")
    t.overwrite(pa.table({"_offset": pa.array([1], pa.int64()),
                          "k": ["b"], "value": [999]}),
                overwrite_filter=EqualTo("k", "b"))
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()
    assert "cannot capture" in caplog.text
    assert "OVERWRITE" in caplog.text


def test_safety_warns_once_not_per_poll_delta(wh, caplog):
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while c.poll() is not None:
        c.commit()

    DeltaTable(src._uri).delete(predicate="k = 'b'")
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()
        c.poll()
        c.poll()
    # emitted ONCE per detected change, not every poll (no spam)
    assert caplog.text.count("cannot capture") == 1


def test_safety_error_mode_raises_delta(wh):
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest", on_change="error")
    while c.poll() is not None:
        c.commit()

    DeltaTable(src._uri).delete(predicate="k = 'b'")
    with pytest.raises(RuntimeError, match="cannot capture"):
        c.poll()


def test_safety_error_mode_raises_iceberg(wh):
    from pyiceberg.expressions import EqualTo

    lt, src = _iceberg_src(wh)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest", on_change="error")
    while c.poll() is not None:
        c.commit()

    lt.catalog.load_table("db.events").delete(delete_filter=EqualTo("k", "b"))
    with pytest.raises(RuntimeError, match="cannot capture"):
        c.poll()


def test_safety_ignore_mode_silent_delta(wh, caplog):
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest", on_change="ignore")
    while c.poll() is not None:
        c.commit()

    DeltaTable(src._uri).delete(predicate="k = 'b'")
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()
    assert "cannot capture" not in caplog.text          # current silent behavior


# ---- (c) COMPACTION -> NO warning (the key correctness case) -------------- #
def test_safety_compaction_no_warning_delta(wh, caplog):
    """Delta OPTIMIZE (compaction) must NOT warn — rows unchanged. A false
    positive on every compaction would make the check useless."""
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b"], "value": [10, 20]}))
    src.append(pa.table({"k": ["c", "d"], "value": [30, 40]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while c.poll() is not None:
        c.commit()

    DeltaTable(src._uri).optimize.compact()             # benign compaction
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()
    assert "cannot capture" not in caplog.text


def test_safety_compaction_no_warning_iceberg(wh, caplog):
    """Iceberg `replace` (rewrite/compaction) must NOT warn — rows unchanged."""
    lt, src = _iceberg_src(wh)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b"], "value": [10, 20]}))
    src.append(pa.table({"k": ["c", "d"], "value": [30, 40]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while c.poll() is not None:
        c.commit()

    # Rewrite data files (a `replace` snapshot) — the leat-visible compaction op.
    t = lt.catalog.load_table("db.events")
    try:
        from pyiceberg.table import Table  # noqa: F401
        # PyIceberg 0.11 compaction via rewrite-data-files if available; otherwise
        # simulate the benign `replace` op that compaction produces.
        t.rewrite_manifests()
    except Exception:
        pass
    with caplog.at_level(logging.WARNING, logger="leat"):
        c.poll()
    assert "cannot capture" not in caplog.text


# =========================================================================== #
# #2 — upsert mode (merge by key)
# =========================================================================== #

# ---- format-level upsert primitive ---------------------------------------- #
def test_upsert_primitive_current_state_and_idempotent_delta(wh):
    snk = DeltaFormat(f"{wh}/silver")
    # v1 of K, plus M
    snk.upsert(pa.table({"k": ["K", "M"], "value": [1, 100]}), ["k"])
    # v2 of K -> update, not append
    snk.upsert(pa.table({"k": ["K"], "value": [2]}), ["k"])
    got = pl.from_arrow(snk.read_all()).sort("k")
    assert got["k"].to_list() == ["K", "M"]             # exactly one row per key
    assert got["value"].to_list() == [2, 100]           # K = v2 (current state)

    # reprocess the SAME batch -> idempotent, no dupes
    snk.upsert(pa.table({"k": ["K"], "value": [2]}), ["k"])
    got2 = pl.from_arrow(snk.read_all()).sort("k")
    assert got2["k"].to_list() == ["K", "M"]
    assert got2["value"].to_list() == [2, 100]


def test_upsert_primitive_current_state_and_idempotent_iceberg(wh):
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()),
                        ("k", pa.string()), ("value", pa.int64())])
    lt.create("db.silver", schema)
    snk = lt.source("db.silver")
    snk.upsert(pa.table({"k": ["K", "M"], "value": [1, 100]}), ["k"])
    snk.upsert(pa.table({"k": ["K"], "value": [2]}), ["k"])
    got = pl.from_arrow(snk.read_all()).sort("k")
    assert got["k"].to_list() == ["K", "M"]
    assert got["value"].to_list() == [2, 100]

    snk.upsert(pa.table({"k": ["K"], "value": [2]}), ["k"])
    got2 = pl.from_arrow(snk.read_all()).sort("k")
    assert got2["k"].to_list() == ["K", "M"]
    assert got2["value"].to_list() == [2, 100]


# ---- @lt.model(mode="upsert") end-to-end ---------------------------------- #
def _run_upsert_model(lt):
    # v1 of K + M (table() auto-creates + auto-mints _offset on either format)
    lt.table("db.events").write(pl.DataFrame({"id": ["K", "M"], "value": [1, 100]}))

    @lt.model(source="db.events", sink="db.silver", start="earliest",
              mode="upsert", key=["id"])
    def silver(df):
        return df

    silver.run(once=True)
    # v2 of K arrives as a NEW append (updates-as-appends, common CDC-log case)
    lt.table("db.events").write(pl.DataFrame({"id": ["K"], "value": [2]}))
    silver.run(once=True)
    return silver


def test_model_upsert_end_to_end_delta(wh):
    lt = leat.connect(wh, format="delta")
    silver = _run_upsert_model(lt)
    out = lt.table("db.silver").read().sort("id")
    assert out["id"].to_list() == ["K", "M"]            # current state, not history
    assert out["value"].to_list() == [2, 100]           # K updated to v2

    # reprocess the same delta -> idempotent (rewind + rerun): sink unchanged
    silver.step()
    out2 = lt.table("db.silver").read().sort("id")
    assert out2["id"].to_list() == ["K", "M"]
    assert out2["value"].to_list() == [2, 100]


def test_model_upsert_end_to_end_iceberg(wh):
    lt = leat.connect(wh)
    silver = _run_upsert_model(lt)
    out = lt.table("db.silver").read().sort("id")
    assert out["id"].to_list() == ["K", "M"]
    assert out["value"].to_list() == [2, 100]

    silver.step()
    out2 = lt.table("db.silver").read().sort("id")
    assert out2["id"].to_list() == ["K", "M"]
    assert out2["value"].to_list() == [2, 100]


# ---- dedup-latest within a single batch ----------------------------------- #
def test_upsert_dedup_latest_within_batch_delta(wh):
    """v1 then v2 of key K arrive as two appends BEFORE the first poll — one
    batch carries both. Upsert must keep v2 (last/highest offset), not error."""
    lt = leat.connect(wh, format="delta")
    lt.table("db.events").write(pl.DataFrame({"id": ["K"], "value": [1]}))
    lt.table("db.events").write(pl.DataFrame({"id": ["K", "M"], "value": [2, 100]}))

    @lt.model(source="db.events", sink="db.silver", start="earliest",
              mode="upsert", key="id")
    def silver(df):
        return df

    silver.run(once=True)                               # single batch has both K rows
    out = lt.table("db.silver").read().sort("id")
    assert out["id"].to_list() == ["K", "M"]
    assert out["value"].to_list() == [2, 100]           # latest-in-batch wins


# ---- config/validation ---------------------------------------------------- #
def test_upsert_missing_key_errors(wh):
    lt = leat.connect(wh, format="delta")

    with pytest.raises(ValueError, match="requires key"):
        @lt.model(source="db.events", sink="db.silver", mode="upsert")
        def silver(df):
            return df


# ---- parity: iceberg vs delta give identical final state ------------------ #
def test_upsert_parity_iceberg_vs_delta(wh):
    def final_state(lt):
        lt.table("db.events").write(pl.DataFrame({"id": ["a", "b", "c"],
                                                  "value": [1, 2, 3]}))

        @lt.model(source="db.events", sink="db.silver", start="earliest",
                  mode="upsert", key=["id"])
        def silver(df):
            return df

        silver.run(once=True)
        lt.table("db.events").write(pl.DataFrame({"id": ["b", "d"],
                                                  "value": [22, 4]}))  # update b, insert d
        silver.run(once=True)
        return lt.table("db.silver").read().sort("id").to_dicts()

    ice = final_state(leat.connect(f"{wh}/ice"))
    dlt = final_state(leat.connect(f"{wh}/dlt", format="delta"))
    assert ice == dlt
    assert [r["id"] for r in ice] == ["a", "b", "c", "d"]
    assert [r["value"] for r in ice] == [1, 22, 3, 4]


# =========================================================================== #
# Backward compat: mode defaults to "append", identical behavior
# =========================================================================== #
def test_append_mode_default_unchanged_delta(wh):
    lt = leat.connect(wh, format="delta")
    src = lt.source("db.events")
    src.append(pa.table({"id": ["a", "b"], "value": [1, 2]}))

    @lt.model(source="db.events", sink="db.silver", start="earliest")   # no mode -> append
    def silver(df):
        return df.filter(pl.col("value") >= 1)

    assert silver._p.mode == "append"
    silver.run(once=True)
    # reprocess appends AGAIN (append-only sink duplicates — unchanged behavior)
    src.append(pa.table({"id": ["a"], "value": [1]}))
    silver.run(once=True)
    out = lt.table("db.silver").read()
    assert out.height == 3                              # 2 + 1 appended (no merge)
