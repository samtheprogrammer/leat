"""Characterization tests: how leat behaves under UPDATES and DELETES of source
data (not just appends).

leat's incremental model is a monotonic `_offset` column + a `read_since(offset)`
that returns only rows with `_offset > committed_offset` (PyIceberg 0.11 has NO
snapshot-diff scan, so this offset column IS the incremental cursor). This file
pins down EXACTLY what that means when existing source rows are MUTATED or
REMOVED rather than appended.

Three hypotheses, all asserted against REAL observed behavior:

  H1  updates-as-appends WORK. Emitting a NEW row (higher offset) for a changed
      business key is a normal append; a dedup-to-latest transform yields the
      correct current state.                                        -> PASSES.

  H2  in-place UPDATE is MISSED. Mutating an existing row (Delta `update`,
      Iceberg `overwrite`) does NOT advance that row's `_offset`, so the
      incremental `read_since` never returns it.  This is a KNOWN LIMITATION
      (CLAUDE.md roadmap item "CDC deletes / Change Data Feed"), NOT desired
      behavior -- the tests document it with assertions.

  H3  DELETE is MISSED by the incremental reader. Removing a source row (Delta
      `delete`, Iceberg `delete`) is invisible to `read_since`, so an
      append-only silver still contains the deleted row.  Also a documented
      limitation.  (Note: `read_all()` DOES reflect the delete -- it's only the
      incremental cursor that misses it.)

See docs/updates-and-cdc.md for the scope-boundary writeup and the Delta
Change-Data-Feed path forward (proven in the last section of this file).

No-space temp paths only (delta-rs + PyIceberg on Windows); forward slashes.
"""
import shutil
import tempfile

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import leat
from leat import DeltaFormat
from leat.consumer import Consumer
from leat.checkpoint import JsonCheckpointStore

deltalake = pytest.importorskip("deltalake")


@pytest.fixture
def wh():
    d = tempfile.mkdtemp(prefix="leat_upd_")
    yield d.replace("\\", "/")
    shutil.rmtree(d, ignore_errors=True)


def _dedup_latest(arrow, key: str):
    """The 'current state' transform: keep the highest-`_offset` row per key.

    This is exactly the pattern that makes updates-as-appends correct -- sort by
    the leat offset and take the last row of each business key group."""
    return (pl.from_arrow(arrow)
              .sort("_offset")
              .group_by(key)
              .last()
              .sort(key))


# =========================================================================== #
# H1 -- updates-as-appends WORK (the supported pattern for mutable data today)
# =========================================================================== #
def test_h1_updates_as_appends_delta(wh):
    """v1 of key K, consume; v2 of K as a NEW append (new offset), consume again;
    a dedup-latest transform yields v2. This is CORRECT and SUPPORTED."""
    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")

    # v1: three users.
    src.append(pa.table({"user_id": [1, 2, 3], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")

    # Drain to a "silver" list of arrow batches (append-only history).
    seen = []
    while (b := c.poll()) is not None:
        seen.append(b.arrow())
        c.commit()
    assert c.position() == 2

    # v2 of user 2 arrives as a NEW append -> new higher offset.
    src.append(pa.table({"user_id": [2], "value": [999]}))
    b2 = c.poll()
    assert b2 is not None                       # the new append IS seen
    assert b2.num_rows == 1
    assert b2.offset == 3                        # new offset, advanced
    seen.append(b2.arrow())
    c.commit()

    # Dedup-to-latest over the whole append history yields the current state:
    # user 2's value is the v2 value, others unchanged.
    all_rows = pa.concat_tables(seen)
    current = _dedup_latest(all_rows, "user_id")
    assert current["user_id"].to_list() == [1, 2, 3]
    assert current["value"].to_list() == [10, 999, 30]   # user 2 updated to v2


def test_h1_updates_as_appends_iceberg(wh):
    """Same as above on Iceberg via connect()."""
    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()),
                        ("user_id", pa.int64()), ("value", pa.int64())])
    src = lt.create("db.events", schema)
    ckpt = lt.checkpoint

    src.append(pa.table({"user_id": [1, 2, 3], "value": [10, 20, 30]}))
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    seen = []
    while (b := c.poll()) is not None:
        seen.append(b.arrow())
        c.commit()

    src.append(pa.table({"user_id": [2], "value": [999]}))   # v2 as new append
    b2 = c.poll()
    assert b2 is not None and b2.num_rows == 1 and b2.offset == 3
    seen.append(b2.arrow())
    c.commit()

    current = _dedup_latest(pa.concat_tables(seen), "user_id")
    assert current["user_id"].to_list() == [1, 2, 3]
    assert current["value"].to_list() == [10, 999, 30]


# =========================================================================== #
# H2 -- in-place UPDATE is MISSED by the incremental reader (KNOWN LIMITATION)
# =========================================================================== #
def test_h2_inplace_update_is_missed_delta(wh):
    """Delta `DeltaTable.update(...)` mutates a row WITHOUT changing its `_offset`.
    leat's `read_since(committed_offset)` therefore never returns it: the max
    offset didn't advance, so `poll()` yields None. Documents a KNOWN LIMITATION
    (roadmap: CDC / Change Data Feed), not desired behavior."""
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))

    # Consume everything, commit the offset (=2, the max).
    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    silver = []
    while (b := c.poll()) is not None:
        silver.append(b.arrow())
        c.commit()
    assert c.position() == 2
    committed = c.position()

    # In-place mutation of an EXISTING row -- offset column is untouched.
    DeltaTable(src._uri).update(updates={"value": "999"}, predicate="k = 'b'")

    # The offset did NOT advance...
    assert src.latest_offset() == committed == 2
    # ...so the incremental reader misses the change entirely.
    data, new_off = src.read_since(committed)
    assert data.num_rows == 0                       # <-- change is SILENTLY MISSED
    assert c.poll() is None                          # consumer sees nothing new

    # read_all() (full-table dimension read) DOES reflect the new value -- it's
    # only the *incremental* cursor that misses it.
    allrows = pl.from_arrow(src.read_all())
    assert allrows.filter(pl.col("k") == "b")["value"].to_list() == [999]

    # An append-only silver still carries the STALE (pre-update) value for b.
    silver_state = pl.from_arrow(pa.concat_tables(silver))
    assert silver_state.filter(pl.col("k") == "b")["value"].to_list() == [20]


def test_h2_inplace_update_is_missed_iceberg(wh):
    """Iceberg `Table.overwrite(new_row, overwrite_filter=...)` replaces a row in
    place, reusing the same `_offset`. Same outcome: read_since misses it."""
    from pyiceberg.expressions import EqualTo

    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()),
                        ("k", pa.string()), ("value", pa.int64())])
    src = lt.create("db.events", schema)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))

    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    while (b := c.poll()) is not None:
        c.commit()
    committed = c.position()
    assert committed == 2

    # Overwrite row b in place, KEEPING its offset (=1) -> no new max offset.
    t = lt.catalog.load_table("db.events")
    t.overwrite(pa.table({"_offset": pa.array([1], pa.int64()),
                          "k": ["b"], "value": [999]}),
                overwrite_filter=EqualTo("k", "b"))

    assert src.latest_offset() == 2                  # offset did NOT advance
    data, _ = src.read_since(committed)
    assert data.num_rows == 0                        # <-- MISSED incrementally
    assert c.poll() is None

    # Full read shows the update; incremental read never surfaced it.
    allrows = pl.from_arrow(src.read_all())
    assert allrows.filter(pl.col("k") == "b")["value"].to_list() == [999]


# =========================================================================== #
# H3 -- DELETE is MISSED by the incremental reader (KNOWN LIMITATION)
# =========================================================================== #
def test_h3_delete_is_missed_delta(wh):
    """Delta `DeltaTable.delete(predicate)` removes a source row. The incremental
    reader misses it (nothing new to read), so an append-only silver still holds
    the deleted row. read_all() DOES shrink. Documents a KNOWN LIMITATION."""
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))

    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    silver = []
    while (b := c.poll()) is not None:
        silver.append(b.arrow())
        c.commit()
    committed = c.position()
    assert committed == 2

    # Delete a NON-max-offset row (k='b', offset=1) so latest_offset is unchanged.
    DeltaTable(src._uri).delete(predicate="k = 'b'")

    # latest_offset unchanged (max offset row 'c' still present)...
    assert src.latest_offset() == 2
    # ...and the incremental reader sees nothing new -> deletion not propagated.
    data, _ = src.read_since(committed)
    assert data.num_rows == 0                        # <-- DELETE MISSED incrementally
    assert c.poll() is None

    # read_all() DOES reflect the delete (row count dropped 3 -> 2).
    assert src.read_all().num_rows == 2
    assert "b" not in pl.from_arrow(src.read_all())["k"].to_list()

    # But an append-only silver STILL contains the deleted row -> stale downstream.
    silver_state = pl.from_arrow(pa.concat_tables(silver))
    assert "b" in silver_state["k"].to_list()        # deleted row persists in silver


def test_h3_delete_of_max_offset_row_moves_latest_offset_backwards_delta(wh):
    """Edge characterization: deleting the row that HOLDS the max offset makes
    `latest_offset()` go BACKWARDS (bounds are recomputed from surviving data).
    This is a real footgun -- a fresh 'latest' consumer would then re-read rows
    below the old committed offset. Documents the behavior; not desired."""
    from deltalake import DeltaTable

    src = DeltaFormat(f"{wh}/events")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))
    assert src.latest_offset() == 2                  # 'c' holds offset 2

    DeltaTable(src._uri).delete(predicate="k = 'c'")  # delete the max-offset row
    assert src.latest_offset() == 1                  # <-- max offset went BACKWARDS
    assert src.earliest_offset() == 0
    assert src.read_all().num_rows == 2


def test_h3_delete_is_missed_iceberg(wh):
    """Iceberg `Table.delete(delete_filter=...)` removes a row; incremental reader
    misses it, read_all() reflects it."""
    from pyiceberg.expressions import EqualTo

    lt = leat.connect(wh)
    schema = pa.schema([("_offset", pa.int64()),
                        ("k", pa.string()), ("value", pa.int64())])
    src = lt.create("db.events", schema)
    ckpt = lt.checkpoint
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))

    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    silver = []
    while (b := c.poll()) is not None:
        silver.append(b.arrow())
        c.commit()
    committed = c.position()
    assert committed == 2

    lt.catalog.load_table("db.events").delete(delete_filter=EqualTo("k", "b"))

    assert src.latest_offset() == 2                  # non-max row deleted
    data, _ = src.read_since(committed)
    assert data.num_rows == 0                        # <-- DELETE MISSED incrementally
    assert c.poll() is None

    assert src.read_all().num_rows == 2              # read_all reflects the delete
    silver_state = pl.from_arrow(pa.concat_tables(silver))
    assert "b" in silver_state["k"].to_list()        # stale row persists in silver


# =========================================================================== #
# Sink-side: does re-processing after a source change corrupt an append-only
# silver? Characterize (via seek() to force a re-read of already-consumed rows).
# =========================================================================== #
def test_sink_reprocessing_duplicates_appendonly_silver(wh):
    """If you rewind the consumer (seek) and re-run after a source change, an
    append-only silver DUPLICATES the re-read rows -- there is no key-based
    upsert on the sink. This is why the 'current state' is a dedup-latest READ
    concern (H1), not a sink guarantee. Characterizes the append-only sink."""
    src = DeltaFormat(f"{wh}/events")
    snk = DeltaFormat(f"{wh}/silver")
    ckpt = JsonCheckpointStore(f"{wh}/off.json")
    src.append(pa.table({"k": ["a", "b", "c"], "value": [10, 20, 30]}))

    c = Consumer(src, name="silver", checkpoint=ckpt, start="earliest")
    b = c.poll()
    snk.append(pa.table({"k": b.arrow().column("k"),
                         "value": b.arrow().column("value")}))
    c.commit()
    assert snk.read_all().num_rows == 3

    # Rewind and reprocess the same source rows (e.g. an operator re-runs after a
    # perceived source change). Append-only sink -> the rows are DUPLICATED.
    c.seek(None)
    b2 = c.poll()
    snk.append(pa.table({"k": b2.arrow().column("k"),
                         "value": b2.arrow().column("value")}))
    c.commit()
    assert snk.read_all().num_rows == 6              # <-- duplicated (append-only)

    # A dedup-latest read still recovers a sane current state per key, though.
    current = _dedup_latest(snk.read_all(), "k")
    assert current.height == 3


# =========================================================================== #
# Part 2 -- Delta Change Data Feed (CDF): a TRACTABLE CDC path (PROOF, not a
# feature). Proves leat COULD consume in-place updates + deletes on Delta by
# reading the CDF stream instead of the offset column.
# =========================================================================== #
def test_delta_cdf_captures_update_and_delete(wh):
    """Empirically confirm delta-rs (1.6.2) CDF yields row-level changes with
    `_change_type` in {insert, update_preimage, update_postimage, delete} once
    `delta.enableChangeDataFeed=true`. This is the API that unblocks real
    update/delete support on Delta."""
    from deltalake import DeltaTable, write_deltalake

    path = f"{wh}/cdf_tbl"
    write_deltalake(path,
                    pa.table({"_offset": pa.array([0, 1, 2], pa.int64()),
                              "k": ["a", "b", "c"], "value": [10, 20, 30]}),
                    mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})
    dt = DeltaTable(path)
    assert dt.version() == 0

    dt.update(updates={"value": "99"}, predicate="k = 'b'")   # in-place UPDATE
    dt.delete(predicate="k = 'c'")                            # DELETE
    dt = DeltaTable(path)
    assert dt.version() == 2

    cdf = pl.from_arrow(dt.load_cdf(starting_version=1).read_all())
    assert "_change_type" in cdf.columns
    types = set(cdf["_change_type"].to_list())
    # The UPDATE surfaces as a pre/post pair; the DELETE as a delete row.
    assert {"update_preimage", "update_postimage", "delete"} <= types

    # The update's new value and the deleted key are both present at row level.
    post = cdf.filter(pl.col("_change_type") == "update_postimage")
    assert post.filter(pl.col("k") == "b")["value"].to_list() == [99]
    deleted = cdf.filter(pl.col("_change_type") == "delete")
    assert deleted["k"].to_list() == ["c"]


def test_delta_cdf_read_changes_adapter_sketch(wh):
    """PROOF-of-path: a sketch of the `read_changes(since_version)` adapter method
    leat WOULD add to DeltaFormat to consume updates+deletes. It classifies CDF
    rows into inserts / updates / deletes so a downstream MERGE-into-silver could
    apply them. NOT wired into leat/ -- this is a design proof only.

    Returns (inserts, updates, deletes, latest_version) where:
      - inserts: rows with _change_type == 'insert'
      - updates: the POST-image rows (the new value to upsert on the business key)
      - deletes: the rows to remove (by business key)
    """
    from deltalake import DeltaTable, write_deltalake

    path = f"{wh}/cdf_tbl2"
    write_deltalake(path,
                    pa.table({"_offset": pa.array([0, 1, 2], pa.int64()),
                              "k": ["a", "b", "c"], "value": [10, 20, 30]}),
                    mode="overwrite",
                    configuration={"delta.enableChangeDataFeed": "true"})

    dt = DeltaTable(path)
    dt.update(updates={"value": "99"}, predicate="k = 'b'")
    dt.delete(predicate="k = 'c'")
    dt = DeltaTable(path)

    def read_changes(table_path: str, since_version: int):
        t = DeltaTable(table_path)
        latest = t.version()
        if since_version > latest:
            empty = pl.DataFrame()
            return empty, empty, empty, latest
        cdf = pl.from_arrow(t.load_cdf(starting_version=since_version + 1).read_all())
        biz = [c for c in cdf.columns
               if c not in ("_change_type", "_commit_version", "_commit_timestamp")]
        inserts = cdf.filter(pl.col("_change_type") == "insert").select(biz)
        updates = cdf.filter(pl.col("_change_type") == "update_postimage").select(biz)
        deletes = cdf.filter(pl.col("_change_type") == "delete").select(biz)
        return inserts, updates, deletes, latest

    inserts, updates, deletes, latest = read_changes(path, since_version=0)
    assert latest == 2
    assert inserts.height == 0
    assert updates.filter(pl.col("k") == "b")["value"].to_list() == [99]   # upsert b->99
    assert deletes["k"].to_list() == ["c"]                                 # remove c

    # Applying the CDC changes to a "current state" silver yields the CORRECT
    # post-update/post-delete state -- exactly what the offset reader could NOT do.
    silver = pl.DataFrame({"k": ["a", "b", "c"], "value": [10, 20, 30]})
    silver = (silver.join(updates, on="k", how="left", suffix="_new")
                    .with_columns(pl.coalesce(["value_new", "value"]).alias("value"))
                    .select(["k", "value"]))
    silver = silver.filter(~pl.col("k").is_in(deletes["k"].to_list()))
    silver = silver.sort("k")
    assert silver["k"].to_list() == ["a", "b"]           # c deleted
    assert silver["value"].to_list() == [10, 99]         # b updated
