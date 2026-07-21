# Updates, deletes, and the CDC path — an honest scope boundary

leat's incremental cursor is a **monotonic `_offset` column** plus a
`read_since(offset)` that returns rows with `_offset > committed_offset`
(PyIceberg 0.11 has no snapshot-diff scan, so this column *is* the cursor). That
design is append-optimal — and it has a precise, testable boundary when source
rows are **mutated or deleted** rather than appended. This doc states exactly
what leat handles today, what it does not, and the concrete path to closing the
gap. Characterization tests: `tests/test_updates_deletes.py`.

## What leat handles today

- **Append-only ingestion** — the core case. New rows → higher offsets →
  `read_since` returns them → exactly-once into silver/gold.
- **Updates-as-appends** — emitting a **new row** (new higher offset) for a
  changed business key. The append is seen normally; a **dedup-to-latest**
  transform (`sort by _offset` → `group_by(key).last()`) yields the correct
  current state. This is the supported pattern for mutable data today, and it
  works on both Iceberg and Delta (tests `test_h1_*`). If your upstream is a
  CDC/append log or you can model changes as versioned appends, leat is safe.
- **`mode="upsert"` — inserts + updates by key (SHIPPED).**
  `@lt.model(source, sink, mode="upsert", key=["id"])` reads incrementally
  (the same cheap offset scan as append — it catches updates-expressed-as-appends
  at append cost) and **MERGES the transform output into the sink by business
  key** instead of appending: matching rows are updated, new keys inserted, so the
  sink holds *current state* rather than history. The sink primitive is
  `format.upsert(data, keys, offsets=...)` — Iceberg `Table.upsert(join_cols=...)`
  and Delta `MERGE` — with the offset embedded in the **same** merge commit
  (`snapshot_properties` / `commit_properties`), so it advances atomically.
  Because merge-by-key is **idempotent**, reprocessing the same batch is a no-op
  (no dupes) → exactly-once is more forgiving than append. Within a single batch,
  a repeated key is deduped to its highest-offset (latest) row before the merge.
  Tests: `tests/test_modes.py` (`test_upsert_*`, `test_model_upsert_end_to_end_*`,
  parity Iceberg == Delta). Still offset-driven, so **in-place-no-append** updates
  (below) remain `cdc`-mode territory.
- **`on_change` — append-mode safety against unseen mutations (SHIPPED).**
  A `Consumer` / `@lt.model(..., on_change=...)` now remembers the source's commit
  marker (Iceberg snapshot id / Delta version) and, on each `poll()`, checks for
  **row-changing** commits the offset cursor can't see (below). Policy:
  `"warn"` (default, non-breaking — logs a one-line warning ONCE per detected
  change), `"error"` (raises, Spark-strict), `"ignore"` (silent, the prior
  behavior). It distinguishes genuine mutation from **benign compaction**:
  Iceberg flags `overwrite`/`delete` but not `replace` (rewrite/compaction) or
  `append`; Delta flags `DELETE`/`UPDATE`/`MERGE` and `WRITE`s that carry
  remove-actions (overwrite) but not `OPTIMIZE` or plain appends. Tests:
  `tests/test_modes.py` (`test_safety_*`).

## What leat does NOT handle (real, documented gaps)

These are **incremental-reader** limitations, confirmed empirically against the
installed stack (`pyiceberg 0.11.1`, `deltalake 1.6.2`). Note: they are no longer
**silent** — the `on_change` safety check (above) now surfaces them (warn/error)
so append mode can't quietly miss them; but *capturing* the change still needs
`upsert` mode (for updates-as-appears) or the `cdc` path (for in-place / deletes).

- **In-place UPDATE is missed by the offset cursor** (H2). A Delta
  `DeltaTable.update(...)` or an Iceberg `Table.overwrite(row, overwrite_filter=...)`
  mutates an existing row **without changing its `_offset`**. `latest_offset()`
  does not advance, so `read_since(committed)` returns **0 rows** and `poll()`
  yields `None`. (`read_all()` *does* show the new value — only the incremental
  cursor misses it.) It is now **flagged** by `on_change` (the source's
  snapshot/version advanced with a row-changing op), but capturing it is
  `cdc`-mode territory. Tests: `test_h2_inplace_update_is_missed_{delta,iceberg}`,
  `test_safety_update_warns_iceberg`.

- **DELETE is missed by the offset cursor** (H3). A Delta
  `DeltaTable.delete(predicate)` or Iceberg `Table.delete(delete_filter=...)`
  removes a source row. The incremental reader sees nothing new. It is now
  **flagged** by `on_change`, but actually propagating the delete downstream is
  the remaining `cdc` roadmap item. Tests:
  `test_h3_delete_is_missed_{delta,iceberg}`, `test_safety_delete_warns_delta`.

- **Footgun: deleting the max-offset row moves `latest_offset()` BACKWARDS.**
  Bounds are recomputed from surviving data (`min/max` of the `_offset` column),
  so deleting the highest-offset row lowers `latest_offset()`. A fresh
  `start="latest"` consumer would then resume *below* the previously committed
  offset and re-read rows. Test:
  `test_h3_delete_of_max_offset_row_moves_latest_offset_backwards_delta`.

- **Append-*mode* sink has no key upsert** (but `mode="upsert"` does). In the
  default `mode="append"`, reprocessing already-consumed rows (e.g. an operator
  rewinds and re-runs) **duplicates** them in silver — "current state" is then a
  *read-time* dedup-latest concern (H1). **`mode="upsert"` closes this at the sink**
  via merge-by-key (idempotent: reprocessing leaves the sink unchanged). Tests:
  `test_sink_reprocessing_duplicates_appendonly_silver` (append),
  `test_upsert_primitive_current_state_and_idempotent_{delta,iceberg}` (upsert).

Plain verdict: **leat today = append-only + updates-as-appends + `upsert`
(insert/update merge-by-key), with `on_change` surfacing unseen in-place
UPDATE/DELETE instead of silently missing them. Actually *capturing* in-place
mutation / deletes via the offset reader remains a documented gap → the `cdc`
path below.**

## The CDC path forward

### Delta Change Data Feed — VIABLE (the way to close the gap)

`deltalake 1.6.2` fully supports CDF. Enable it at table creation with
`configuration={"delta.enableChangeDataFeed": "true"}`, then read row-level
changes:

```python
DeltaTable(path).load_cdf(starting_version=v).read_all()   # -> Arrow RecordBatchReader
```

Empirically (tests `test_delta_cdf_*`), an in-place `update` + a `delete`
surface as CDF rows with a `_change_type` column taking the values
`insert`, `update_preimage`, `update_postimage`, `delete`, plus
`_commit_version` / `_commit_timestamp`. That is exactly the signal the offset
reader lacks.

**Proposed adapter (design proof only — NOT wired into `leat/`):** a
`read_changes(since_version)` on `DeltaFormat` that reads the CDF from
`since_version + 1`, splits rows by `_change_type` into inserts / update
post-images / deletes, and lets a downstream **MERGE-into-silver** apply them by
business key. The proof in `test_delta_cdf_read_changes_adapter_sketch`
reconstructs the correct post-update/post-delete state that the offset reader
could not. The version cursor (`_commit_version`) would live alongside — or
replace — the offset cursor for Delta CDF sources. This is a scoped follow-on
(roadmap item "CDC deletes / Change Data Feed"), not built here because it is a
design decision (offset cursor vs. CDF cursor, exactly-once across CDF batches).
Note the **sink MERGE half is now solved**: `format.upsert(data, keys)` (see
`mode="upsert"` above) is exactly the "MERGE-into-silver by business key" this
sketch needs — so the remaining CDF work is the *read* side (classify
insert/update/delete from `_change_type`) plus delete-application, not the merge.

### Iceberg — BLOCKED by PyIceberg 0.11.x

`pyiceberg 0.11.1` has **no** incremental/CDC scan: no
`incremental_append_scan`, no snapshot-range scan, no equality-delete read API.
`Table` exposes only a single `scan()` (plus `delete`/`overwrite`/`upsert` for
*writing* mutations, and `inspect` for metadata). So there is no CDF-equivalent
on Iceberg today — the offset-column approach is the only incremental path, and
it inherits the H2/H3 gaps. Closing them on Iceberg requires either a newer
PyIceberg that adds incremental/snapshot-diff scans (or equality-delete reads),
or the Java/Spark Iceberg path (out of scope for a single-node, JVM-free tool).

## Bottom line

- **Safe for teams whose source data is append-only, CDC-log-shaped, or
  insert/update** — model changes as versioned appends and use `mode="upsert",
  key=[...]` to merge them into current state (idempotent). ✅
- **No longer *silent* on in-place `UPDATE`/`DELETE`** — `on_change` (default
  `"warn"`) surfaces row-changing source commits the offset cursor can't see;
  `"error"` makes it strict. *Capturing* (not just flagging) in-place mutation /
  deletes via the offset reader remains a documented gap. ❌→⚠️
- **Delta CDF is a viable, proven path** to real update/delete *capture*
  (`load_cdf` + a `_change_type`-aware `read_changes`); the sink-merge half is
  already shipped as `upsert`. **Iceberg is blocked** until PyIceberg ships an
  incremental/CDC scan (or a manifest-diff spike).
