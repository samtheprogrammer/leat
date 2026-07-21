"""Delta Lake adapter (control layer) built on delta-rs (`deltalake`).

Mirrors IcebergFormat exactly: same 5-method TableFormat surface, same
monotonic offset-column strategy. delta-rs is Rust-core, Arrow-native, and
needs no JVM and no catalog — a Delta table is just a path (local dir or an
object-store URI). Incremental reads use the same offset column + predicate
pushdown: `to_pyarrow_table(filters=[(offset_col, ">", offset)])` prunes the
data files delta-rs scans via per-file min/max stats. The offset column IS the
Kafka offset.
"""
from __future__ import annotations
import random
import time
from typing import Optional, Tuple, Dict
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake, CommitProperties

_OFF_PREFIX = "leat.offset."

# delta-rs history() `operation` values that CHANGE existing rows (an append-mode
# offset cursor cannot see these). `WRITE` (plain append) and `OPTIMIZE`
# (compaction) are benign. A `WRITE` that carries remove-actions (an
# overwrite/replaceWhere) IS row-changing — detected via operationMetrics below.
# Verified empirically against deltalake 1.6.2 (operations: WRITE/UPDATE/DELETE/
# MERGE/OPTIMIZE).
_ROW_CHANGING_OPS = {"DELETE", "UPDATE", "MERGE"}

# Substrings identifying a delta-rs optimistic-concurrency write clash.
_CONFLICT_HINTS = ("commitfailed", "database is locked", "concurrent",
                   "conflict", "version already exists", "metadata changed")


def _is_conflict(exc: Exception) -> bool:
    msg = f"{type(exc).__name__} {exc}".lower()
    return any(h in msg for h in _CONFLICT_HINTS)


def _mint_offsets(data: pa.Table, off_col: str, base: int) -> pa.Table:
    """Return ``data`` with a fresh int64 ``off_col`` = base+1 .. base+len in row order."""
    n = data.num_rows
    minted = pa.array(range(base + 1, base + 1 + n), type=pa.int64())
    return data.append_column(off_col, minted)


class DeltaFormat:
    def __init__(self, table_uri: str, offset_column: str = "_offset",
                 storage_options: Optional[dict] = None):
        self._uri = str(table_uri)
        self._off = offset_column
        self._storage = storage_options

    def _table(self) -> Optional[DeltaTable]:
        # A Delta table is path-based; it "exists" once a _delta_log is written.
        if not DeltaTable.is_deltatable(self._uri, storage_options=self._storage):
            return None
        return DeltaTable(self._uri, storage_options=self._storage)

    def _bounds(self):
        t = self._table()
        if t is None:
            return None, None
        col = t.to_pyarrow_table(columns=[self._off]).column(self._off)
        if len(col) == 0:
            return None, None
        return pc.min(col).as_py(), pc.max(col).as_py()

    def earliest_offset(self) -> Optional[int]:
        return self._bounds()[0]

    def latest_offset(self) -> Optional[int]:
        return self._bounds()[1]

    def read_since(self, offset: Optional[int],
                   hi: Optional[int] = None) -> Tuple[pa.Table, Optional[int]]:
        """Rows with ``offset < _offset`` (all if None), optionally also ``<= hi``.

        The optional ``hi`` bounds the read to a contiguous offset RANGE so
        delta-rs prunes files on both ends via min/max stats — used by the elastic
        bucket loop. ``hi=None`` preserves the original open-ended scan.
        """
        t = self._table()
        if t is None:
            return pa.table({}), offset
        filters = []
        if offset is not None:
            filters.append((self._off, ">", offset))
        if hi is not None:
            filters.append((self._off, "<=", hi))
        if filters:
            data = t.to_pyarrow_table(filters=filters)
        else:
            data = t.to_pyarrow_table()
        new_off = pc.max(data.column(self._off)).as_py() if data.num_rows else offset
        return data, new_off

    def append(self, data: pa.Table, offsets: Optional[Dict[str, int]] = None) -> None:
        # deltalake 1.6.2: custom_metadata rides the commit via CommitProperties,
        # so the offset commits atomically with the appended data.
        props = None
        if offsets:
            meta = {f"{_OFF_PREFIX}{k}": str(v) for k, v in offsets.items()}
            props = CommitProperties(custom_metadata=meta)
        if self._off in data.column_names:
            # Explicit `_offset` present (low-level caller) — use as-is.
            write_deltalake(self._uri, data, mode="append",
                            storage_options=self._storage, commit_properties=props)
            return
        _t = self._table()
        if _t is not None and self._off not in _t.schema().to_arrow().names:
            # Existing table deliberately has no offset column (dimension table) —
            # don't inject one; append the data exactly as given.
            write_deltalake(self._uri, data, mode="append",
                            storage_options=self._storage, commit_properties=props)
            return
        # Auto-mint: leat owns `_offset`. Reading latest_offset() then appending is
        # not atomic, so on a concurrent-write conflict RE-READ, RE-MINT, retry with
        # jittered backoff (mirrors elastic._append_with_retry).
        tries, base_sleep, cap = 8, 0.05, 2.0
        last = None
        for attempt in range(tries):
            base = self.latest_offset()
            base = -1 if base is None else base
            minted = _mint_offsets(data, self._off, base)
            try:
                write_deltalake(self._uri, minted, mode="append",
                                storage_options=self._storage, commit_properties=props)
                return
            except Exception as e:  # noqa: BLE001 — backend commit/lock errors
                if not _is_conflict(e) or attempt == tries - 1:
                    raise
                last = e
                sleep = min(cap, base_sleep * (2 ** attempt)) * (0.5 + random.random())
                time.sleep(sleep)
        raise last

    def read_offsets(self) -> Dict[str, int]:
        # history() is newest-first; custom_metadata surfaces as top-level keys
        # in each commitInfo dict. First (most recent) value per key wins.
        t = self._table()
        if t is None:
            return {}
        out: Dict[str, int] = {}
        for entry in t.history():
            for key, val in entry.items():
                if key.startswith(_OFF_PREFIX):
                    name = key[len(_OFF_PREFIX):]
                    if name not in out:
                        out[name] = int(val)
        return out

    def read_all(self) -> pa.Table:
        t = self._table()
        if t is None:
            return pa.table({})
        return t.to_pyarrow_table()

    # --- mutation awareness (safety check + upsert) --------------------------

    def current_marker(self) -> Optional[int]:
        """Opaque cursor for the source's *commit* history (the current Delta
        version), remembered by the consumer so it can tell whether NEW
        row-changing commits appeared. `None` when the table is not yet created."""
        t = self._table()
        return None if t is None else t.version()

    def nonappend_ops_since(self, marker: Optional[int]) -> list:
        """Row-changing operations (DELETE/UPDATE/MERGE, or a WRITE that removes
        files = overwrite) committed AFTER Delta version `marker`. The offset
        cursor cannot see these, so append mode surfaces them.

        `history()` is newest-first; we take entries with version > marker.
        Compaction (`OPTIMIZE`) and plain appends (`WRITE` with no removed files)
        are NOT flagged — telling genuine mutation from benign compaction is the
        whole point of the check.
        """
        t = self._table()
        if t is None:
            return []
        ops: list = []
        for entry in t.history():
            ver = entry.get("version")
            if marker is not None and ver is not None and ver <= marker:
                continue
            op = entry.get("operation")
            if op in _ROW_CHANGING_OPS:
                ops.append(op)
            elif op == "WRITE":
                removed = (entry.get("operationMetrics") or {}).get("num_removed_files")
                try:
                    if removed is not None and int(removed) > 0:
                        ops.append("overwrite")   # a WRITE that replaced existing files
                except (TypeError, ValueError):
                    pass
        ops.reverse()                             # chronological, oldest→newest
        return ops

    def upsert(self, data: pa.Table, keys, offsets: Optional[Dict[str, int]] = None) -> None:
        """MERGE `data` into the table by business `keys` (update matching rows,
        insert new) so the table holds *current state* and reprocessing the same
        batch is idempotent (a no-op merge). Creates the table on first upsert.

        The offset (if given) rides the SAME merge commit via `commit_properties`,
        so it advances atomically with the merge (true exactly-once); with merge
        idempotency this is more forgiving than append. Verified against
        deltalake 1.6.2.
        """
        keys = [keys] if isinstance(keys, str) else list(keys)
        props = None
        if offsets:
            meta = {f"{_OFF_PREFIX}{k}": str(v) for k, v in offsets.items()}
            props = CommitProperties(custom_metadata=meta)
        t = self._table()
        # Auto-mint _offset if leat owns it and the caller didn't supply one.
        want_offset = (t is None) or (self._off in t.schema().to_arrow().names)
        if want_offset and self._off not in data.column_names:
            base = self.latest_offset()
            base = -1 if base is None else base
            data = _mint_offsets(data, self._off, base)
        if t is None:
            # First upsert = create + insert-all (nothing to match yet).
            write_deltalake(self._uri, data, mode="append",
                            storage_options=self._storage, commit_properties=props)
            return
        pred = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (t.merge(data, predicate=pred, source_alias="s", target_alias="t",
                 commit_properties=props)
          .when_matched_update_all()
          .when_not_matched_insert_all()
          .execute())
