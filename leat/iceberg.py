"""Iceberg adapter (control layer) built on PyIceberg.

Incremental reads use a monotonic offset column + Iceberg predicate pushdown,
so only the data files containing new offsets are scanned (Iceberg prunes the
rest via per-file min/max stats). The offset column IS the Kafka offset.
"""
from __future__ import annotations
import random
import time
from typing import Optional, Tuple, Dict
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import And, GreaterThan, LessThanOrEqual

_OFF_PREFIX = "leat.offset."

# Iceberg snapshot summary operations that CHANGE existing rows (an append-mode
# offset cursor cannot see these). `append` is benign (new rows -> higher offsets);
# `replace` is compaction/rewrite (same rows, new files) and is ALSO benign — the
# whole point of the safety check is to flag genuine mutations, not compaction.
# Verified empirically against pyiceberg 0.11.1 (Operation enum: append/replace/
# overwrite/delete; .value is the lowercase string).
_ROW_CHANGING_OPS = {"overwrite", "delete"}

# Substrings that identify an Iceberg/SQLite optimistic-concurrency commit clash.
# On a clash we RE-READ latest_offset, RE-MINT, and retry (mirrors elastic.py).
_CONFLICT_HINTS = ("commitfailed", "database is locked", "concurrent",
                   "conflict", "stale")


def _is_conflict(exc: Exception) -> bool:
    msg = f"{type(exc).__name__} {exc}".lower()
    return any(h in msg for h in _CONFLICT_HINTS)


def _mint_offsets(data: pa.Table, off_col: str, base: int) -> pa.Table:
    """Return ``data`` with a fresh int64 ``off_col`` = base+1 .. base+len in row order."""
    n = data.num_rows
    minted = pa.array(range(base + 1, base + 1 + n), type=pa.int64())
    return data.append_column(off_col, minted)


class IcebergFormat:
    def __init__(self, catalog, identifier: str, offset_column: str = "_offset"):
        self._catalog = catalog
        self._id = identifier
        self._off = offset_column

    def _table(self):
        return self._catalog.load_table(self._id)

    def _bounds(self):
        col = self._table().scan().to_arrow().column(self._off)
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

        The optional ``hi`` bounds the read to a contiguous offset RANGE so Iceberg
        can prune files on both ends via min/max stats — used by the elastic
        bucket loop. Keeping ``hi=None`` preserves the original open-ended scan.
        """
        t = self._table()
        lo_expr = None if offset is None else GreaterThan(self._off, offset)
        hi_expr = None if hi is None else LessThanOrEqual(self._off, hi)
        if lo_expr is not None and hi_expr is not None:
            scan = t.scan(row_filter=And(lo_expr, hi_expr))
        elif lo_expr is not None:
            scan = t.scan(row_filter=lo_expr)
        elif hi_expr is not None:
            scan = t.scan(row_filter=hi_expr)
        else:
            scan = t.scan()
        data = scan.to_arrow()
        new_off = pc.max(data.column(self._off)).as_py() if data.num_rows else offset
        return data, new_off

    def append(self, data: pa.Table, offsets: Optional[Dict[str, int]] = None) -> None:
        # Every Iceberg append creates a snapshot; snapshot_properties ride that
        # SAME snapshot's summary, so the offset commits atomically with the data.
        props = {f"{_OFF_PREFIX}{k}": str(v) for k, v in (offsets or {}).items()}
        if self._off in data.column_names:
            # Explicit `_offset` present (low-level caller / benchmark) — use as-is.
            self._table().append(data, snapshot_properties=props)
            return
        if self._off not in self._table().schema().column_names:
            # Table deliberately has no offset column (e.g. a dimension table) —
            # don't inject one; append the data exactly as given.
            self._table().append(data, snapshot_properties=props)
            return
        # Auto-mint: leat owns `_offset` (like Kafka assigning offsets on produce).
        # Reading latest_offset() then appending is not atomic, so on a commit
        # conflict RE-READ, RE-MINT, and retry with jittered backoff (mirrors
        # elastic._append_with_retry). Single-writer bronze never hits this.
        tries, base_sleep, cap = 8, 0.05, 2.0
        last = None
        for attempt in range(tries):
            base = self.latest_offset()
            base = -1 if base is None else base
            minted = _mint_offsets(data, self._off, base)
            try:
                self._table().append(minted, snapshot_properties=props)
                return
            except Exception as e:  # noqa: BLE001 — backend commit/lock errors
                if not _is_conflict(e) or attempt == tries - 1:
                    raise
                last = e
                sleep = min(cap, base_sleep * (2 ** attempt)) * (0.5 + random.random())
                time.sleep(sleep)
        raise last

    def read_offsets(self) -> Dict[str, int]:
        # Custom props live in each snapshot's summary.additional_properties and
        # are NOT accumulated across snapshots — scan newest→oldest, first wins.
        # A not-yet-created sink (easy-path auto-create) simply has no offsets.
        try:
            table = self._table()
        except Exception:
            return {}
        out: Dict[str, int] = {}
        for snap in reversed(table.snapshots()):
            for key, val in snap.summary.additional_properties.items():
                if key.startswith(_OFF_PREFIX):
                    name = key[len(_OFF_PREFIX):]
                    if name not in out:
                        out[name] = int(val)
        return out

    def read_all(self) -> pa.Table:
        return self._table().scan().to_arrow()

    # --- mutation awareness (safety check + upsert) --------------------------

    def current_marker(self) -> Optional[int]:
        """Opaque cursor for the source's *commit* history (the current snapshot
        id), remembered by the consumer so it can tell whether NEW row-changing
        commits appeared. `None` when the table is empty / not yet created."""
        try:
            snap = self._table().current_snapshot()
        except Exception:
            return None
        return snap.snapshot_id if snap is not None else None

    def nonappend_ops_since(self, marker: Optional[int]) -> list:
        """Row-changing operations (UPDATE/DELETE/overwrite) committed AFTER
        `marker` (a snapshot id from `current_marker`). The offset cursor cannot
        see these, so append mode surfaces them instead of silently missing them.

        Walks `snapshots()` (chronological, oldest→newest) from just after the
        snapshot whose id == `marker`, and collects `summary.operation` values in
        `_ROW_CHANGING_OPS`. Compaction (`replace`) and `append` are NOT flagged —
        distinguishing genuine mutation from benign compaction is the whole point.
        """
        try:
            snaps = self._table().snapshots()
        except Exception:
            return []
        ops: list = []
        seen_marker = marker is None            # None => consider the whole history
        for snap in snaps:
            if not seen_marker:
                if snap.snapshot_id == marker:
                    seen_marker = True
                continue
            op = snap.summary.operation
            op_str = getattr(op, "value", op)   # Operation enum -> its lowercase string
            if op_str in _ROW_CHANGING_OPS:
                ops.append(op_str)
        return ops

    def upsert(self, data: pa.Table, keys, offsets: Optional[Dict[str, int]] = None) -> None:
        """MERGE `data` into the table by business `keys` (update matching rows,
        insert new) — so the table holds *current state* and reprocessing the same
        batch is idempotent (a no-op merge). Creates the table on first upsert.

        The offset (if given) rides the SAME upsert commit's `snapshot_properties`,
        so it advances atomically with the merge (true exactly-once); combined with
        merge idempotency this is more forgiving than append (re-merging a batch
        leaves the table unchanged). Verified against pyiceberg 0.11.1.
        """
        keys = [keys] if isinstance(keys, str) else list(keys)
        props = {f"{_OFF_PREFIX}{k}": str(v) for k, v in (offsets or {}).items()}
        # Auto-mint _offset if leat owns it and the caller didn't supply one, so
        # inserted rows get a fresh monotonic offset (same rule as append()).
        try:
            table = self._table()
        except Exception:
            table = None
        if table is None:
            # First upsert = create + insert-all (there is nothing to match).
            raise RuntimeError(
                "upsert requires the sink table to exist; the pipeline auto-creates it")
        if self._off in table.schema().column_names and self._off not in data.column_names:
            base = self.latest_offset()
            base = -1 if base is None else base
            data = _mint_offsets(data, self._off, base)
        # PyIceberg's upsert casts source→target by POSITION and requires identical
        # column ORDER, so reorder `data` to the table's schema (auto-minting puts
        # `_offset` last, but the table may declare it first).
        target = [f for f in table.schema().column_names if f in data.column_names]
        if target and list(data.column_names) != target:
            data = data.select(target)
        table.upsert(data, join_cols=keys, snapshot_properties=props)
