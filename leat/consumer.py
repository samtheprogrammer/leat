"""Kafka-style consumer over a table (Iceberg/Delta).

Offsets are the table's monotonic offset column. `poll()` returns the next batch
of changes since the committed offset; `commit()` advances and persists it.
"""
from __future__ import annotations
import logging
from typing import Optional, Union
import pyarrow as pa

from .format import TableFormat
from .checkpoint import JsonCheckpointStore

logger = logging.getLogger("leat")


class Batch:
    """A batch of changes since the last offset. Hand it to Polars or DuckDB (Arrow-native)."""
    def __init__(self, data: pa.Table, offset: Optional[int]):
        self.data = data
        self.offset = offset

    def arrow(self) -> pa.Table:
        return self.data

    def pl(self):
        import polars as pl
        return pl.from_arrow(self.data)

    @property
    def num_rows(self) -> int:
        return self.data.num_rows


class Consumer:
    def __init__(self, source: TableFormat, name: str, checkpoint: JsonCheckpointStore,
                 start: Union[str, int] = "latest", delivery: str = "exactly_once",
                 on_change: str = "warn"):
        self._src = source
        self._name = name
        self._ckpt = checkpoint
        self._delivery = delivery
        self._pending: Optional[int] = None
        self._offset = self._resolve_start(start)
        # --- mutation-awareness safety (append mode can't see UPDATE/DELETE) ---
        if on_change not in ("warn", "error", "ignore"):
            raise ValueError(
                f"invalid on_change: {on_change!r} (expected 'warn'/'error'/'ignore')")
        self._on_change = on_change
        # Remember the source's current commit marker so poll() can detect NEW
        # row-changing commits the offset cursor would silently miss.
        self._marker = self._read_marker()
        self._warned = False                      # emit the warning ONCE, not per poll

    def _read_marker(self):
        fn = getattr(self._src, "current_marker", None)
        return fn() if callable(fn) else None

    def _check_mutations(self) -> None:
        """If the source got row-changing commits (UPDATE/DELETE/overwrite/MERGE)
        that append mode's offset cursor can't capture, act per `on_change`."""
        if self._on_change == "ignore":
            return
        fn = getattr(self._src, "nonappend_ops_since", None)
        if not callable(fn):
            return
        ops = fn(self._marker)
        self._marker = self._read_marker()        # advance the marker past what we saw
        if not ops:
            return
        kinds = "/".join(sorted(set(o.upper() for o in ops)))
        msg = (f"leat: source '{self._name}' had {kinds} commits that append mode "
               f"cannot capture — use mode='upsert' (updates) or mode='cdc' (deletes). "
               f"Set on_change='ignore' to silence.")
        if self._on_change == "error":
            raise RuntimeError(msg)
        if not self._warned:                      # warn: once per detected change
            logger.warning(msg)
            self._warned = True

    def _resolve_start(self, start) -> Optional[int]:
        committed = self._ckpt.get(self._name)
        if committed is not None:
            return committed                      # continue from committed offset
        if start == "earliest":
            return None                           # before all rows
        if start == "latest":
            return self._src.latest_offset()      # only new data going forward
        if isinstance(start, int):
            return start
        raise ValueError(f"invalid start: {start!r}")

    def poll(self) -> Optional[Batch]:
        self._check_mutations()                   # surface unseen UPDATE/DELETE
        data, new_off = self._src.read_since(self._offset)
        if data.num_rows == 0:
            return None
        self._pending = new_off
        return Batch(data, new_off)

    def commit(self) -> None:
        if self._pending is not None:
            self._offset = self._pending
            self._ckpt.set(self._name, self._offset)

    # --- Kafka-style offset controls ---
    def position(self) -> Optional[int]:
        return self._offset

    def lag(self) -> int:
        latest = self._src.latest_offset()
        if latest is None:
            return 0
        cur = self._offset if self._offset is not None else latest - self._count_all()
        return max(0, latest - cur)

    def _count_all(self) -> int:
        # rows available when starting from earliest (for lag reporting)
        data, _ = self._src.read_since(None)
        return data.num_rows

    def seek(self, offset: Optional[int]) -> None:
        self._offset = offset

    def reset(self) -> None:
        self._offset = None                       # back to earliest -> full reprocess
