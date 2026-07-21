"""The control layer: an adapter over an open table format (Iceberg, Delta).

Offsets are monotonic integers, Kafka-style. The compute layer (Polars/DuckDB)
never touches this interface — it only sees Arrow.
"""
from __future__ import annotations
from typing import Protocol, Optional, Tuple, Dict
import pyarrow as pa


class TableFormat(Protocol):
    def earliest_offset(self) -> Optional[int]:
        """Smallest offset currently retained, or None if empty."""

    def latest_offset(self) -> Optional[int]:
        """Largest committed offset, or None if empty."""

    def read_since(self, offset: Optional[int]) -> Tuple[pa.Table, Optional[int]]:
        """Rows with offset_column > `offset` (all rows if offset is None).
        Returns (data, max_offset_in_data)."""

    def append(self, data: pa.Table, offsets: Optional[Dict[str, int]] = None) -> None:
        """Append rows (a commit / new snapshot).

        If `offsets` is given, each `{name: offset}` is embedded in THIS commit's
        own metadata as a `leat.offset.<name>` property — so the offset advances
        iff the append commits (one atomic transaction → true exactly-once).
        Default None keeps the plain-append behavior.
        """

    def read_offsets(self) -> Dict[str, int]:
        """Latest committed offset per key, read from commit metadata.

        Scans commit history newest→oldest and takes the first (most recent)
        value seen per `leat.offset.*` key (each key is single-writer, so its
        last commit wins). The `leat.offset.` prefix is stripped from the keys.
        Empty dict if the table is empty / has no embedded offsets.
        """

    def read_all(self) -> pa.Table:
        """Full current table — for dimension / reference reads (e.g. stream-table joins)."""

    def upsert(self, data: pa.Table, keys, offsets: Optional[Dict[str, int]] = None) -> None:
        """MERGE `data` into the table by business `keys` (update matching rows,
        insert new) so the sink holds CURRENT STATE and reprocessing a batch is
        idempotent. `offsets` (if given) rides the same merge commit atomically."""

    def current_marker(self) -> Optional[int]:
        """Opaque cursor for this table's COMMIT history (Iceberg snapshot id /
        Delta version). Remembered by a consumer to detect new row-changing
        commits that the append-mode offset cursor can't see. None if empty."""

    def nonappend_ops_since(self, marker: Optional[int]) -> list:
        """Row-changing operations (UPDATE/DELETE/overwrite/MERGE) committed after
        `marker`. Benign compaction (`replace`/`OPTIMIZE`) and plain appends are
        NOT included — used by the append-mode safety check."""
