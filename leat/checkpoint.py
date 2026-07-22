"""Offset store — where a consumer's committed offset lives (its 'consumer group').

JSON for v0. In production this becomes an Iceberg/Delta table so the commit is
atomic with the sink write (true exactly-once).
"""
from __future__ import annotations
import json
import os
from typing import Optional


class JsonCheckpointStore:
    def __init__(self, path: str):
        self._path = path

    def _load(self) -> dict:
        if os.path.exists(self._path):
            with open(self._path) as f:
                return json.load(f)
        return {}

    def get(self, name: str) -> Optional[int]:
        return self._load().get(name)

    def set(self, name: str, offset: Optional[int]) -> None:
        data = self._load()
        data[name] = offset
        with open(self._path, "w") as f:
            json.dump(data, f)


class SinkCheckpointStore:
    """Offsets live in the sink table's own commit metadata → atomic with the
    append (true exactly-once). No separate file, so no crash window between the
    data write and the offset write.

    The default `@lt.model` loop persists the offset *inside* the data append
    (`sink.append(offsets=...)`) and advances the consumer in memory — it never
    calls `set()`. `set()` exists for the control paths — `reset`/`seek` and a
    low-level `Consumer.commit()` — which need to move a sink-stored offset without
    a data write. It does that with an offset-only commit (empty data + embedded
    offset), so the sink stays the single source of truth.
    """
    def __init__(self, sink):                    # a TableFormat (IcebergFormat/DeltaFormat)
        self._sink = sink

    def get(self, name: str) -> Optional[int]:
        return self._sink.read_offsets().get(name)

    def set(self, name: str, offset: Optional[int]) -> None:
        # Offset-only control commit: empty data matching the sink schema, with the
        # offset in the snapshot metadata. None -> -1 ("before all" = earliest).
        empty = self._sink.read_all().schema.empty_table()
        self._sink.append(empty, offsets={name: -1 if offset is None else int(offset)})
