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

    Shares the JsonCheckpointStore get/set interface, but `set` is a no-op: the
    offset is persisted inside `sink.append(offsets=...)`, so Consumer.commit()
    (which calls set) stays a safe call and the sink is the source of truth.
    """
    def __init__(self, sink):                    # a TableFormat (IcebergFormat/DeltaFormat)
        self._sink = sink

    def get(self, name: str) -> Optional[int]:
        return self._sink.read_offsets().get(name)

    def set(self, name: str, offset: Optional[int]) -> None:
        pass                                     # persisted atomically in sink.append(offsets=...)
