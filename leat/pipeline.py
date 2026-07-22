"""Ergonomic layer: connect() + declarative pipeline().run().

Turns "wire up a catalog, format, checkpoint, consumer, and a poll/commit loop"
into "declare source -> transform -> sink, then run()".
"""
from __future__ import annotations
import os
import time
from typing import Callable, Optional, Union
import pyarrow as pa

from .iceberg import IcebergFormat
from .consumer import Consumer
from .checkpoint import JsonCheckpointStore, SinkCheckpointStore

_OFF_COL = "_offset"


def _delta_path(warehouse: str, identifier: str) -> str:
    """Map a string identifier (e.g. ``"db.events"``) to a Delta table PATH under
    ``warehouse``. Convention: dots become directory separators, so ``db.events``
    -> ``<warehouse>/db/events``. Always forward-slash, no spaces (Windows-safe;
    delta-rs accepts plain absolute paths with forward slashes)."""
    rel = identifier.replace(".", "/")
    base = str(warehouse).replace("\\", "/").rstrip("/")
    return f"{base}/{rel}"


class TableHandle:
    """Polars-easy wrapper over an Iceberg table: `_offset` is leat-owned & invisible.

    `.write(df)` accepts a Polars DataFrame OR a pyarrow Table with NO `_offset`
    column — leat infers the schema, auto-creates the table (prepending an
    `_offset: int64` field), then appends (auto-minting `_offset`). `.read()`
    returns a Polars DataFrame with `_offset` stripped (business columns only).
    `.source()` / `.format` expose the underlying `IcebergFormat` (which still
    carries `_offset`) for advanced/low-level use.
    """

    def __init__(self, session: "Session", identifier: str):
        self._session = session
        self._id = identifier

    @property
    def format(self):
        return self._session.source(self._id)

    def source(self):
        return self.format

    def _exists(self) -> bool:
        return self._session._exists(self._id)

    def write(self, df) -> "TableHandle":
        # Accept Polars DataFrame or pyarrow Table.
        arrow = df.to_arrow() if hasattr(df, "to_arrow") else df
        if not self._exists():
            schema = arrow.schema
            if schema.get_field_index(_OFF_COL) == -1:
                schema = schema.insert(0, pa.field(_OFF_COL, pa.int64()))
            self._session.create(self._id, schema)
        # append() auto-mints `_offset` (the frame carries only business columns).
        self.format.append(arrow)
        return self

    def read(self):
        import polars as pl
        arrow = self.format.read_all()
        df = pl.from_arrow(arrow)
        if _OFF_COL in df.columns:
            df = df.drop(_OFF_COL)
        return df


class Pipeline:
    def __init__(self, session: "Session", name: str, source, sink, transform, start="latest",
                 mode: str = "append", key=None, on_change: str = "warn"):
        self.name = name
        self.session = session
        self.sink_id = sink if isinstance(sink, str) else None
        self.src = session.source(source) if isinstance(source, str) else source
        self.snk = session.source(sink) if isinstance(sink, str) else sink
        self.transform = transform
        # --- mutation-aware modes -------------------------------------------
        self.mode = (mode or "append").lower()
        if self.mode not in ("append", "upsert"):
            raise ValueError(f"invalid mode: {mode!r} (expected 'append' or 'upsert')")
        if self.mode == "upsert":
            if not key:
                raise ValueError(
                    "mode='upsert' requires key=[...] (the business key to merge by)")
            self.keys = [key] if isinstance(key, str) else list(key)
        else:
            self.keys = None
        # The sink is per-pipeline, so the sink-checkpoint must be built here, per
        # sink — not once globally. In "sink" mode the consumer resolves its start
        # offset from THIS sink's own commit metadata (the atomic source of truth).
        self._sink_ready = False
        self._sink_mode = session.checkpoint_mode == "sink"
        checkpoint = SinkCheckpointStore(self.snk) if self._sink_mode else session.checkpoint
        self.consumer = Consumer(self.src, name=name, checkpoint=checkpoint, start=start,
                                 on_change=on_change)

    def _ensure_sink(self, arrow) -> None:
        """Easy-path sink auto-create: if the sink was given as an identifier and its
        table doesn't exist yet, infer schema from the first transform output (adding
        an `_offset: int64` field) and create it. No-op when the sink already exists
        (pre-created sinks, e.g. the benchmark, are untouched) or when a format object
        was passed directly."""
        if self.sink_id is None or self._sink_ready:
            return
        if self.session._exists(self.sink_id):
            self._sink_ready = True
            return
        schema = arrow.schema
        if schema.get_field_index(_OFF_COL) == -1:
            schema = schema.insert(0, pa.field(_OFF_COL, pa.int64()))
        self.session.create(self.sink_id, schema)
        self._sink_ready = True

    def _dedup_latest(self, arrow):
        """Keep the LAST row per business key (stable) — the batch is already in
        ascending source-offset order, so 'last' = 'latest version in this batch'.
        No-op when every key is unique. Returns an Arrow table."""
        import polars as pl
        df = arrow if hasattr(arrow, "unique") else pl.from_arrow(arrow)
        n = df.height
        df = df.unique(subset=self.keys, keep="last", maintain_order=True)
        if df.height == n:                                       # no dupes -> unchanged
            return arrow
        return df.to_arrow()

    def step(self) -> int:
        """Process one batch: read delta -> transform -> append -> commit. Exactly-once."""
        batch = self.consumer.poll()
        if batch is None:
            return 0
        # Invisible-offset boundary: the user's transform sees BUSINESS columns only.
        # We strip the source `_offset` before the transform and append the result
        # WITHOUT it, so the sink auto-mints its own fresh offset (see append()).
        # The source-consumer commit still uses batch.offset (max source `_offset`,
        # captured internally) — UNCHANGED.
        df = batch.pl()
        if _OFF_COL in df.columns:
            # Sort by source offset so batch order is deterministic ascending —
            # this is what makes upsert's dedup-latest-within-batch (keep="last")
            # correctly pick the highest-offset version of a repeated key.
            if self.mode == "upsert":
                df = df.sort(_OFF_COL)
            df = df.drop(_OFF_COL)
        result = self.transform(df)                              # user's transform (Polars in)
        arrow = result.to_arrow() if hasattr(result, "to_arrow") else result  # Polars or Arrow out
        # A low-level caller may still emit `_offset`; append() honors it if present,
        # otherwise it mints a fresh sink offset.
        self._ensure_sink(arrow)                                 # easy-path: auto-create if missing
        if self.mode == "upsert":
            # Dedup-latest WITHIN the batch by key: if the same key appears twice
            # (v1 then v2 as two appends in one poll), keep the LAST row (highest
            # source offset, since read_since returns in offset order). Required —
            # Iceberg upsert REJECTS duplicate source keys and Delta would keep
            # both. Then MERGE-by-key so the sink holds CURRENT STATE and
            # reprocessing the same batch is idempotent (no dupes). The offset
            # rides the merge commit in sink mode (atomic → exactly-once); in json
            # mode the merge's own idempotency backstops the separate offset write.
            arrow = self._dedup_latest(arrow)
            if self._sink_mode:
                self.snk.upsert(arrow, self.keys, offsets={self.name: batch.offset})
                self.consumer.seek(batch.offset)
            else:
                self.snk.upsert(arrow, self.keys)
                self.consumer.commit()
        elif self._sink_mode:
            # ONE atomic commit: data + offset ride the same sink snapshot, so the
            # offset advances iff the append commits (no crash window → exactly-once).
            self.snk.append(arrow, offsets={self.name: batch.offset})
            self.consumer.seek(batch.offset)                     # advance in-memory; sink is truth
        else:
            self.snk.append(arrow)
            self.consumer.commit()                               # separate offset write (JSON path)
        return batch.num_rows

    def run(self, once: bool = False, idle_sleep: float = 1.0, max_batches: Optional[int] = None) -> None:
        """Run the incremental loop. once=True -> single batch (perfect for a DAG task)."""
        done = 0
        while True:
            n = self.step()
            done += 1 if n else 0
            if once or (max_batches and done >= max_batches):
                return
            if n == 0:
                time.sleep(idle_sleep)

    def lag(self) -> int:
        return self.consumer.lag()

    def position(self):
        return self.consumer.position()


class Model:
    """A named pipeline defined by a decorated transform function (dbt-model style)."""
    def __init__(self, pipeline: "Pipeline"):
        self._p = pipeline
        self.name = pipeline.name

    def run(self, once: bool = False, **kw):
        return self._p.run(once=once, **kw)

    def step(self) -> int:
        return self._p.step()

    def lag(self) -> int:
        return self._p.lag()


class Session:
    """Format-aware session. ``format="iceberg"`` (default) resolves a string
    identifier to an ``IcebergFormat`` on ``self.catalog``; ``format="delta"``
    resolves it to a ``DeltaFormat`` at a PATH under ``self.warehouse`` (dots ->
    directory separators, e.g. ``"db.events"`` -> ``<warehouse>/db/events``).

    All identifier-taking entry points (`source`/`create`/`table`/`pipeline`/
    `model`) go through `_resolve`, so the same ergonomic surface works on either
    format. Passing a format OBJECT (not a string) anywhere is honored as-is.
    """

    def __init__(self, catalog, checkpoint: JsonCheckpointStore, checkpoint_mode: str = "json",
                 format: str = "iceberg", warehouse: Optional[str] = None,
                 storage_options: Optional[dict] = None):
        self.catalog = catalog
        self.checkpoint = checkpoint          # used by the "json" path (shared file)
        self.checkpoint_mode = checkpoint_mode  # "json" (default) or "sink" (atomic, per-pipeline)
        self.format = (format or "iceberg").lower()
        self.warehouse = warehouse            # base path for the Delta identifier->path map
        self._storage_options = storage_options
        self._models: dict = {}

    def _resolve(self, identifier):
        """Resolve a string identifier to a TableFormat (or pass a format object
        through unchanged)."""
        if not isinstance(identifier, str):
            return identifier                 # already a TableFormat object
        if self.format == "delta":
            from .delta import DeltaFormat
            return DeltaFormat(_delta_path(self.warehouse, identifier),
                               storage_options=self._storage_options)
        return IcebergFormat(self.catalog, identifier)

    def _exists(self, identifier) -> bool:
        """Does the underlying table exist yet? Format-aware (catalog for Iceberg,
        ``_delta_log`` presence for Delta)."""
        if not isinstance(identifier, str):
            identifier = getattr(identifier, "_uri", None) or getattr(identifier, "_id", None)
        if self.format == "delta":
            from deltalake import DeltaTable
            return DeltaTable.is_deltatable(_delta_path(self.warehouse, identifier),
                                            storage_options=self._storage_options)
        try:
            return self.catalog.table_exists(identifier)
        except Exception:
            try:
                self.catalog.load_table(identifier)
                return True
            except Exception:
                return False

    def source(self, identifier):
        return self._resolve(identifier)

    def table(self, identifier: str) -> TableHandle:
        """Polars-easy handle: `.write(df)` (infer+auto-create+auto-mint `_offset`),
        `.read()` (Polars, no `_offset`), `.source()`/`.format` (low-level)."""
        return TableHandle(self, identifier)

    def write(self, identifier: str, df) -> TableHandle:
        """Convenience: `self.table(identifier).write(df)`."""
        return self.table(identifier).write(df)

    def create(self, identifier: str, schema: pa.Schema):
        """Create a table (and its namespace) if needed — convenience for quickstarts.

        For Delta, delta-rs creates the table on first write, so `create` is a
        dir-prep no-op that returns a usable `DeltaFormat` handle (the `_offset`
        field, if present in `schema`, is honored at first append time)."""
        if self.format == "delta":
            path = _delta_path(self.warehouse, identifier)
            os.makedirs(path, exist_ok=True)  # parent dir prep; delta-rs writes _delta_log on append
            return self._resolve(identifier)
        ns = identifier.rsplit(".", 1)[0] if "." in identifier else "default"
        try:
            self.catalog.create_namespace(ns)
        except Exception:
            pass
        self.catalog.create_table(identifier, schema=schema)
        return self.source(identifier)

    def pipeline(self, name: str, source, sink,
                 transform: Callable, start: Union[str, int] = "latest",
                 mode: str = "append", key=None, on_change: str = "warn") -> Pipeline:
        return Pipeline(self, name, source, sink, transform, start,
                        mode=mode, key=key, on_change=on_change)

    def model(self, source: str, sink: str, start: Union[str, int] = "latest",
              mode: str = "append", key=None, on_change: str = "warn"):
        """Decorator: turn a Polars transform function into a named pipeline (dbt-model style).

            @lt.model(source="db.events", sink="db.silver")
            def silver_clean(df):
                return df.filter(pl.col("value") > 100)

        `mode="upsert", key=["id"]` MERGES the transform output into the sink by
        business key (update matching rows, insert new) instead of appending — so
        the sink holds current state and reprocessing is idempotent. `on_change`
        ("warn"/"error"/"ignore") controls how append mode surfaces source
        UPDATE/DELETE commits its offset cursor can't see.
        """
        def deco(func: Callable) -> Model:
            m = Model(self.pipeline(func.__name__, source, sink, func, start,
                                    mode=mode, key=key, on_change=on_change))
            self._models[m.name] = m
            return m
        return deco

    def run_all(self, once: bool = False) -> None:
        """Run every registered model once (or loop)."""
        for m in self._models.values():
            m.run(once=once)


def connect(warehouse: str, uri: Optional[str] = None,
            checkpoint: Optional[str] = None, name: str = "leat",
            catalog: Optional[str] = None, format: str = "iceberg", **opts) -> Session:
    """Zero-config session — builds a catalog with Windows-safe FileIO.

        lt = leat.connect("/data/leat")                     # SQLite, exactly-once sink offsets (default)
        lt = leat.connect("/data/leat", checkpoint="json")  # opt into a side JSON offset file
        lt = leat.connect("/data/leat", format="delta")     # Delta Lake, no catalog (path-based)

    `format`: "iceberg" (default) → builds a catalog (see below). "delta" → NO
    catalog is built; `warehouse` becomes the base directory and a string
    identifier like ``"db.events"`` resolves to a Delta table PATH
    ``<warehouse>/db/events`` (dots → directory separators; forward-slash,
    Windows-safe). `table()/create()/model()/pipeline()` all work exactly as the
    Iceberg easy path, just backed by `DeltaFormat`. `catalog`/`uri` are ignored
    for Delta. Extra `**opts` are passed to `DeltaFormat` as `storage_options`
    (e.g. S3/Azure creds).

        # A live REST catalog (Snowflake-Polaris / Unity / Tabular / Nessie / iceberg-rest)
        # backed by S3-compatible storage is a one-liner — pass catalog="rest":
        lt = leat.connect(
            "s3://warehouse/", uri="http://localhost:8181", catalog="rest",
            **{"s3.endpoint": "http://localhost:9002",
               "s3.access-key-id": "admin", "s3.secret-access-key": "password",
               "s3.path-style-access": "true"})

    `catalog`: None/"sql"/"sqlite" (default) → a local `SqlCatalog` (SQLite),
    warehouse is a local dir. "rest" → a `pyiceberg.catalog.rest.RestCatalog`;
    `warehouse` is the object-store URI (e.g. ``s3://warehouse/``) and `uri`
    is the REST endpoint. Any extra `**opts` (e.g. ``s3.endpoint``,
    ``s3.access-key-id``, ``s3.secret-access-key``, ``s3.path-style-access``,
    ``s3.region``, or a custom ``py-io-impl``) pass straight through to the
    catalog — so the SAME leat pipeline code runs against a real cloud-shaped
    catalog with only this call changed.

    `checkpoint`: "sink" (default) → each pipeline's offset lives in ITS sink
    table's commit metadata (atomic with the append → true exactly-once, even
    across a crash). "json" → offsets in a side JSON file (at-least-once under a
    crash between the append and the offset write). Any other value is a JSON path.
    """
    kind = (catalog or "").lower()
    mode = "json" if (checkpoint is not None and checkpoint != "sink") else "sink"

    if (format or "iceberg").lower() == "delta":
        # Path-based, no catalog. `warehouse` is the base dir; identifiers map to
        # <warehouse>/<id-with-dots-as-slashes>. `catalog`/`uri` are irrelevant.
        base = warehouse.replace("file:///", "").replace("\\", "/").rstrip("/")
        os.makedirs(base, exist_ok=True)
        storage_options = opts or None
        path = None if checkpoint in ("sink", "json", None) else checkpoint
        ck = JsonCheckpointStore(path or os.path.join(base, "offsets.json"))
        return Session(None, ck, checkpoint_mode=mode,
                       format="delta", warehouse=base, storage_options=storage_options)

    if kind in ("rest",):
        from pyiceberg.catalog.rest import RestCatalog
        props = {
            "uri": uri,                              # REST endpoint (required)
            "warehouse": warehouse,                  # object-store URI, e.g. s3://warehouse/
            # PyArrowFileIO talks to MinIO/S3 with an endpoint override + path-style
            # access; callers can override any of these via **opts.
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        }
        props.update(opts)
        cat = RestCatalog(name, **props)
        # Offsets ride sink commits ("sink") or a side JSON file. For a remote
        # warehouse the default JSON path is placed in the CWD (local scratch).
        path = None if checkpoint in ("sink", "json", None) else checkpoint
        ck = JsonCheckpointStore(path or os.path.join(os.getcwd(), f"{name}.offsets.json"))
        return Session(cat, ck, checkpoint_mode=mode)

    # Default: local SQLite catalog (unchanged behavior).
    from pyiceberg.catalog.sql import SqlCatalog
    local = warehouse.replace("file:///", "").rstrip("/")
    os.makedirs(local, exist_ok=True)
    props = {
        "uri": uri or f"sqlite:///{local}/catalog.db",
        "warehouse": f"file:///{local}",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }
    props.update(opts)
    cat = SqlCatalog(name, **props)
    path = None if checkpoint in ("sink", "json", None) else checkpoint
    ck = JsonCheckpointStore(path or os.path.join(local, "offsets.json"))
    return Session(cat, ck, checkpoint_mode=mode)


def session(catalog, checkpoint: JsonCheckpointStore, checkpoint_mode: str = "json") -> Session:
    """Bring your own catalog (REST / Glue / Snowflake-Polaris) + checkpoint store.

    `checkpoint_mode="sink"` makes each pipeline persist its offset atomically in
    its own sink's commit metadata (the passed `checkpoint` becomes unused)."""
    return Session(catalog, checkpoint, checkpoint_mode=checkpoint_mode)
