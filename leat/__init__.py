"""leat — lightweight, engine-neutral incremental ETL over Iceberg & Delta.

Kafka-style consumer semantics (offsets, earliest/latest, seek, exactly-once)
over lakehouse tables. Runs in any DAG. No broker, no cluster.
"""
from .format import TableFormat
from .iceberg import IcebergFormat
from .consumer import Consumer, Batch
from .checkpoint import JsonCheckpointStore, SinkCheckpointStore
from .compute import sql
from .pipeline import connect, session, Session, Pipeline, Model, TableHandle
from .backfill import Backfill
from .elastic import run_worker, ElasticRunner
from .coordination import (
    ClaimStore, Claim, LocalClaimStore, EtcdClaimStore, open_claim_store,
)

try:  # optional until the Delta adapter lands
    from .delta import DeltaFormat
except Exception:  # pragma: no cover
    DeltaFormat = None

__all__ = [
    "TableFormat", "IcebergFormat", "DeltaFormat",
    "Consumer", "Batch", "JsonCheckpointStore", "SinkCheckpointStore", "sql",
    "connect", "session", "Session", "Pipeline", "Model", "TableHandle",
    "Backfill",
    "run_worker", "ElasticRunner",
    "ClaimStore", "Claim", "LocalClaimStore", "EtcdClaimStore", "open_claim_store",
]
__version__ = "0.0.1"
