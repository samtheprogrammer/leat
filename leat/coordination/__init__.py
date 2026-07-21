"""Coordination layer — shared, atomic shard-ownership state for backfill/failover.

A `ClaimStore` is the "who is working on which shard" table that lets N workers
agree on ownership via an atomic compare-and-swap guarded by a TTL/lease. Pick a
backend with `open_claim_store(uri)`:

    open_claim_store("local")              # SQLite in a warehouse-relative file
    open_claim_store("sqlite:///path.db")  # SQLite at an explicit path
    open_claim_store("etcd://host:2379")   # distributed; lease == failover

The default is SQLite (single machine, cross-process). etcd is for real
distributed coordination where a lease expiry auto-frees a dead worker's shards.
"""
from __future__ import annotations

import os
import tempfile
from urllib.parse import urlparse

from .base import Claim, ClaimStore
from .local import LocalClaimStore
from .etcd import EtcdClaimStore

__all__ = [
    "ClaimStore",
    "Claim",
    "LocalClaimStore",
    "EtcdClaimStore",
    "open_claim_store",
]

# Default sqlite file lives under the OS temp dir (cross-platform, no-space).
# Override with LEAT_CLAIMS_DB.
_DEFAULT_LOCAL_DB = os.environ.get(
    "LEAT_CLAIMS_DB",
    os.path.join(tempfile.gettempdir(), "leat_coord", "claims.db"),
)


def open_claim_store(uri: str = "local") -> ClaimStore:
    """Build a ClaimStore from a URI.

    - ``"local"``                -> LocalClaimStore at the default warehouse-relative file
    - ``"sqlite:///abs/path.db"``-> LocalClaimStore at that file
    - ``"etcd://host:port"``     -> EtcdClaimStore
    """
    if uri == "local":
        return LocalClaimStore(_DEFAULT_LOCAL_DB)

    parsed = urlparse(uri)
    scheme = parsed.scheme

    if scheme == "sqlite":
        # sqlite:///F:/x/claims.db  -> path is "/F:/x/claims.db"; strip a Windows
        # leading slash (/F:/... -> F:/...) so os/sqlite are happy.
        path = parsed.path
        if len(path) >= 3 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        if not path:
            raise ValueError(f"sqlite claim-store URI needs a path: {uri!r}")
        return LocalClaimStore(path)

    if scheme == "etcd":
        host = parsed.hostname or "localhost"
        port = parsed.port or 2379
        return EtcdClaimStore(host=host, port=port)

    raise ValueError(
        f"Unrecognized claim-store URI {uri!r}. "
        "Use 'local', 'sqlite:///<path>', or 'etcd://<host>:<port>'."
    )
