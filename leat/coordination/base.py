"""ClaimStore — shared, atomic "who owns which shard" state for distributed work.

Backfill and failover need one thing: a place where N workers can atomically
agree on shard ownership. The contract is a compare-and-swap claim guarded by a
TTL/lease, so a dead worker's shards free themselves. This is coordination, not
throughput — claims are seconds/minutes apart, so correctness > speed.

Backends: `LocalClaimStore` (SQLite, cross-process on one box, the default) and
`EtcdClaimStore` (distributed; the lease IS the failover primitive).
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

# Sentinel for "no bookmark yet" — a shard that has committed nothing.
NO_BOOKMARK = -1


def now_ms() -> int:
    """Wall-clock milliseconds. Lease expiry is wall-clock so it survives restarts."""
    return int(time.time() * 1000)


@dataclass
class Claim:
    """One worker's ownership of one shard.

    lease_expiry_ms is wall-clock ms: the claim is live iff now_ms() < it. A
    holder renews before expiry; if it dies, the lease lapses and the shard is
    claimable by anyone. bookmark_offset is the last committed offset for the
    shard (the per-shard resume point).
    """
    shard: str
    worker: str
    lease_expiry_ms: int
    bookmark_offset: int = NO_BOOKMARK
    status: str = "in_progress"  # "in_progress" | "done"

    def expired(self, at_ms: Optional[int] = None) -> bool:
        return (at_ms if at_ms is not None else now_ms()) >= self.lease_expiry_ms


class ClaimStore(ABC):
    """Pluggable shared state for shard coordination.

    All mutating ops are guarded by (shard, worker): a worker can only affect a
    shard it currently holds. `claim` is the sole exception — it acquires an
    unheld or expired shard via an atomic compare-and-swap.
    """

    @abstractmethod
    def claim(self, shard: str, worker: str, ttl: float) -> bool:
        """Atomically acquire `shard` for `worker` for `ttl` seconds.

        Returns True iff acquired — i.e. the shard was unheld or its lease had
        expired. A worker re-claiming a shard it already holds also returns True
        (idempotent re-acquire that extends the lease).
        """

    @abstractmethod
    def renew(self, shard: str, worker: str, ttl: float) -> bool:
        """Heartbeat: extend our lease by `ttl` seconds. True iff still ours."""

    @abstractmethod
    def bookmark(self, shard: str, worker: str, offset: int) -> None:
        """Persist per-shard progress (last committed offset). No-op if not ours."""

    @abstractmethod
    def get(self, shard: str) -> Optional[Claim]:
        """Current claim for `shard`, or None if unclaimed/expired-and-cleared."""

    @abstractmethod
    def complete(self, shard: str, worker: str) -> None:
        """Mark the shard done (status='done'). No-op if not ours."""

    @abstractmethod
    def release(self, shard: str, worker: str) -> None:
        """Give up the claim so another worker can take the shard. No-op if not ours."""

    @abstractmethod
    def list_claims(self) -> dict[str, Claim]:
        """Every currently stored claim, keyed by shard."""

    @abstractmethod
    def close(self) -> None:
        """Release backend resources (connections, leases)."""

    # Context-manager sugar so callers can `with open_claim_store(...) as cs:`.
    def __enter__(self) -> "ClaimStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
