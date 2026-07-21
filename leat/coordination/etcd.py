"""EtcdClaimStore — distributed ClaimStore where the etcd lease IS failover.

Key idea: attach every claim to an etcd lease with the same TTL. While the
holder is alive it keepalives (refreshes) the lease. If the holder dies, the
lease expires, etcd auto-deletes the key, and the shard frees itself — no
reaper, no liveness table, no split-brain. That's the whole failover story.

Values are stored as protobuf `leat.Claim` bytes (compact + language-neutral),
so a worker in any language can read/write the same coordination state.

Key layout: /leat/claims/<shard> -> serialized Claim.

etcd3 is an optional dependency and is imported lazily in __init__ so that
`import leat.coordination` works without it installed.
"""
from __future__ import annotations

from typing import Optional

from .base import NO_BOOKMARK, Claim, ClaimStore, now_ms
from .proto import claim_pb2

_PREFIX = "/leat/claims/"


def _key(shard: str) -> str:
    return _PREFIX + shard


def _serialize(c: Claim) -> bytes:
    msg = claim_pb2.Claim(
        shard=c.shard,
        worker=c.worker,
        lease_expiry_ms=c.lease_expiry_ms,
        bookmark_offset=c.bookmark_offset,
        status=c.status,
    )
    return msg.SerializeToString()


def _parse(raw: bytes) -> Claim:
    msg = claim_pb2.Claim.FromString(raw)
    return Claim(
        shard=msg.shard,
        worker=msg.worker,
        lease_expiry_ms=msg.lease_expiry_ms,
        bookmark_offset=msg.bookmark_offset,
        status=msg.status,
    )


class EtcdClaimStore(ClaimStore):
    def __init__(self, host: str = "localhost", port: int = 2379, **kwargs):
        try:
            import etcd3  # lazy: keeps the package importable without etcd3
        except ImportError as e:  # pragma: no cover - depends on optional dep
            raise ImportError(
                "EtcdClaimStore requires the 'etcd3' package. "
                "Install it with:  pip install etcd3"
            ) from e
        self._client = etcd3.client(host=host, port=port, **kwargs)
        # Leases we currently hold, keyed by shard, so renew()/release() can act on them.
        self._leases: dict[str, object] = {}

    # --- ClaimStore --------------------------------------------------------
    def claim(self, shard: str, worker: str, ttl: float) -> bool:
        key = _key(shard)
        expiry = now_ms() + int(ttl * 1000)
        lease = self._client.lease(int(ttl) or 1)
        value = _serialize(
            Claim(shard=shard, worker=worker, lease_expiry_ms=expiry,
                  bookmark_offset=NO_BOOKMARK, status="in_progress")
        )
        txn = self._client.transactions
        # Acquire iff the key does not exist (create_revision == 0). A live lease
        # keeps the key present, so this fails while another worker holds it; a
        # dead worker's key is already gone (lease expired -> etcd deleted it).
        put_ok, _ = self._client.transaction(
            compare=[txn.create(key) == 0],
            success=[txn.put(key, value, lease=lease)],
            failure=[],
        )
        if put_ok:
            self._leases[shard] = lease
            return True

        # Key exists. If it's ours (re-claim) or its wall-clock lease has lapsed,
        # overwrite it. This is best-effort — the etcd lease is the real guard.
        current = self.get(shard)
        if current is not None and (current.worker == worker or current.expired()):
            replace_ok, _ = self._client.transaction(
                compare=[txn.value(key) == self._client.get(key)[0]],
                success=[txn.put(key, value, lease=lease)],
                failure=[],
            )
            if replace_ok:
                old = self._leases.pop(shard, None)
                if old is not None:
                    self._revoke(old)
                self._leases[shard] = lease
                return True

        # Lost the race — drop the lease we speculatively created.
        self._revoke(lease)
        return False

    def renew(self, shard: str, worker: str, ttl: float) -> bool:
        lease = self._leases.get(shard)
        if lease is None:
            return False
        current = self.get(shard)
        if current is None or current.worker != worker:
            return False
        # Refresh keepalives the lease; also bump the stored wall-clock expiry.
        lease.refresh()
        updated = Claim(shard=current.shard, worker=current.worker,
                        lease_expiry_ms=now_ms() + int(ttl * 1000),
                        bookmark_offset=current.bookmark_offset,
                        status=current.status)
        self._rmw_put(shard, worker, updated, lease)
        return True

    def bookmark(self, shard: str, worker: str, offset: int) -> None:
        current = self.get(shard)
        if current is None or current.worker != worker:
            return
        current.bookmark_offset = offset
        self._rmw_put(shard, worker, current, self._leases.get(shard))

    def get(self, shard: str) -> Optional[Claim]:
        raw, _meta = self._client.get(_key(shard))
        return _parse(raw) if raw is not None else None

    def complete(self, shard: str, worker: str) -> None:
        current = self.get(shard)
        if current is None or current.worker != worker:
            return
        current.status = "done"
        self._rmw_put(shard, worker, current, self._leases.get(shard))

    def release(self, shard: str, worker: str) -> None:
        current = self.get(shard)
        if current is not None and current.worker != worker:
            return
        self._client.delete(_key(shard))
        lease = self._leases.pop(shard, None)
        if lease is not None:
            self._revoke(lease)

    def list_claims(self) -> dict[str, Claim]:
        out: dict[str, Claim] = {}
        for raw, _meta in self._client.get_prefix(_PREFIX):
            c = _parse(raw)
            out[c.shard] = c
        return out

    def close(self) -> None:
        for lease in list(self._leases.values()):
            self._revoke(lease)
        self._leases.clear()
        self._client.close()

    # --- internals ---------------------------------------------------------
    def _rmw_put(self, shard: str, worker: str, claim: Claim, lease) -> None:
        """Best-effort compare-and-set of the value under an existing key.

        Guards on the current bytes so we don't clobber a concurrent writer;
        preserves the lease so failover semantics survive the update.
        """
        key = _key(shard)
        txn = self._client.transactions
        existing, _ = self._client.get(key)
        if existing is None:
            return
        kwargs = {"lease": lease} if lease is not None else {}
        self._client.transaction(
            compare=[txn.value(key) == existing],
            success=[txn.put(key, _serialize(claim), **kwargs)],
            failure=[],
        )

    @staticmethod
    def _revoke(lease) -> None:
        try:
            lease.revoke()
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
