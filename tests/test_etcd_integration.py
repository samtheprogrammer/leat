"""LIVE etcd integration tests for EtcdClaimStore.

Unlike ``tests/test_coordination.py`` (which can only round-trip the protobuf
wire format because it has no server), this module runs the FULL ClaimStore
contract against a real etcd, plus the two things that ONLY a live etcd can
prove:

  * lease-expiry failover — a worker "dies" (stops renewing), etcd auto-deletes
    the key when the lease lapses, and another worker reclaims the shard. The
    etcd lease TTL *is* leat's failover primitive; this test measures the real
    expiry -> reclaim latency.
  * true cross-connection distributed coordination — two independent
    ``EtcdClaimStore`` instances (stand-ins for two machines) see each other's
    claims, which SQLite cannot do across hosts.

Bring etcd up first (see ``infra/etcd/docker-compose.yml``):

    cd infra/etcd && docker compose up -d

Then:  pytest tests/test_etcd_integration.py

If etcd is unreachable (or the etcd3 client can't be imported), every test
SKIPS with a clear message so the suite stays green without a server.

etcd3 client compat note
------------------------
etcd3 0.12.0 ships stale generated protobuf stubs. Under modern protobuf
(>= 4, C/upb backend) importing etcd3 raises "Descriptors cannot be created
directly". The fix is to select the pure-Python protobuf backend BEFORE
protobuf is first imported, via the env var
``PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`` — done in the repo-root
``conftest.py``. If protobuf was already loaded with the C backend, these tests
skip with guidance rather than crashing the run.
"""
from __future__ import annotations

# Belt-and-suspenders: also set the backend here in case this module is ever
# collected/imported without the root conftest (e.g. copied elsewhere). This is
# a no-op if protobuf is already loaded — the reachability probe below then
# reports the real state and we skip cleanly.
import os
os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")

import threading
import time

import pytest

ETCD_HOST = os.environ.get("LEAT_ETCD_HOST", "localhost")
ETCD_PORT = int(os.environ.get("LEAT_ETCD_PORT", "2379"))
ETCD_URI = f"etcd://{ETCD_HOST}:{ETCD_PORT}"


# --- reachability probe: skip the whole module if there's no live etcd --------
def _probe_reason() -> str | None:
    """Return None if a live etcd is usable, else a human skip reason."""
    try:
        import etcd3  # noqa: F401
    except Exception as e:  # ImportError, or the protobuf descriptor TypeError
        return (
            f"etcd3 client not importable ({type(e).__name__}: {e}). "
            "Install with `pip install etcd3` and ensure "
            "PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python is set before "
            "protobuf loads (see repo-root conftest.py)."
        )
    try:
        c = etcd3.client(host=ETCD_HOST, port=ETCD_PORT, timeout=2)
        c.status()  # round-trips to the server
        c.close()
    except Exception as e:
        return (
            f"no live etcd at {ETCD_HOST}:{ETCD_PORT} ({type(e).__name__}: {e}). "
            "Start one with: cd infra/etcd && docker compose up -d"
        )
    return None


_SKIP_REASON = _probe_reason()
pytestmark = pytest.mark.skipif(_SKIP_REASON is not None, reason=_SKIP_REASON or "")


# --- fixtures -----------------------------------------------------------------
def _new_store():
    from leat.coordination import open_claim_store
    return open_claim_store(ETCD_URI)


@pytest.fixture
def shard(request):
    """A unique shard key per test, cleaned up before and after.

    Uniqueness (test name + pid + monotonic ns) keeps tests independent even if
    a previous run left keys behind or tests run in parallel.
    """
    key = f"itest-{request.node.name}-{os.getpid()}-{time.monotonic_ns()}"
    cleanup = _new_store()
    # `release(worker=<current>)` only deletes if unheld-or-ours; force a raw
    # delete to guarantee a clean slate regardless of prior owner.
    cleanup._client.delete(f"/leat/claims/{key}")
    try:
        yield key
    finally:
        cleanup._client.delete(f"/leat/claims/{key}")
        cleanup.close()


@pytest.fixture
def store():
    cs = _new_store()
    try:
        yield cs
    finally:
        cs.close()


# ============================================================================
# 1. Basic claim contract (mirrors test_coordination.py, but against LIVE etcd)
# ============================================================================
def test_claim_is_exclusive(store, shard):
    assert store.claim(shard, "A", ttl=60) is True
    assert store.claim(shard, "B", ttl=60) is False  # already held by A
    c = store.get(shard)
    assert c is not None and c.worker == "A" and c.status == "in_progress"


def test_reclaim_by_owner_is_idempotent(store, shard):
    assert store.claim(shard, "A", ttl=60) is True
    assert store.claim(shard, "A", ttl=60) is True  # re-acquire, still True


def test_get_and_list_reflect_holder(store, shard):
    store.claim(shard, "A", ttl=60)
    got = store.get(shard)
    assert got.worker == "A"
    claims = store.list_claims()
    assert shard in claims and claims[shard].worker == "A"


def test_renew_only_for_owner(store, shard):
    store.claim(shard, "A", ttl=60)
    assert store.renew(shard, "A", ttl=60) is True
    assert store.renew(shard, "B", ttl=60) is False  # not B's claim


def test_bookmark_persists_owner_only(store, shard):
    store.claim(shard, "A", ttl=60)
    assert store.get(shard).bookmark_offset == -1
    store.bookmark(shard, "A", 1234)
    assert store.get(shard).bookmark_offset == 1234
    store.bookmark(shard, "B", 9999)  # non-owner cannot move it
    assert store.get(shard).bookmark_offset == 1234


def test_complete_sets_status(store, shard):
    store.claim(shard, "A", ttl=60)
    store.complete(shard, "A")
    assert store.get(shard).status == "done"


def test_release_frees_shard(store, shard):
    store.claim(shard, "A", ttl=60)
    store.release(shard, "A")
    assert store.get(shard) is None
    assert store.claim(shard, "B", ttl=60) is True  # freed -> B can take it


def test_protobuf_value_round_trips_through_real_etcd(shard):
    """Write via one client, read via a SECOND client -> identical Claim.

    Proves the protobuf wire format survives a real etcd write/read across
    independent connections (not just an in-process serialize/parse).
    """
    writer = _new_store()
    reader = _new_store()
    try:
        assert writer.claim(shard, "worker-A", ttl=60) is True
        writer.bookmark(shard, "worker-A", 42)
        writer.complete(shard, "worker-A")

        from_writer = writer.get(shard)
        from_reader = reader.get(shard)  # separate connection
        assert from_reader is not None
        assert from_reader == from_writer  # dataclass equality: all fields
        assert from_reader.shard == shard
        assert from_reader.worker == "worker-A"
        assert from_reader.bookmark_offset == 42
        assert from_reader.status == "done"
    finally:
        writer.close()
        reader.close()


# ============================================================================
# 2. ⭐ Lease-expiry failover — THE key etcd feature.
#    A dies (stops renewing) -> lease lapses -> etcd auto-deletes the key ->
#    B can claim. Measures the real expiry -> reclaim latency.
# ============================================================================
def test_lease_expiry_frees_shard_for_failover(shard, capsys):
    a = _new_store()
    b = _new_store()
    try:
        ttl = 2.0  # short lease; etcd TTL granularity is ~1s
        assert a.claim(shard, "A", ttl=ttl) is True
        # While A's lease is live, B cannot claim.
        assert b.claim(shard, "B", ttl=60) is False
        assert b.get(shard).worker == "A"

        # A "dies": stop touching it entirely (no renew, no keepalive). Simulate
        # by dropping the reference to A's client without revoking the lease.
        a._leases.clear()  # forget the lease so nothing keepalives it
        # NOTE: we deliberately do NOT call a.close() yet — close() would revoke
        # the lease and delete the key immediately, which would NOT prove that
        # *etcd's own TTL expiry* frees the shard. We want the lease to lapse on
        # its own. Stop the client's background keepalive threads by closing the
        # transport only after we've cleared our lease bookkeeping is not needed:
        # python-etcd3 does not keepalive automatically, so simply not calling
        # refresh() is a faithful "dead worker".

        # Poll until etcd auto-deletes the key (lease TTL elapsed).
        deadline = time.monotonic() + ttl + 8.0
        t_death = time.monotonic()
        freed_at = None
        while time.monotonic() < deadline:
            if b.get(shard) is None:
                freed_at = time.monotonic()
                break
            time.sleep(0.1)

        assert freed_at is not None, (
            "etcd never auto-deleted the key after the lease TTL — "
            "failover primitive did NOT fire"
        )
        expiry_latency = freed_at - t_death

        # Now the shard is claimable by the replacement worker.
        t0 = time.monotonic()
        assert b.claim(shard, "B", ttl=60) is True
        reclaim_latency = time.monotonic() - t0
        assert b.get(shard).worker == "B"

        with capsys.disabled():
            print(
                f"\n[lease-expiry failover] ttl={ttl:.1f}s  "
                f"key auto-deleted {expiry_latency:.2f}s after death  "
                f"(TTL + {expiry_latency - ttl:+.2f}s slack); "
                f"B reclaimed in {reclaim_latency*1000:.1f}ms  "
                f"=> total death->reclaimed {expiry_latency + reclaim_latency:.2f}s"
            )
    finally:
        # close A last; its lease is already lapsed/gone.
        try:
            a.close()
        except Exception:
            pass
        b.close()


def test_dead_worker_cannot_renew_after_expiry(shard):
    """A dead worker whose lease lapsed cannot renew its way back in."""
    a = _new_store()
    b = _new_store()
    try:
        ttl = 2.0
        assert a.claim(shard, "A", ttl=ttl) is True
        # wait past expiry
        deadline = time.monotonic() + ttl + 8.0
        while time.monotonic() < deadline and a.get(shard) is not None:
            time.sleep(0.1)
        assert a.get(shard) is None, "lease did not expire"
        # A's renew must fail (its lease/key are gone); B can now claim.
        assert a.renew(shard, "A", ttl=ttl) is False
        assert b.claim(shard, "B", ttl=60) is True
    finally:
        try:
            a.close()
        except Exception:
            pass
        b.close()


# ============================================================================
# 3. Cross-client sharing — two separate connections (two "machines") coordinate
# ============================================================================
def test_two_independent_clients_share_state(shard):
    m1 = _new_store()
    m2 = _new_store()
    try:
        assert m1.claim(shard, "m1", ttl=60) is True
        # m2 is a fully independent connection (separate machine stand-in).
        assert m2.claim(shard, "m2", ttl=60) is False
        assert m2.get(shard).worker == "m1"
        m1.bookmark(shard, "m1", 777)
        assert m2.get(shard).bookmark_offset == 777  # m2 sees m1's progress
        assert shard in m2.list_claims()
    finally:
        m1.close()
        m2.close()


# ============================================================================
# 4. Concurrent claim race — N clients race the SAME shard -> exactly ONE wins
#    (atomic compare-and-swap via etcd txn; no double ownership).
# ============================================================================
def test_concurrent_claim_exactly_one_winner(shard):
    n = 12
    stores = [_new_store() for _ in range(n)]
    barrier = threading.Barrier(n)
    results: list[bool] = [False] * n

    def race(i):
        barrier.wait()  # line everyone up so the claims truly collide
        results[i] = stores[i].claim(shard, f"w{i}", ttl=60)

    threads = [threading.Thread(target=race, args=(i,)) for i in range(n)]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        winners = sum(results)
        assert winners == 1, f"expected exactly 1 winner, got {winners}"
        # The stored owner must be one of the racers, and consistent across a
        # fresh read (no split-brain / double ownership).
        owner = _new_store()
        try:
            c = owner.get(shard)
            assert c is not None
            winning_idx = results.index(True)
            assert c.worker == f"w{winning_idx}"
        finally:
            owner.close()
    finally:
        for s in stores:
            s.close()


# ============================================================================
# 5. Elastic failover over etcd — a worker "dies" mid-run, another reclaims its
#    bucket from the SINK offset and finishes exactly-once, all coordinated
#    through the LIVE etcd claim store. Ties the coordination layer to the real
#    elastic loop. Uses lightweight fake source/sink (no Iceberg) so the test is
#    fast and hermetic; the coordination + exactly-once logic is identical.
# ============================================================================
class _FakeSource:
    """Bounded monotonic ``_offset`` source over [0, until]."""

    def __init__(self, until: int):
        self._until = until

    def latest_offset(self):
        return self._until

    def read_since(self, cur: int, hi=None):
        import pyarrow as pa
        top = self._until if hi is None else min(hi, self._until)
        offs = [o for o in range(cur + 1, top + 1)]
        tbl = pa.table({"_offset": pa.array(offs, pa.int64()),
                        "v": pa.array([o * 10 for o in offs], pa.int64())})
        return tbl, top


class _FakeSink:
    """Thread-safe in-memory sink: append(data, offsets) commits atomically.

    Mirrors leat's contract that the per-bucket offset rides the same commit as
    the data, so a reclaiming worker resumes from ``read_offsets()[key]``.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._offsets: dict[str, int] = {}
        self._rows: list = []

    def read_offsets(self):
        with self._lock:
            return dict(self._offsets)

    def append(self, data, offsets):
        with self._lock:
            for k, v in offsets.items():
                # exactly-once guard: offset only moves forward
                if v <= self._offsets.get(k, -1):
                    return
                self._offsets[k] = v
            try:
                rows = data.to_pylist()
            except Exception:
                rows = []
            self._rows.extend(rows)

    def total_rows(self):
        with self._lock:
            return len(self._rows)


def test_elastic_failover_over_etcd(shard):
    """Worker A claims a bucket, dies mid-drain; worker B reclaims via etcd lease
    expiry and finishes. Verify exactly-once (each source row lands once)."""
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")
    from leat.elastic import run_worker

    until = 40
    name = shard  # unique namespace so bucket keys are unique per test
    src = _FakeSource(until)
    snk = _FakeSink()

    def transform(df):
        return df  # identity silver step

    # --- Worker A: claim the single bucket, commit ONE chunk, then "die". ----
    # We drive A manually (not run_worker) so we can stop it mid-bucket while
    # holding a live-but-soon-to-lapse lease.
    a = _new_store()
    bucket_key = f"{name}.bucket0"
    ttl = 2.0
    assert a.claim(bucket_key, "A", ttl=ttl) is True
    # A commits a partial chunk: source rows (0, 20], advancing the SINK offset.
    part, _ = src.read_since(0, hi=20)
    snk.append(transform_arrow(part), {bucket_key: 20})
    a.bookmark(bucket_key, "A", 20)
    assert snk.read_offsets()[bucket_key] == 20
    # A dies: drop its lease bookkeeping, never renew.
    a._leases.clear()

    # --- Wait for A's etcd lease to lapse so the bucket frees itself. --------
    b_probe = _new_store()
    deadline = time.monotonic() + ttl + 8.0
    while time.monotonic() < deadline and b_probe.get(bucket_key) is not None:
        time.sleep(0.1)
    assert b_probe.get(bucket_key) is None, "A's lease never expired -> no failover"
    b_probe.close()

    # --- Worker B: full elastic loop over the SAME etcd store finishes it. ---
    b_store = _new_store()
    events = []
    stats = run_worker(
        src, snk, transform, name=name, num_buckets=1,
        claim_store=b_store, until=until, worker="B", ttl=30.0,
        batch_rows=1000, idle_sleep=0.02, on_event=events.append,
    )
    b_store.close()

    # B must have reclaimed and completed the bucket from offset 20, not 0.
    assert snk.read_offsets()[bucket_key] == until
    # exactly-once: total sink rows == distinct source offsets [1..until] == until.
    # A wrote rows (0,20]; B wrote (20,40]; no overlap, no gap.
    assert snk.total_rows() == until
    # No source offset was written twice (the exactly-once teeth): every _offset
    # in [1..until] appears exactly once across A's and B's commits.
    seen = sorted(r["_offset"] for r in snk._rows)
    assert seen == list(range(1, until + 1)), "double-write or gap across failover"
    # B genuinely RECLAIMED: the elastic loop emitted a `failover` event because
    # it resumed the bucket above its low bound (from A's committed offset 20).
    assert any(e.get("event") == "failover" for e in events), (
        "expected a failover event — B should have resumed from A's sink offset"
    )
    try:
        a.close()
    except Exception:
        pass


def transform_arrow(tbl):
    """Identity passthrough used by the elastic-failover test's manual step."""
    return tbl
