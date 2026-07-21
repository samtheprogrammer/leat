"""Tests for the coordination layer (ClaimStore).

The LocalClaimStore is exercised for real (it must actually run). The etcd
backend can't be integration-tested here (no live server), so we cover the
protobuf round-trip that its wire format depends on, plus the URI factory.
"""
from __future__ import annotations

import os
import time

import pytest

from leat.coordination import (
    Claim,
    LocalClaimStore,
    open_claim_store,
)
from leat.coordination.proto import claim_pb2


# --- protobuf round-trip ---------------------------------------------------
def test_protobuf_round_trip():
    msg = claim_pb2.Claim(
        shard="shard-7",
        worker="worker-A",
        lease_expiry_ms=1_723_456_789_000,
        bookmark_offset=42,
        status="done",
    )
    raw = msg.SerializeToString()
    assert isinstance(raw, bytes)

    back = claim_pb2.Claim.FromString(raw)
    assert back.shard == "shard-7"
    assert back.worker == "worker-A"
    assert back.lease_expiry_ms == 1_723_456_789_000
    assert back.bookmark_offset == 42
    assert back.status == "done"


def test_protobuf_defaults_round_trip():
    # proto3 defaults: empty string / 0.
    msg = claim_pb2.Claim(shard="s")
    back = claim_pb2.Claim.FromString(msg.SerializeToString())
    assert back.shard == "s"
    assert back.worker == ""
    assert back.lease_expiry_ms == 0
    assert back.bookmark_offset == 0
    assert back.status == ""


# --- LocalClaimStore -------------------------------------------------------
@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "claims.db")


def test_claim_is_exclusive(db_path):
    cs = LocalClaimStore(db_path)
    try:
        assert cs.claim("0", "A", ttl=60) is True
        assert cs.claim("0", "B", ttl=60) is False  # already held by A
        c = cs.get("0")
        assert c is not None and c.worker == "A" and c.status == "in_progress"
    finally:
        cs.close()


def test_reclaim_by_owner_is_idempotent(db_path):
    cs = LocalClaimStore(db_path)
    try:
        assert cs.claim("0", "A", ttl=60) is True
        assert cs.claim("0", "A", ttl=60) is True  # re-acquire extends, still True
    finally:
        cs.close()


def test_expired_claim_is_reclaimable(db_path):
    cs = LocalClaimStore(db_path)
    try:
        assert cs.claim("0", "A", ttl=0.2) is True
        assert cs.claim("0", "B", ttl=60) is False  # not expired yet
        time.sleep(0.35)
        assert cs.claim("0", "B", ttl=60) is True  # A's lease lapsed -> B wins
        assert cs.get("0").worker == "B"
    finally:
        cs.close()


def test_renew_extends_only_for_owner(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=0.3)
        assert cs.renew("0", "A", ttl=60) is True
        assert cs.renew("0", "B", ttl=60) is False  # not B's claim
        # After renew the lease is far in the future, so B still can't claim.
        assert cs.claim("0", "B", ttl=60) is False
    finally:
        cs.close()


def test_renew_after_expiry_fails(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=0.2)
        time.sleep(0.35)
        assert cs.renew("0", "A", ttl=60) is False  # lease already lapsed
    finally:
        cs.close()


def test_bookmark_persists(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=60)
        assert cs.get("0").bookmark_offset == -1
        cs.bookmark("0", "A", 1234)
        assert cs.get("0").bookmark_offset == 1234
        # A non-owner cannot move the bookmark.
        cs.bookmark("0", "B", 9999)
        assert cs.get("0").bookmark_offset == 1234
    finally:
        cs.close()


def test_complete_sets_status(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=60)
        cs.complete("0", "A")
        assert cs.get("0").status == "done"
    finally:
        cs.close()


def test_release_frees_shard(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=60)
        cs.release("0", "A")
        assert cs.get("0") is None
        assert cs.claim("0", "B", ttl=60) is True  # freed -> B can take it
    finally:
        cs.close()


def test_list_claims_and_get(db_path):
    cs = LocalClaimStore(db_path)
    try:
        cs.claim("0", "A", ttl=60)
        cs.claim("1", "B", ttl=60)
        cs.claim("2", "A", ttl=60)
        claims = cs.list_claims()
        assert set(claims) == {"0", "1", "2"}
        assert claims["0"].worker == "A"
        assert claims["1"].worker == "B"
        assert isinstance(claims["2"], Claim)
        assert cs.get("nope") is None
    finally:
        cs.close()


def test_cross_process_sharing(db_path):
    """Two independent stores on the same file see each other -> proves shared state."""
    a = LocalClaimStore(db_path)
    b = LocalClaimStore(db_path)
    try:
        assert a.claim("0", "A", ttl=60) is True
        # b is a separate connection/instance (stand-in for another process).
        assert b.claim("0", "B", ttl=60) is False
        assert b.get("0").worker == "A"
        a.bookmark("0", "A", 500)
        assert b.get("0").bookmark_offset == 500
        assert set(b.list_claims()) == {"0"}
    finally:
        a.close()
        b.close()


# --- factory ---------------------------------------------------------------
def test_open_claim_store_sqlite(tmp_path):
    path = str(tmp_path / "f.db")
    cs = open_claim_store(f"sqlite:///{path}")
    try:
        assert isinstance(cs, LocalClaimStore)
        assert cs.claim("0", "A", ttl=60) is True
    finally:
        cs.close()
    assert os.path.exists(path)


def test_open_claim_store_bad_uri():
    with pytest.raises(ValueError):
        open_claim_store("redis://localhost:6379")


def test_open_claim_store_context_manager(tmp_path):
    path = str(tmp_path / "ctx.db")
    with open_claim_store(f"sqlite:///{path}") as cs:
        assert cs.claim("0", "A", ttl=60) is True
