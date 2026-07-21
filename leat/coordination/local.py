"""LocalClaimStore — SQLite-backed ClaimStore for single-machine coordination.

Works across processes on one box (WAL + a single conditional UPDATE gives us
atomic compare-and-swap without a broker). This is the default backend: perfect
for a multi-process backfill on one node, and the fallback when there's no etcd.

The whole trick is that CAS is expressed as one SQL statement —
`UPDATE ... WHERE holder IS NULL OR lease_expiry < :now` — so SQLite's row lock
serializes racing claimers for us. cursor.rowcount tells us who won.
"""
from __future__ import annotations

import os
import sqlite3
import time
from typing import Optional

from .base import NO_BOOKMARK, Claim, ClaimStore, now_ms

_SCHEMA = """
CREATE TABLE IF NOT EXISTS claims (
    shard            TEXT PRIMARY KEY,
    worker           TEXT,
    lease_expiry_ms  INTEGER NOT NULL,
    bookmark_offset  INTEGER NOT NULL DEFAULT -1,
    status           TEXT NOT NULL DEFAULT 'in_progress'
);
"""


class LocalClaimStore(ClaimStore):
    def __init__(self, path: str = ":memory:"):
        """`path` is a SQLite file (shared across processes) or ':memory:'.

        Note: ':memory:' is per-connection and therefore NOT shared across
        processes — use a real file for cross-process coordination.
        """
        self._path = path
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            os.makedirs(parent, exist_ok=True)
        # isolation_level=None -> autocommit; we manage txns explicitly via BEGIN.
        self._db = sqlite3.connect(path, isolation_level=None, timeout=30.0)
        self._db.row_factory = sqlite3.Row
        # busy_timeout first so the WAL pragma / schema create below also wait out
        # a concurrent writer (multiple processes open this same file at once).
        self._db.execute("PRAGMA busy_timeout=30000")
        if path != ":memory:":
            # The WAL mode-switch takes a brief exclusive lock that busy_timeout
            # does not always honor when many connections open at once (a burst of
            # workers spawning together). Retry it explicitly.
            self._exec_retry("PRAGMA journal_mode=WAL")
        self._exec_retry(_SCHEMA)

    def _exec_retry(self, sql: str, tries: int = 40, delay: float = 0.05) -> None:
        for attempt in range(tries):
            try:
                self._db.execute(sql)
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == tries - 1:
                    raise
                time.sleep(delay)

    # --- helpers -----------------------------------------------------------
    @staticmethod
    def _row_to_claim(row: sqlite3.Row) -> Claim:
        return Claim(
            shard=row["shard"],
            worker=row["worker"],
            lease_expiry_ms=row["lease_expiry_ms"],
            bookmark_offset=row["bookmark_offset"],
            status=row["status"],
        )

    # --- ClaimStore --------------------------------------------------------
    def claim(self, shard: str, worker: str, ttl: float) -> bool:
        now = now_ms()
        expiry = now + int(ttl * 1000)
        cur = self._db.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            # Fresh insert wins if the row does not exist yet.
            cur.execute(
                "INSERT OR IGNORE INTO claims "
                "(shard, worker, lease_expiry_ms, bookmark_offset, status) "
                "VALUES (?, ?, ?, ?, 'in_progress')",
                (shard, worker, expiry, NO_BOOKMARK),
            )
            if cur.rowcount == 1:
                cur.execute("COMMIT")
                return True
            # Row exists: atomic CAS — take it iff unheld, expired, or already ours.
            cur.execute(
                "UPDATE claims SET worker = ?, lease_expiry_ms = ?, status = 'in_progress' "
                "WHERE shard = ? AND (worker IS NULL OR lease_expiry_ms <= ? OR worker = ?)",
                (worker, expiry, shard, now, worker),
            )
            won = cur.rowcount == 1
            cur.execute("COMMIT")
            return won
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def renew(self, shard: str, worker: str, ttl: float) -> bool:
        now = now_ms()
        expiry = now + int(ttl * 1000)
        cur = self._db.cursor()
        # Only extend if still ours and not yet expired (a lapsed lease is not renewable).
        cur.execute(
            "UPDATE claims SET lease_expiry_ms = ? "
            "WHERE shard = ? AND worker = ? AND lease_expiry_ms > ?",
            (expiry, shard, worker, now),
        )
        return cur.rowcount == 1

    def bookmark(self, shard: str, worker: str, offset: int) -> None:
        self._db.execute(
            "UPDATE claims SET bookmark_offset = ? WHERE shard = ? AND worker = ?",
            (offset, shard, worker),
        )

    def get(self, shard: str) -> Optional[Claim]:
        row = self._db.execute(
            "SELECT * FROM claims WHERE shard = ? AND worker IS NOT NULL",
            (shard,),
        ).fetchone()
        return self._row_to_claim(row) if row else None

    def complete(self, shard: str, worker: str) -> None:
        self._db.execute(
            "UPDATE claims SET status = 'done' WHERE shard = ? AND worker = ?",
            (shard, worker),
        )

    def release(self, shard: str, worker: str) -> None:
        # Clear the holder (keep bookmark/status history by deleting the row).
        self._db.execute(
            "DELETE FROM claims WHERE shard = ? AND worker = ?",
            (shard, worker),
        )

    def list_claims(self) -> dict[str, Claim]:
        rows = self._db.execute(
            "SELECT * FROM claims WHERE worker IS NOT NULL"
        ).fetchall()
        return {row["shard"]: self._row_to_claim(row) for row in rows}

    def close(self) -> None:
        self._db.close()
