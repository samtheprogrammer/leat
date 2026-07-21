"""Elastic worker loop — anonymous workers that claim, process, and survive death.

This is backfill's successor. Where :mod:`leat.backfill` uses *static* modulo
shards and a *separate* bookmark store, the elastic model is:

1. **Anonymous workers.** No worker is assigned a bucket up front. Every worker
   runs the SAME loop: find a bucket nobody live is working, claim it, drain it,
   move on. Scale = start more processes; they self-balance.

2. **Contiguous offset-range buckets.** The bounded range ``(0, until]`` is split
   into ``num_buckets`` even, adjacent ranges: bucket ``i`` = ``(lo_i, hi_i]``.
   Contiguous (not modulo) so each bucket read is a single RANGE the table format
   prunes on both ends; because ``_offset`` is monotonic/dense the buckets are
   also perfectly load-balanced. Bucket key = ``f"{name}.bucket{i}"``.

3. **Resume comes from the SINK, not the ClaimStore.** The offset for a bucket
   rides the sink's own commit (``sink.append(data, offsets={key: off})`` is one
   atomic transaction). So a reclaiming worker reads
   ``sink.read_offsets()[key]`` and continues from exactly there. A worker killed
   mid-batch either committed (offset advanced atomically) or didn't (offset
   unchanged) — never half — so the replacement never double-writes. The
   ClaimStore therefore manages **ownership/lease only** (who is on a bucket right
   now), never progress. ``bookmark``/``get`` are used for observability only.

4. **Concurrent multi-writer sink.** Several worker PROCESSES append to the same
   sink table at once. PyIceberg/Delta appends are optimistic-concurrency commits;
   on the SQLite catalog a racing commit can raise "database is locked" or
   ``CommitFailedException``. :func:`_append_with_retry` retries with jittered
   backoff. Exactly-once still holds: a losing commit simply never happened (the
   offset didn't advance), so the retry re-reads the same delta and re-appends —
   the bucket's offset only ever moves forward on a *successful* commit.
"""
from __future__ import annotations

import os
import random
import socket
import time
from typing import Callable, Optional

import pyarrow as pa
import pyarrow.compute as pc


def _default_worker() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _buckets(until: int, num_buckets: int, name: str):
    """Return ``[(key, lo, hi), ...]`` splitting ``(0, until]`` into contiguous ranges.

    Bucket ``i`` owns ``lo_i < _offset <= hi_i``. ``lo_0 = 0`` (offsets are
    typically ``[0, until]``, so ``> 0`` would drop offset 0 — see below). To keep
    offset ``0`` we treat the very first bucket's lower bound as ``-1`` so its
    predicate ``_offset > -1`` includes 0. Ranges are even; the last bucket
    absorbs the remainder.
    """
    if num_buckets < 1:
        raise ValueError("num_buckets must be >= 1")
    n = num_buckets
    span = until + 1  # offsets [0, until] inclusive
    step = max(1, span // n)
    out = []
    lo = -1  # so bucket 0's `_offset > lo` includes offset 0
    for i in range(n):
        if i == n - 1:
            hi = until
        else:
            hi = (i + 1) * step - 1
            if hi < lo:
                hi = lo
            if hi > until:
                hi = until
        out.append((f"{name}.bucket{i}", lo, hi))
        lo = hi
    return out


def _append_with_retry(sink, data: pa.Table, offsets: dict, *,
                       tries: int = 8, base: float = 0.05, cap: float = 2.0):
    """Append data + offset atomically, retrying optimistic-concurrency failures.

    Multiple processes commit to one table; the SQLite catalog serializes commits
    and a loser raises (locked / CommitFailed). We retry with jittered exponential
    backoff. This is safe for exactly-once because a failed commit did NOT advance
    the sink offset — the caller re-derives the same delta and retries; only a
    committed append moves the bucket forward.
    """
    last = None
    for attempt in range(tries):
        try:
            sink.append(data, offsets=offsets)
            return
        except Exception as e:  # noqa: BLE001 — backend-specific commit/lock errors
            last = e
            if attempt == tries - 1:
                break
            sleep = min(cap, base * (2 ** attempt)) * (0.5 + random.random())
            time.sleep(sleep)
    raise last


class ElasticRunner:
    """Runs ONE anonymous elastic worker over ``num_buckets`` contiguous buckets.

    See module docstring for the model. Prefer :func:`run_worker` unless you want
    to introspect the bucket plan.
    """

    def __init__(self, source, sink, transform: Callable, *, name: str,
                 num_buckets: int, claim_store, until: Optional[int] = None,
                 worker: Optional[str] = None, ttl: float = 30.0,
                 batch_rows: int = 200_000, idle_sleep: float = 0.2,
                 on_event: Optional[Callable] = None):
        self.src = source
        self.snk = sink
        self.transform = transform
        self.name = name
        self.claim = claim_store
        self.worker = worker or _default_worker()
        self.ttl = ttl
        self.batch_rows = batch_rows
        self.idle_sleep = idle_sleep
        self.on_event = on_event

        cap = source.latest_offset() if until is None else until
        self.until: int = -1 if cap is None else cap
        self.buckets = _buckets(self.until, num_buckets, name) if self.until >= 0 else []
        self._by_key = {k: (lo, hi) for (k, lo, hi) in self.buckets}

    # --- events -------------------------------------------------------------
    def _emit(self, kind: str, **kw) -> None:
        if self.on_event is not None:
            ev = {"event": kind, "worker": self.worker, "t": time.time(), **kw}
            try:
                self.on_event(ev)
            except Exception:  # pragma: no cover — logging must never break the loop
                pass

    # --- completion detection (sink offset >= hi OR claim status done) ------
    def _bucket_done(self, key: str, hi: int, offsets: Optional[dict] = None) -> bool:
        if offsets is None:
            offsets = self.snk.read_offsets()
        if offsets.get(key, -1) >= hi:
            return True
        c = self.claim.get(key)
        return c is not None and getattr(c, "status", None) == "done"

    def all_done(self) -> bool:
        if self.until < 0:
            return True
        offsets = self.snk.read_offsets()
        return all(self._bucket_done(k, hi, offsets) for (k, lo, hi) in self.buckets)

    def _claim_is_live(self, key: str) -> bool:
        c = self.claim.get(key)
        if c is None:
            return False
        expiry = getattr(c, "lease_expiry_ms", None)
        if expiry is None:
            return False
        return (time.time() * 1000) < expiry

    # --- the loop -----------------------------------------------------------
    def run(self) -> dict:
        stats = {"worker": self.worker, "buckets_completed": [],
                 "buckets_worked": [], "rows_out": 0, "chunks": 0,
                 "reclaimed": [], "until": self.until,
                 "num_buckets": len(self.buckets)}
        if self.until < 0:
            return stats

        while True:
            offsets = self.snk.read_offsets()
            # find a candidate: not complete, not live-claimed-by-another
            candidate = None
            remaining = 0
            for (key, lo, hi) in self.buckets:
                if self._bucket_done(key, hi, offsets):
                    continue
                remaining += 1
                if self._claim_is_live(key):
                    continue
                candidate = (key, lo, hi)
                break

            if remaining == 0:
                return stats
            if candidate is None:
                # everything left is held by a live worker; wait and re-check
                time.sleep(self.idle_sleep)
                continue

            key, lo, hi = candidate
            if not self.claim.claim(key, self.worker, self.ttl):
                # lost the race; loop again
                time.sleep(self.idle_sleep * random.random())
                continue

            try:
                worked = self._drain_bucket(key, lo, hi, stats)
                if worked and key not in stats["buckets_worked"]:
                    stats["buckets_worked"].append(key)
            finally:
                # if we did not finish it, release so another worker can reclaim now
                if not self._bucket_done(key, hi):
                    self.claim.release(key, self.worker)

        return stats

    def _drain_bucket(self, key: str, lo: int, hi: int, stats: dict) -> bool:
        """Process bucket ``key`` from its SINK-recorded offset to ``hi``.

        Returns True if this worker committed at least one chunk here.
        """
        import polars as pl

        resume = self.snk.read_offsets().get(key, lo)
        if resume >= hi:
            self._finish(key, hi)
            return False

        # Detect a reclaim: someone (dead worker) already advanced this bucket.
        if resume > lo:
            stats.setdefault("reclaimed", [])
            if key not in stats["reclaimed"]:
                stats["reclaimed"].append(key)
            self._emit("failover", bucket=key, resume=resume, lo=lo, hi=hi)

        self._emit("claimed", bucket=key, resume=resume, lo=lo, hi=hi)

        worked = False
        cur = resume
        while cur < hi:
            data, _ = self.src.read_since(cur, hi=hi)
            if data.num_rows == 0:
                break
            # bounded read guarantees _offset in (cur, hi]; sort + chunk by offset
            order = pc.sort_indices(data.column("_offset"))
            data = data.take(order)

            n = data.num_rows
            start = 0
            while start < n:
                chunk = data.slice(start, self.batch_rows)
                start += chunk.num_rows
                new_off = pc.max(chunk.column("_offset")).as_py()

                result = self.transform(pl.from_arrow(chunk))
                arrow = result.to_arrow() if hasattr(result, "to_arrow") else result

                # ATOMIC: data + this bucket's offset commit together (retry on
                # concurrent-commit contention). Even if transform dropped every
                # row we still commit an empty append to advance the offset.
                _append_with_retry(self.snk, arrow, {key: new_off})
                cur = new_off
                worked = True
                stats["rows_out"] += arrow.num_rows
                stats["chunks"] += 1
                self.claim.renew(key, self.worker, self.ttl)
                self.claim.bookmark(key, self.worker, new_off)  # observability only
                self._emit("committed", bucket=key, offset=new_off,
                           rows_out=arrow.num_rows, hi=hi)
            # loop re-reads from `cur` in case the bucket held more than one read
            if start == 0:
                break

        self._finish(key, hi)
        if key not in stats["buckets_completed"]:
            stats["buckets_completed"].append(key)
        self._emit("completed", bucket=key, hi=hi)
        return worked

    def _finish(self, key: str, hi: int) -> None:
        self.claim.complete(key, self.worker)


def run_worker(source, sink, transform, *, name: str, num_buckets: int,
               claim_store, until: Optional[int] = None,
               worker: Optional[str] = None, ttl: float = 30.0,
               batch_rows: int = 200_000, idle_sleep: float = 0.2,
               on_event: Optional[Callable] = None) -> dict:
    """Run one anonymous elastic worker until every bucket is complete.

    Parameters
    ----------
    source, sink : TableFormat
        Iceberg/Delta adapters. ``sink.append(data, offsets={key: off})`` must
        embed the offset in the same commit (both adapters do).
    transform : Callable
        Polars ``df -> df`` (or Arrow out).
    name : str
        Namespace for bucket keys (``f"{name}.bucket{i}"``).
    num_buckets : int
        How many contiguous offset-range buckets to split ``(0, until]`` into.
    claim_store : ClaimStore
        Shared ownership/lease store (e.g. ``LocalClaimStore`` on a shared sqlite
        file). Manages *who owns a bucket now* — NOT progress.
    until : int, optional
        High-water mark; defaults to ``source.latest_offset()`` captured now.
    worker : str, optional
        This worker's identity. Defaults to ``host:pid``.
    ttl : float
        Lease seconds; renewed each committed chunk.
    batch_rows : int
        Max source rows per atomic commit.
    on_event : Callable, optional
        Called with a dict per event: ``claimed`` / ``committed`` / ``completed``
        / ``failover``. For demo/observability logging.

    Returns a stats dict (buckets worked/completed, rows out, chunks, reclaimed).
    """
    return ElasticRunner(
        source, sink, transform, name=name, num_buckets=num_buckets,
        claim_store=claim_store, until=until, worker=worker, ttl=ttl,
        batch_rows=batch_rows, idle_sleep=idle_sleep, on_event=on_event,
    ).run()
