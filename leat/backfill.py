"""Backfill mode — static sharding for the initial load / catch-up.

A single incremental Consumer is too slow for the first full load of a large
table. Backfill splits the offset range into ``num_shards`` disjoint buckets
(``offset % num_shards``) and runs each shard as an independent, **bounded**,
run-to-completion job up to a fixed ``until`` high-water mark (the latest offset
captured at construction). Because each shard is a fixed, finite chunk it needs
no always-on coordinator: it is a checkpointed batch job. When every shard is
done, a normal steady-state :class:`~leat.consumer.Consumer` takes over from
``until``.

Sharding is by modulo on the offset column, bounded by ``until``::

    shard i processes rows where (offset % num_shards) == i AND offset <= until

Reads reuse the existing :class:`~leat.iceberg.IcebergFormat` scan path
(``read_since`` / ``read_all``); the modulo filter is applied in Arrow because
PyIceberg predicates cannot express it. Bounding by ``until`` keeps each shard
finite and lets shards resume from a per-shard checkpoint.

Exactly-once per shard: append the transformed batch, *then* record the
per-shard bookmark (last processed offset). A rerun resumes strictly after the
bookmark, so a crash between append and bookmark at worst reprocesses a batch
whose rows are filtered out on resume (append-then-checkpoint, same contract as
``pipeline.Pipeline.step``).

Optional failover: pass a ``claim_store`` (``leat.coordination.ClaimStore``) and
workers lease shards. A dead worker's lease expires and another worker reclaims
the shard, resuming from its bookmark.
"""
from __future__ import annotations

import socket
import os
from typing import Callable, List, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc


def _default_worker() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


class Backfill:
    """Parallel bounded backfill over a table, sharded by ``offset % num_shards``.

    Parameters
    ----------
    source, sink : IcebergFormat-like
        Control-layer adapters (anything with ``read_all`` / ``latest_offset`` /
        ``append``). Both the incremental ``read_since`` path and full
        ``read_all`` are supported; ``read_all`` + Arrow filter is used because
        the shard predicate is a modulo.
    transform : Callable
        Polars ``df -> df`` (same contract as ``pipeline.Model``).
    num_shards : int
        Number of disjoint shards.
    until : int, optional
        High-water mark; only rows with ``offset <= until`` are processed.
        Defaults to ``source.latest_offset()`` captured now.
    shard_by : str
        Offset column used for both bounding and the modulo bucket.
    checkpoint : store, optional
        ``get(name)/set(name, offset)`` store (e.g. ``JsonCheckpointStore``) for
        per-shard bookmarks. Defaults to an in-memory dict store.
    claim_store : ClaimStore, optional
        Enables lease-based failover. Bookmarks live in the claim store when set.
    worker : str, optional
        This worker's identity for the claim store. Defaults to host:pid.
    name : str
        Checkpoint / claim key namespace. Defaults to ``"backfill"``.
    """

    def __init__(self, source, sink, transform, *, num_shards: int,
                 until: Optional[int] = None, shard_by: str = "_offset",
                 checkpoint=None, claim_store=None, worker: Optional[str] = None,
                 name: str = "backfill", batch_rows: int = 50_000,
                 ttl: float = 30.0):
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")
        self._src = source
        self._snk = sink
        self._transform = transform
        self._num_shards = num_shards
        self._shard_by = shard_by
        self._name = name
        self._batch_rows = batch_rows
        self._ttl = ttl
        self._worker = worker or _default_worker()
        self._claim = claim_store
        self._ckpt = checkpoint if checkpoint is not None else _DictCheckpoint()

        cap = source.latest_offset() if until is None else until
        self._until: int = -1 if cap is None else cap

    # --- keys ---------------------------------------------------------------
    def _ckpt_key(self, shard: int) -> str:
        return f"{self._name}:shard{shard}"

    def _claim_key(self, shard: int) -> str:
        return f"{self._name}:shard{shard}"

    # --- bookmark plumbing (claim store wins if present) --------------------
    def _get_bookmark(self, shard: int) -> Optional[int]:
        if self._claim is not None:
            c = self._claim.get(self._claim_key(shard))
            if c is not None:
                bm = getattr(c, "bookmark_offset", None)
                if bm is not None:
                    return bm
        return self._ckpt.get(self._ckpt_key(shard))

    def _set_bookmark(self, shard: int, offset: int) -> None:
        # Always persist to the durable checkpoint; also to the claim store so a
        # failover worker sees it.
        self._ckpt.set(self._ckpt_key(shard), offset)
        if self._claim is not None:
            self._claim.bookmark(self._claim_key(shard), self._worker, offset)

    def _is_complete(self, shard: int) -> bool:
        if self._claim is not None:
            c = self._claim.get(self._claim_key(shard))
            if c is not None and getattr(c, "status", None) == "done":
                return True
        bm = self._ckpt.get(self._ckpt_key(shard))
        return bm is not None and bm >= self._until

    # --- core ---------------------------------------------------------------
    @property
    def until(self) -> int:
        return self._until

    @property
    def num_shards(self) -> int:
        return self._num_shards

    def _shard_slice(self, shard: int, after: Optional[int]) -> pa.Table:
        """All rows for ``shard`` with ``after < offset <= until``, offset-sorted."""
        data = self._src.read_all()
        if data.num_rows == 0:
            return data
        off = data.column(self._shard_by)
        mask = pc.less_equal(off, pa.scalar(self._until))
        if after is not None:
            mask = pc.and_(mask, pc.greater(off, pa.scalar(after)))
        # bucket = offset % num_shards == shard
        bucket = pc.equal(
            pc.subtract(off, pc.multiply(pc.divide(off, pa.scalar(self._num_shards)),
                                         pa.scalar(self._num_shards))),
            pa.scalar(shard),
        )
        mask = pc.and_(mask, bucket)
        sel = data.filter(mask)
        if sel.num_rows == 0:
            return sel
        order = pc.sort_indices(sel.column(self._shard_by))
        return sel.take(order)

    def run_shard(self, shard: int) -> int:
        """Process ``shard``'s slice up to ``until``, run-to-completion.

        Resumes from the per-shard bookmark. Returns the number of *source* rows
        processed in this call. Idempotent once the shard is complete.
        """
        if shard < 0 or shard >= self._num_shards:
            raise ValueError(f"shard {shard} out of range [0,{self._num_shards})")
        if self._until < 0:
            return 0

        # If a claim store is present, ensure we hold the lease before writing
        # bookmarks (bookmarks are keyed by the owning worker).
        if self._claim is not None:
            if not self._claim.claim(self._claim_key(shard), self._worker, self._ttl):
                return 0  # another live worker owns this shard

        after = self._get_bookmark(shard)
        slice_ = self._shard_slice(shard, after)
        total = slice_.num_rows
        if total == 0:
            # nothing left: mark complete at the high-water mark
            self._finish(shard)
            return 0

        import polars as pl

        processed = 0
        n = slice_.num_rows
        start = 0
        while start < n:
            chunk = slice_.slice(start, self._batch_rows)
            start += chunk.num_rows

            result = self._transform(pl.from_arrow(chunk))
            arrow = result.to_arrow() if hasattr(result, "to_arrow") else result
            if arrow.num_rows:
                self._snk.append(arrow)

            # bookmark = max source offset in this committed chunk (append first,
            # then bookmark -> exactly-once on rerun).
            last_off = pc.max(chunk.column(self._shard_by)).as_py()
            self._set_bookmark(shard, last_off)
            processed += chunk.num_rows

            if self._claim is not None:
                self._claim.renew(self._claim_key(shard), self._worker, self._ttl)

        self._finish(shard)
        return processed

    def _finish(self, shard: int) -> None:
        # Ensure the bookmark reflects completion (>= until) so _is_complete is
        # true even if the shard was empty.
        cur = self._ckpt.get(self._ckpt_key(shard))
        if cur is None or cur < self._until:
            self._ckpt.set(self._ckpt_key(shard), self._until)
        if self._claim is not None:
            self._claim.complete(self._claim_key(shard), self._worker)

    def run(self, shards: Union[str, int, List[int]] = "all") -> int:
        """Run one/all shards in this process.

        ``shards`` may be an int, a list of ints, or ``"all"``. With a
        ``claim_store``, ``"all"`` loops claiming any unclaimed/expired shard
        until every shard is ``done`` — that's the failover behavior.
        """
        if isinstance(shards, int):
            targets = [shards]
        elif shards == "all":
            targets = list(range(self._num_shards))
        else:
            targets = list(shards)

        if self._claim is None:
            return sum(self.run_shard(s) for s in targets)

        # --- claim-store path: lease each shard, retry until all done ---------
        total = 0
        while True:
            remaining = [s for s in targets if not self._is_complete(s)]
            if not remaining:
                return total
            progressed = False
            for s in remaining:
                if self._is_complete(s):
                    continue
                if not self._claim.claim(self._claim_key(s), self._worker, self._ttl):
                    continue  # owned by a live worker; try later
                try:
                    total += self.run_shard(s)
                    progressed = True
                finally:
                    # complete() already released ownership semantics; release the
                    # lease if we did not finish so another worker can reclaim.
                    if not self._is_complete(s):
                        self._claim.release(self._claim_key(s), self._worker)
            if not progressed:
                # every remaining shard is leased by a live worker and we made no
                # progress; nothing more this process can do.
                return total

    def status(self) -> dict:
        """Per-shard: rows-so-far bookmark, and completion."""
        out = {}
        for s in range(self._num_shards):
            bm = self._get_bookmark(s)
            out[s] = {
                "bookmark": bm,
                "complete": self._is_complete(s),
                "until": self._until,
            }
        return out


class _DictCheckpoint:
    """In-memory fallback checkpoint (get/set), matching JsonCheckpointStore."""

    def __init__(self):
        self._d: dict = {}

    def get(self, name: str) -> Optional[int]:
        return self._d.get(name)

    def set(self, name: str, offset: Optional[int]) -> None:
        self._d[name] = offset
