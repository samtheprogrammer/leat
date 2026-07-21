"""Elastic worker loop — the money demo (REAL OS processes).

Run:  python examples/elastic_demo.py

What it proves, visibly:
  * Anonymous workers are separate OS PROCESSES sharing ONE sqlite ClaimStore and
    ONE Iceberg sink table — genuine cross-process coordination, not threads.
  * SCALE-UP: start 2 workers, then add 4 more mid-flight; the new workers
    immediately absorb still-open buckets (watch the timeline).
  * FAILOVER: kill one worker mid-bucket; its lease lapses and another worker
    RECLAIMS the bucket, resuming from the SINK's committed offset (not from any
    separate bookmark) — so no row is processed twice.
  * EXACTLY-ONCE: at the end the sink equals a single-process reference transform
    of the whole source — same rows, zero duplicates.

Each worker logs (with timestamps) which bucket it claims / commits / completes,
so the output literally shows buckets being absorbed by new workers and a dead
worker's bucket being reclaimed.

Windows note: multiprocessing REQUIRES the ``if __name__ == "__main__":`` guard
and picklable, module-level targets (spawn re-imports this file). Both are honored.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import time

import numpy as np
import pyarrow as pa
import polars as pl

# make `import leat` work when run from the repo root or the examples/ dir
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import leat  # noqa: E402
from leat import run_worker, LocalClaimStore, IcebergFormat  # noqa: E402


# --- config ----------------------------------------------------------------
# A STABLE path (not mkdtemp): the worker subprocesses must all share ONE
# warehouse + ONE claim-store file, so it can't be per-process. Under the OS
# temp dir → cross-platform + no-space. Override with LEAT_DEMO_WH.
WAREHOUSE = os.environ.get("LEAT_DEMO_WH", os.path.join(tempfile.gettempdir(), "leat_elastic_demo"))
CLAIMS_DB = os.path.join(WAREHOUSE, "claims.db")
N_ROWS = 3_000_000
NUM_BUCKETS = 12
BATCH_ROWS = 100_000
TTL = 4.0                 # short lease so a killed worker frees its bucket fast
NAME = "silver"


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Silver clean: keep value > 100, scale it. Applied exactly once per source row."""
    return df.filter(pl.col("value") > 100).with_columns((pl.col("value") * 2).alias("value"))


# --- worker entrypoint (module-level -> picklable for spawn) ----------------
def worker_main(worker_id: str, warehouse: str, claims_db: str, until: int,
                event_q: "mp.Queue", slow: bool = False) -> None:
    """One anonymous worker, run in its OWN process.

    Opens its own catalog + claim-store connection on the SHARED files, then runs
    the elastic loop. Every claim/commit/complete/failover is pushed to the
    controller via ``event_q`` for the unified timeline. ``slow`` throttles a
    worker slightly so it is a reliable kill target mid-bucket.
    """
    lt = leat.connect(warehouse)
    src = IcebergFormat(lt.catalog, "db.events")
    snk = IcebergFormat(lt.catalog, "db.silver")
    store = LocalClaimStore(claims_db)

    def on_event(ev: dict) -> None:
        ev = dict(ev)
        ev["pid"] = os.getpid()
        event_q.put(ev)
        if ev["event"] == "committed":
            # Throttle every worker a little so buckets stay open long enough for
            # OTHER workers to claim remaining buckets (otherwise one fast worker
            # drains everything before its peers even connect). The kill target is
            # extra-slow so it is reliably mid-bucket when terminated.
            time.sleep(0.25 if slow else 0.06)

    try:
        stats = run_worker(src, snk, transform, name=NAME, num_buckets=NUM_BUCKETS,
                           claim_store=store, until=until, worker=worker_id,
                           ttl=TTL, batch_rows=BATCH_ROWS, on_event=on_event)
        event_q.put({"event": "worker_done", "worker": worker_id,
                     "pid": os.getpid(), "t": time.time(),
                     "completed": stats["buckets_completed"],
                     "reclaimed": stats.get("reclaimed", [])})
    finally:
        store.close()


# --- controller -------------------------------------------------------------
def _log(t0: float, line: str) -> None:
    print(f"[{time.time() - t0:6.2f}s] {line}", flush=True)


def _drain(event_q, t0, seen_reclaim, completed_by):
    """Print any pending events; track reclaims and completions."""
    while True:
        try:
            ev = event_q.get_nowait()
        except Exception:
            return
        kind = ev.get("event")
        w = ev.get("worker", "?")
        if kind == "claimed":
            _log(t0, f"  CLAIM    {w:<8} -> {ev['bucket']}  (resume@{ev['resume']}, range {ev['lo']+1}..{ev['hi']})")
        elif kind == "failover":
            seen_reclaim.append((w, ev["bucket"]))
            _log(t0, f"  RECLAIM  {w:<8} picks up {ev['bucket']} from SINK offset {ev['resume']} (a dead worker's bucket)")
        elif kind == "completed":
            _log(t0, f"  DONE     {w:<8} completed {ev['bucket']}")
        elif kind == "worker_done":
            completed_by[w] = ev["completed"]
            _log(t0, f"  EXIT     {w:<8} finished; buckets completed by it: {ev['completed']}")
        # 'committed' events are frequent; summarize instead of spamming


def main() -> int:
    # fresh warehouse (no-space path, Windows-safe)
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    os.makedirs(WAREHOUSE, exist_ok=True)

    print(f"warehouse : {WAREHOUSE}")
    print(f"rows      : {N_ROWS:,}   buckets: {NUM_BUCKETS}   ttl: {TTL}s\n")

    lt = leat.connect(WAREHOUSE)
    schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
    events = lt.create("db.events", schema)
    lt.create("db.silver", schema)

    # ~N_ROWS of source data (a few million); _offset is dense/monotonic.
    rng = np.random.default_rng(20260720)
    _log_t0 = time.time()
    events.append(pa.table({
        "_offset": np.arange(N_ROWS, dtype=np.int64),
        "value": rng.integers(0, 200, N_ROWS, dtype=np.int64),
    }))
    print(f"seeded {N_ROWS:,} source rows in {time.time()-_log_t0:.1f}s")

    until = lt.source("db.events").latest_offset()

    # single-process REFERENCE result (what exactly-once must equal)
    ref = transform(pl.from_arrow(lt.source("db.events").read_all())).sort("_offset")
    expected_rows = ref.height
    print(f"reference (single-process) silver rows: {expected_rows:,}\n")

    ctx = mp.get_context("spawn")  # Windows-safe; also fine on posix
    event_q = ctx.Queue()
    t0 = time.time()
    procs: dict[str, mp.Process] = {}
    seen_reclaim: list = []
    completed_by: dict = {}

    def spawn(wid, slow=False):
        p = ctx.Process(target=worker_main,
                        args=(wid, WAREHOUSE, CLAIMS_DB, until, event_q, slow),
                        daemon=False)
        p.start()
        procs[wid] = p
        _log(t0, f"SPAWN    {wid} (pid {p.pid}){'  [slow/kill-target]' if slow else ''}")

    print("=== TIMELINE ===")
    # Phase 1: start 2 workers; one is 'slow' so we can reliably kill it mid-bucket.
    spawn("w1-kill", slow=True)
    spawn("w2")

    # let them claim & start committing
    kill_deadline = t0 + 2.0
    while time.time() < kill_deadline:
        _drain(event_q, t0, seen_reclaim, completed_by)
        time.sleep(0.05)

    # Phase 2: SCALE-UP — add 4 more workers mid-flight.
    _log(t0, "SCALE-UP: adding 4 more workers")
    for i in range(3, 7):
        spawn(f"w{i}")

    # Phase 3: FAILOVER — kill the slow worker mid-bucket (give scaled-up workers
    # a moment to each grab a bucket first, so the timeline shows real spread).
    time.sleep(1.0)
    _drain(event_q, t0, seen_reclaim, completed_by)
    victim = procs["w1-kill"]
    if victim.is_alive():
        _log(t0, f"KILL     w1-kill (pid {victim.pid}) mid-bucket  <-- FAILOVER TRIGGER")
        victim.terminate()

    # Phase 4: wait for all workers to finish, draining the timeline.
    deadline = time.time() + 90
    while any(p.is_alive() for w, p in procs.items() if w != "w1-kill") and time.time() < deadline:
        _drain(event_q, t0, seen_reclaim, completed_by)
        time.sleep(0.05)
    # final drain
    time.sleep(0.3)
    _drain(event_q, t0, seen_reclaim, completed_by)

    for w, p in procs.items():
        if p.is_alive() and w == "w1-kill":
            continue
        p.join(timeout=10)

    print("=== END TIMELINE ===\n")

    # --- exactly-once verification ------------------------------------------
    snk = lt.source("db.silver")
    got = pl.from_arrow(snk.read_all()).sort("_offset")
    n_got = got.height
    n_unique = got["_offset"].n_unique()

    print("VERIFY")
    print(f"  sink rows          : {n_got:,}")
    print(f"  expected (1 proc)  : {expected_rows:,}")
    print(f"  unique _offset     : {n_unique:,}  (== sink rows means NO duplicates)")

    offsets = snk.read_offsets()
    print(f"  buckets recorded   : {len(offsets)} / {NUM_BUCKETS}")
    reclaimed_buckets = sorted({b for (_, b) in seen_reclaim})
    print(f"  reclaimed buckets  : {reclaimed_buckets or 'none observed'}")
    workers_that_finished = [w for w in completed_by if completed_by[w]]
    print(f"  workers that completed buckets: {workers_that_finished}")

    ok_rows = (n_got == expected_rows)
    ok_dupes = (n_unique == n_got)
    ok_offsets = got["_offset"].to_list() == ref["_offset"].to_list()

    print()
    if ok_rows and ok_dupes and ok_offsets:
        print("EXACTLY-ONCE: PASS")
        print(f"  {NUM_BUCKETS} buckets, {len(procs)} worker processes spawned, "
              f"{'reclaim observed' if reclaimed_buckets else 'no reclaim needed'}, "
              f"scale-up applied. Sink == single-process reference, zero duplicates.")
        return 0
    print("EXACTLY-ONCE: FAIL")
    print(f"  ok_rows={ok_rows} ok_dupes={ok_dupes} ok_offsets={ok_offsets}")
    return 1


if __name__ == "__main__":
    mp.freeze_support()  # Windows spawn safety
    raise SystemExit(main())
