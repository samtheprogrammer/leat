"""Thread-capped launcher for the leat medallion benchmark.

Wraps bench/leat_medallion.py so that BOTH of leat's native engines are pinned to
the SAME N-thread budget as Spark's local[N] — for a fair, equal-CPU head-to-head:

  * Polars  : capped via env POLARS_MAX_THREADS=N  (read at import time).
  * DuckDB  : leat/compute.py calls duckdb.connect() with NO config, so we
              MONKEYPATCH duckdb.connect here (a bench/infra file — leat source is
              untouched) to inject config={'threads': N} on every connection.

Both caps are on TOP of the container's --cpus=N cgroup quota (the hard cap on
total CPU-seconds/wall). The cgroup quota alone is not enough for thread-count
parity because Docker Desktop's --cpus is a CFS quota, not a cpuset: the engines
would still SEE all the WSL2 VM's cores and spin up that many worker threads
(oversubscribing the quota). Capping the pools to N makes leat structurally match
Spark local[N].

Effective thread counts for BOTH engines are printed at startup so the cap is
verifiable in the run log.

Usage (identical flags to leat_medallion.py, plus the cap is read from env
LEAT_THREADS or --threads):
    LEAT_THREADS=4 python bench/run_leat_capped.py --preset small
    python bench/run_leat_capped.py --threads 4 --events 5000000 --users 200000 ...
"""
from __future__ import annotations
import os
import sys

# --- resolve N (thread cap) BEFORE importing polars/duckdb/leat --------------
def _resolve_threads(argv):
    n = os.environ.get("LEAT_THREADS")
    # allow --threads N on the command line too (stripped before delegating)
    if "--threads" in argv:
        i = argv.index("--threads")
        n = argv[i + 1]
        del argv[i:i + 2]
    if n is None:
        return None
    return int(n)


_ARGV = sys.argv[1:]
N = _resolve_threads(_ARGV)

if N is not None:
    # Polars reads this env var at import time -> must be set BEFORE `import polars`.
    os.environ["POLARS_MAX_THREADS"] = str(N)
    # Belt-and-suspenders for other Arrow/threadpool consumers.
    os.environ.setdefault("OMP_NUM_THREADS", str(N))
    os.environ.setdefault("RAYON_NUM_THREADS", str(N))
    os.environ.setdefault("NUMEXPR_MAX_THREADS", str(N))

# --- monkeypatch duckdb.connect to inject a thread cap -----------------------
import duckdb

if N is not None:
    _orig_connect = duckdb.connect

    def _capped_connect(database=":memory:", read_only=False, config=None):
        cfg = dict(config or {})
        cfg.setdefault("threads", N)
        return _orig_connect(database=database, read_only=read_only, config=cfg)

    duckdb.connect = _capped_connect

# --- now import the (thread-affected) engines and report effective caps ------
import polars as pl


def _report_effective():
    print("=" * 62)
    print(f"[thread-cap] requested N = {N}")
    print(f"[thread-cap] POLARS_MAX_THREADS env = {os.environ.get('POLARS_MAX_THREADS')}")
    try:
        print(f"[thread-cap] polars.thread_pool_size() = {pl.thread_pool_size()}")
    except Exception as e:
        print(f"[thread-cap] polars thread pool query failed: {e}")
    try:
        con = duckdb.connect()
        eff = con.execute("SELECT current_setting('threads')").fetchone()[0]
        con.close()
        print(f"[thread-cap] duckdb effective threads = {eff}")
    except Exception as e:
        print(f"[thread-cap] duckdb thread query failed: {e}")
    try:
        print(f"[thread-cap] os.cpu_count() (VM view) = {os.cpu_count()}")
        if hasattr(os, "sched_getaffinity"):
            print(f"[thread-cap] sched_getaffinity = {len(os.sched_getaffinity(0))}")
    except Exception:
        pass
    print("=" * 62)


_report_effective()

# --- delegate to the real harness --------------------------------------------
# Rebuild argv WITHOUT --threads (already stripped) and hand off to the harness'
# argparse-driven main(). Import is AFTER the monkeypatch/env so leat picks them up.
sys.argv = [sys.argv[0]] + _ARGV
import leat_medallion  # noqa: E402  (bench/ is on sys.path when run from bench/)

if __name__ == "__main__":
    leat_medallion.main()
