"""In-container launcher that runs spark-submit as a CHILD process and captures
its WHOLE-PROCESS resource usage (JVM-inclusive) via resource.getrusage(
RUSAGE_CHILDREN).

Why this exists
---------------
Under local[*], Spark's compute runs in a JVM that is embedded in / a child of
the spark-submit process. The Python driver's own RUSAGE_SELF does NOT include
that JVM CPU. `/usr/bin/time -v` would give the whole-process totals, but it is
NOT installed in apache/spark:3.5.3 and installing it needs apt+network on every
run. Spawning spark-submit as a subprocess here and reading RUSAGE_CHILDREN gives
the identical numbers (whole-process user+system CPU-seconds and peak RSS in KB)
with zero extra dependencies.

What it measures (AUTHORITATIVE, JVM-inclusive):
  - CPU-seconds  = ru_utime + ru_stime of the spark-submit child (JVM + Python).
  - Peak RSS     = ru_maxrss (Linux: KB) of the child — this is the JVM heap +
                   off-heap + Python, i.e. the real instance-size floor.
  - wall-seconds = wall-clock of the whole spark-submit.
  - eff cores    = CPU-seconds / wall-seconds.

These are WHOLE-RUN totals (cold start + backfill + all K cycles in one process).
The per-stage CPU split (backfill vs per-cycle) is APPORTIONED by wall-clock in
the README, and clearly labelled as an estimate, because RUSAGE only reports the
child total on exit, not per-stage. The Python-side RUSAGE_SELF split written by
spark_medallion.py (resource_driver_self) is reported alongside to make the
JVM-vs-driver gap explicit.

Writes the whole-run block into results_spark.json AFTER spark_medallion.py has
written it (this launcher runs spark_medallion.py itself, so ordering is safe).
"""
from __future__ import annotations
import json
import os
import resource
import subprocess
import sys
import time

WORK = os.environ.get("BENCH_WORK", "/work")
RESULTS_SPARK = os.path.join(WORK, "results_spark.json")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")


def main() -> int:
    # Everything after "--" on our argv is passed through to spark-submit.
    if "--" in sys.argv:
        submit_args = sys.argv[sys.argv.index("--") + 1:]
    else:
        submit_args = sys.argv[1:]
    cmd = [SPARK_SUBMIT] + submit_args
    print(f"[launch] running as child for RUSAGE_CHILDREN capture:\n         {' '.join(cmd)}\n")

    t0 = time.perf_counter()
    proc = subprocess.run(cmd)
    wall_s = time.perf_counter() - t0

    ru = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_s = ru.ru_utime + ru.ru_stime          # whole-process (JVM + Python)
    peak_rss_mb = ru.ru_maxrss / 1024.0         # Linux ru_maxrss is in KB
    eff_cores = (cpu_s / wall_s) if wall_s > 0 else 0.0

    print(f"\n[launch] ===== WHOLE-RUN resource usage (RUSAGE_CHILDREN, JVM-inclusive) =====")
    print(f"[launch] wall        : {wall_s:8.2f} s")
    print(f"[launch] cpu-seconds : {cpu_s:8.2f} s  (user+system, JVM+Python)")
    print(f"[launch] peak RSS    : {peak_rss_mb:8.1f} MB")
    print(f"[launch] eff cores   : {eff_cores:8.2f}")
    print(f"[launch] spark-submit exit code = {proc.returncode}")

    # Merge the whole-run block into results_spark.json (written by the child).
    whole_run = {
        "method": "resource.getrusage(RUSAGE_CHILDREN) over spark-submit child",
        "note": ("AUTHORITATIVE JVM-inclusive whole-run totals. Covers cold start "
                 "+ full backfill + all K incremental cycles in ONE spark-submit. "
                 "Peak RSS is whole-process (JVM heap+off-heap+Python). Per-stage "
                 "CPU split is apportioned by wall-clock in the README (estimate)."),
        "wall_s": wall_s,
        "cpu_seconds": cpu_s,
        "peak_rss_mb": peak_rss_mb,
        "eff_cores": eff_cores,
    }
    if proc.returncode == 0 and os.path.exists(RESULTS_SPARK):
        try:
            with open(RESULTS_SPARK) as f:
                results = json.load(f)
            results["resource_whole_run"] = whole_run
            # Apportion whole-run CPU across backfill vs incremental by wall-clock,
            # then subtract cold start (approx: cold start is mostly wall, low CPU).
            bf = results.get("full_backfill", {})
            inc = results.get("incremental", {})
            bf_wall = bf.get("dag_total_s")
            inc_walls = [t / 1e3 for t in inc.get("per_cycle_ms", [])]
            inc_wall_total = sum(inc_walls)
            work_wall = (bf_wall or 0) + inc_wall_total
            if work_wall > 0:
                # Fraction of CPU-seconds attributable to real DAG work (backfill +
                # cycles), assuming cold start burns comparatively little CPU. This
                # is an ESTIMATE (see README note).
                bf_cpu_est = cpu_s * (bf_wall / work_wall) if bf_wall else None
                inc_cpu_est_total = cpu_s * (inc_wall_total / work_wall)
                per_cycle_cpu_est = (inc_cpu_est_total / len(inc_walls)
                                     if inc_walls else None)
                results["resource_apportioned_estimate"] = {
                    "note": ("CPU-seconds split by WALL-CLOCK share of whole-run CPU "
                             "(RUSAGE_CHILDREN). ESTIMATE, not directly measured — "
                             "RUSAGE only reports the child total on exit. Cold-start "
                             "CPU is folded in proportionally; treat backfill/per-cycle "
                             "CPU as upper bounds."),
                    "backfill_cpu_seconds_est": bf_cpu_est,
                    "per_cycle_cpu_seconds_est_mean": per_cycle_cpu_est,
                    "backfill_wall_s": bf_wall,
                    "per_cycle_wall_s_mean": (inc_wall_total / len(inc_walls)
                                              if inc_walls else None),
                }
            with open(RESULTS_SPARK, "w") as f:
                json.dump(results, f, indent=2)
            print(f"[launch] merged resource_whole_run + apportioned estimate into {RESULTS_SPARK}")
        except Exception as e:  # pragma: no cover
            print(f"[launch] WARNING: could not merge into results_spark.json: {e}")

    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
