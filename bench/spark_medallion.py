"""Spark side of the medallion benchmark — head-to-head vs leat.

Runs the SAME medallion DAG as bench/leat_medallion.py over the SAME generated data
(materialized to neutral parquet by materialize_data.py), and proves Spark produces
the IDENTICAL gold table (same sha256 fingerprint) as leat.

DAG (mirrors leat exactly):
  bronze.events -> silver.events : filter value > 0 AND value < CAP   (CAP=900)
  bronze.users  -> silver.users  : pass-through; dedupe (latest _offset
                                   per user_id) resolved at the gold join
  gold.country_rollup : per cycle, join the silver.events DELTA to the
                        full deduped silver.users dimension AS OF THAT CYCLE,
                        aggregate SUM(value), COUNT(*) per country, then
                        additively MERGE into running gold totals.

CRITICAL SCD note: user deltas MUTATE existing users' country. leat joins each
cycle's event-delta against the users dimension AS OF that cycle (backfill + user
deltas 0..c appended so far, deduped latest-offset-wins). We mirror that exact
per-cycle procedure: append user delta c, THEN process event delta c. This makes
the running-total merge match leat bit-for-bit.

Two regimes measured (same as leat):
  1. FULL BACKFILL  : read full bronze parquet, run whole DAG once -> gold.
                      Time total + per-stage (silver.users, silver.events, gold).
  2. INCREMENTAL    : K cycles; each reads that cycle's delta parquet, runs the
                      incremental DAG, merges gold. per-cycle mean/p50/p95/min/max.
  + COLD START      : process launch -> SparkSession ready -> first action done.
                      Measured separately, kept OUT of steady-state numbers.

Storage: reads the neutral parquet directly (see run_spark.ps1 / report for the
storage-layer note). Correctness/parity + timings are what matter here.

Runs INSIDE the apache/spark:3.5.3 container via spark-submit (see run_spark.ps1).
Paths below are the in-container mount points (/work = bench/).
"""
from __future__ import annotations
import hashlib
import json
import os
import statistics
import time

try:
    import resource  # POSIX only (we run inside the Linux spark container)
except ImportError:  # pragma: no cover - not available on Windows host
    resource = None

# ---- cold start: measured from the VERY FIRST line of real work -------------
_T_PROC = time.perf_counter()


def _rusage_self_cpu_s():
    """User+system CPU-seconds of THIS (Python driver) process only.
    IMPORTANT: under local[*], Spark's compute runs in an embedded JVM that is a
    CHILD of this driver, so RUSAGE_SELF does NOT include the JVM's CPU. We record
    it anyway to demonstrate the gap vs the whole-process /usr/bin/time (or the
    RUSAGE_CHILDREN captured by run_spark's launcher). See README resource notes."""
    if resource is None:
        return None
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return ru.ru_utime + ru.ru_stime


def _rusage_self_maxrss_mb():
    """Peak RSS of the Python driver process (Linux: ru_maxrss is in KB)."""
    if resource is None:
        return None
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# In-container paths (mounted): /work == bench/
WORK = os.environ.get("BENCH_WORK", "/work")
DATA = os.path.join(WORK, "data")
RESULTS_SPARK = os.path.join(WORK, "results_spark.json")
GOLD_SPARK_DIR = os.path.join(WORK, "_gold_spark_out")   # spark writes a dir
GOLD_SPARK_PARQUET = os.path.join(WORK, "gold_spark.parquet")

CAP = 900   # G.VALUE_CAP — silver.events keeps 0 < value < CAP


class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter(); return self
    def __exit__(self, *a):
        self.dt = time.perf_counter() - self.t0


def load_cfg() -> dict:
    with open(os.path.join(WORK, "results_leat.json")) as f:
        return json.load(f)["config"]


# ---------------------------------------------------------------------------
# Transforms (mirror leat semantics)
# ---------------------------------------------------------------------------
def silver_events(df):
    """value > 0 AND value < CAP."""
    return df.filter((F.col("value") > 0) & (F.col("value") < CAP))


def dedupe_users(users_df):
    """silver.users resolved: keep the row with the latest _offset per user_id,
    return (user_id, country). Mirrors leat.dedupe_users (sort by _offset, last)."""
    w = Window.partitionBy("user_id").orderBy(F.col("_offset").desc())
    return (users_df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select("user_id", "country"))


def gold_partial(events_delta, users_dim):
    """Join events delta to the deduped user dimension, aggregate the DELTA per
    country -> (country, total_value, row_count). Mirrors leat.gold_partial."""
    joined = events_delta.join(users_dim, on="user_id", how="inner")
    return (joined.groupBy("country")
            .agg(F.sum("value").cast("long").alias("total_value"),
                 F.count(F.lit(1)).cast("long").alias("row_count")))


def gold_merge(running_rows: dict, partial_df):
    """Additively merge a per-country partial (collected to driver, tiny <=200 rows)
    into the running gold totals dict {country: [total_value, row_count]}."""
    for r in partial_df.collect():
        c = int(r["country"])
        tv = int(r["total_value"])
        rc = int(r["row_count"])
        if c in running_rows:
            running_rows[c][0] += tv
            running_rows[c][1] += rc
        else:
            running_rows[c] = [tv, rc]
    return running_rows


def gold_fingerprint(running_rows: dict) -> dict:
    """EXACT replica of leat.gold_fingerprint: sorted (country,total_value,row_count)
    tuples -> newline-joined 'c,v,n' -> sha256. Same column order, same sort, same hash."""
    rows = [(c, running_rows[c][0], running_rows[c][1])
            for c in sorted(running_rows.keys())]
    blob = "\n".join(f"{c},{v},{n}" for c, v, n in rows).encode()
    return {
        "sha256": hashlib.sha256(blob).hexdigest(),
        "num_countries": len(rows),
        "grand_total_value": int(sum(r[1] for r in rows)),
        "grand_row_count": int(sum(r[2] for r in rows)),
        "rows": rows,
    }


# ---------------------------------------------------------------------------
def main():
    cfg = load_cfg()
    print(f"\n=== Spark medallion benchmark ===")
    print(f"config : {cfg}")
    print(f"storage: neutral parquet (read directly); DAG mirrors leat exactly\n")

    # ---- COLD START: SparkSession init + first action ----
    # Master is overridable via SPARK_MASTER (default local[*]) so the equal-CPU
    # harness can pin Spark to local[N] to match leat's N-thread cap. Keeping the
    # default local[*] preserves the old (unfair) runs.
    _master = os.environ.get("SPARK_MASTER", "local[*]")
    t_sess0 = time.perf_counter()
    spark = (SparkSession.builder
             .master(_master)
             .appName("spark_medallion")
             .config("spark.sql.shuffle.partitions", "8")   # fair for small data
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    t_sess_ready = time.perf_counter()
    # first action (forces JVM/codegen warmup, mirrors a real "first query")
    spark.range(1).count()
    t_first_action = time.perf_counter()
    cold_session_s = t_sess_ready - t_sess0
    cold_total_s = t_first_action - _T_PROC
    print(f"[cold start] SparkSession init : {cold_session_s*1e3:8.0f} ms")
    print(f"[cold start] launch->first action total : {cold_total_s*1e3:8.0f} ms\n")

    # =====================================================================
    # REGIME 1: FULL BACKFILL
    # =====================================================================
    print("[regime 1] full backfill")
    # RUSAGE_SELF CPU marker at the start of the backfill DAG (Python driver only;
    # does NOT include JVM under local[*] — see _rusage_self_cpu_s docstring).
    _cpu_self_backfill0 = _rusage_self_cpu_s()
    bronze_events = spark.read.parquet(os.path.join(DATA, "bronze_events_full.parquet"))
    bronze_users = spark.read.parquet(os.path.join(DATA, "bronze_users_full.parquet"))

    # silver.users (pass-through; we time the materialization/count as leat times its run)
    with Timer() as t_su:
        su = bronze_users            # pass-through
        su.cache()
        su_rows = su.count()
    # silver.events (filter)
    with Timer() as t_se:
        se = silver_events(bronze_events)
        se.cache()
        se_rows = se.count()
    # gold: join full silver.events delta x deduped users dim, agg, merge into empty gold
    running = {}
    with Timer() as t_g:
        dim = dedupe_users(su)
        partial = gold_partial(se, dim)
        partial.cache()
        gold_rows = se_rows   # rows fed into gold = all surviving silver events
        running = gold_merge(running, partial)
        partial.unpersist()

    backfill_total = t_su.dt + t_se.dt + t_g.dt
    _cpu_self_backfill1 = _rusage_self_cpu_s()
    backfill_cpu_self_s = (
        (_cpu_self_backfill1 - _cpu_self_backfill0)
        if _cpu_self_backfill0 is not None else None)
    print(f"  silver.users       : {t_su.dt:8.2f}s  (rows={su_rows:,})")
    print(f"  silver.events      : {t_se.dt:8.2f}s  (rows={se_rows:,})")
    print(f"  gold.country_rollup: {t_g.dt:8.2f}s  (delta rows joined={gold_rows:,})")
    print(f"  --- DAG total      : {backfill_total:8.2f}s")
    if backfill_cpu_self_s is not None:
        print(f"  --- driver rusage  : cpu_self={backfill_cpu_self_s:.2f}s "
              f"(Python driver ONLY; excludes JVM under local[*])\n")

    se.unpersist()
    # keep the running users dimension growing across cycles (as leat appends user deltas)
    # We rebuild the users dimension each cycle by unioning the accumulated user parquet.
    users_accum = [bronze_users]

    # =====================================================================
    # REGIME 2: INCREMENTAL STEADY-STATE
    # =====================================================================
    print(f"[regime 2] incremental steady-state ({cfg['k']} cycles)")
    cycle_times = []
    cycle_rows = []
    cycle_cpu_self_s = []   # Python-driver-only CPU per cycle (excludes JVM)
    for c in range(cfg["k"]):
        _cpu_self_c0 = _rusage_self_cpu_s()
        with Timer() as tc:
            # append this cycle's user delta to the dimension (users-as-of-cycle-c),
            # mirroring leat: bronze.users.append(ud) BEFORE processing event delta c.
            ud = spark.read.parquet(os.path.join(DATA, f"delta_users_c{c}.parquet"))
            users_accum.append(ud)
            users_all = users_accum[0]
            for extra in users_accum[1:]:
                users_all = users_all.unionByName(extra)
            dim_c = dedupe_users(users_all)

            # read + filter this cycle's event delta (silver.events on the delta only)
            ed = spark.read.parquet(os.path.join(DATA, f"delta_events_c{c}.parquet"))
            se_c = silver_events(ed)

            # join delta x users-as-of-cycle-c, aggregate the delta, merge running totals
            partial_c = gold_partial(se_c, dim_c)
            partial_c.cache()
            g_rows = se_c.count()          # surviving delta events (fed to gold)
            running = gold_merge(running, partial_c)
            partial_c.unpersist()
        cycle_times.append(tc.dt)
        cycle_rows.append(g_rows)
        _cpu_self_c1 = _rusage_self_cpu_s()
        if _cpu_self_c0 is not None:
            cycle_cpu_self_s.append(_cpu_self_c1 - _cpu_self_c0)
        print(f"  cycle {c:2d}: {tc.dt*1e3:7.0f} ms  gold delta rows={g_rows:,}")

    def pstats(xs):
        xs = sorted(xs)
        return {
            "mean_ms": statistics.mean(xs) * 1e3,
            "p50_ms": statistics.median(xs) * 1e3,
            "p95_ms": xs[min(len(xs) - 1, int(round(0.95 * (len(xs) - 1))))] * 1e3,
            "min_ms": xs[0] * 1e3,
            "max_ms": xs[-1] * 1e3,
        }
    ps = pstats(cycle_times)
    print(f"\n  per-cycle: mean={ps['mean_ms']:.0f}ms p50={ps['p50_ms']:.0f}ms "
          f"p95={ps['p95_ms']:.0f}ms min={ps['min_ms']:.0f}ms max={ps['max_ms']:.0f}ms")
    print(f"  gold rows/cycle: {cycle_rows}\n")

    # =====================================================================
    # Final gold + fingerprint + parquet
    # =====================================================================
    # whole-run Python-driver rusage (CPU self excludes JVM; maxrss is driver peak)
    total_cpu_self_s = _rusage_self_cpu_s()
    driver_maxrss_mb = _rusage_self_maxrss_mb()
    cyc_cpu_self_mean = (statistics.mean(cycle_cpu_self_s)
                         if cycle_cpu_self_s else None)

    fp = gold_fingerprint(running)
    print(f"[gold] countries={fp['num_countries']} "
          f"grand_total_value={fp['grand_total_value']:,} "
          f"grand_row_count={fp['grand_row_count']:,}")
    print(f"[gold] fingerprint sha256={fp['sha256']}")

    expected_rows = se_rows + sum(cycle_rows)
    exactly_once_ok = fp["grand_row_count"] == expected_rows
    print(f"[check] grand_row_count={fp['grand_row_count']:,} "
          f"expected={expected_rows:,} -> exactly_once="
          f"{'OK' if exactly_once_ok else 'FAIL'}\n")

    # write gold_spark.parquet (single file, schema country,total_value,row_count sorted)
    gold_rows_sorted = fp["rows"]
    gold_df = spark.createDataFrame(
        [(int(c), int(v), int(n)) for c, v, n in gold_rows_sorted],
        schema="country long, total_value long, row_count long",
    ).orderBy("country")
    gold_df.coalesce(1).write.mode("overwrite").parquet(GOLD_SPARK_DIR)
    # move the single part-file to gold_spark.parquet
    import glob, shutil
    part = glob.glob(os.path.join(GOLD_SPARK_DIR, "part-*.parquet"))[0]
    shutil.copyfile(part, GOLD_SPARK_PARQUET)
    print(f"[written] {GOLD_SPARK_PARQUET}")

    # ---- results_spark.json ----
    results = {
        "engine": "spark",
        "spark_version": spark.version,
        "spark_master": _master,
        "storage": "neutral_parquet_direct",
        "gold_strategy": "incremental_delta_merge_running_totals_overwrite",
        "config": cfg,
        "cold_start": {
            "session_init_ms": cold_session_s * 1e3,
            "launch_to_first_action_ms": cold_total_s * 1e3,
        },
        "full_backfill": {
            "silver_users_s": t_su.dt,
            "silver_events_s": t_se.dt,
            "gold_s": t_g.dt,
            "dag_total_s": backfill_total,
            "silver_events_rows": se_rows,
            "silver_users_rows": su_rows,
            "gold_delta_rows": gold_rows,
            # Python-driver-only CPU (RUSAGE_SELF); EXCLUDES the JVM compute under
            # local[*]. Authoritative whole-process CPU+RSS come from the launcher
            # (RUSAGE_CHILDREN over spark-submit); see resource_whole_run below.
            "cpu_self_s": backfill_cpu_self_s,
        },
        "incremental": {
            "cycles": cfg["k"],
            "per_cycle_ms": [t * 1e3 for t in cycle_times],
            "gold_rows_per_cycle": cycle_rows,
            **ps,
            "total_delta_events": cfg["k"] * cfg["delta_events"],
            "total_delta_users": cfg["k"] * cfg["delta_users"],
            "per_cycle_cpu_self_s": cycle_cpu_self_s,
            "cpu_self_s_mean": cyc_cpu_self_mean,
        },
        # Python DRIVER rusage for the whole run (self = driver only, no JVM).
        # This is expected to be far below the launcher's whole-process totals;
        # that gap IS the proof that the JVM CPU is not seen by RUSAGE_SELF.
        "resource_driver_self": {
            "note": ("RUSAGE_SELF on the Python driver; excludes the embedded JVM "
                     "under local[*]. Whole-process (JVM-inclusive) CPU-seconds + "
                     "peak RSS are in resource_whole_run, captured by the launcher."),
            "total_cpu_self_s": total_cpu_self_s,
            "driver_peak_rss_mb": driver_maxrss_mb,
        },
        "gold_fingerprint": {k: v for k, v in fp.items() if k != "rows"},
        "gold_table": [[c, v, n] for c, v, n in fp["rows"]],
        "exactly_once_ok": exactly_once_ok,
    }
    with open(RESULTS_SPARK, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[written] {RESULTS_SPARK}")

    spark.stop()


if __name__ == "__main__":
    main()
