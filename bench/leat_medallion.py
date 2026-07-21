"""leat medallion benchmark — full-backfill + incremental steady-state.

DAG (real medallion bronze -> silver -> gold):

    bronze.events ─┐                       (fact,  big)
                   ├─► silver.events  (Polars: value>0 AND value<CAP)  ─┐
    bronze.users ──┘                                                    │
                   └─► silver.users   (Polars: dedupe on user_id,       │
                                       latest _offset wins)             │
                                                                        ▼
                    gold.country_rollup  ◄── sql() DuckDB join+agg
                    (per country: sum(value), count(*))

Two regimes are measured:
  1. FULL BACKFILL  — whole history loaded to bronze, DAG run once.
  2. INCREMENTAL    — K cycles; each appends a small delta and runs the DAG on
                      ONLY the new offsets (Consumer + atomic sink checkpoint).

Gold strategy = INCREMENTAL-DELTA MERGE (running totals):
  each cycle joins the silver.events DELTA against the *current full*
  silver.users dimension, aggregates the delta per country, and MERGES
  (adds) those partial (sum,count) into the running gold totals. Full backfill
  is just the first, largest "delta" (all silver.events) merged into an empty
  gold. Because the merge is additive and driven off the sink-committed offset,
  it is exactly-once: no country is double-counted across cycles.

Run:
    python bench/leat_medallion.py                 # default (big) sizes
    python bench/leat_medallion.py --preset small  # ~5M/200k, K=5 (validation)
    python bench/leat_medallion.py --events 20000000 --users 500000 --k 10 ...
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import tempfile
import shutil
import statistics
import threading
import time

import numpy as np
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

import leat
from leat import sql

import gen as G   # bench/gen.py (run from bench/ or with bench on sys.path)

# ---------------------------------------------------------------------------
# Paths — the WAREHOUSE (Iceberg tables) goes to a portable, no-space temp dir
# (the repo may live under a path with a space, which PyIceberg dislikes).
# Override with LEAT_BENCH_DIR. Result artifacts stay in the repo bench/ dir.
# ---------------------------------------------------------------------------
WAREHOUSE = (os.environ.get("LEAT_BENCH_DIR")
             or os.path.join(tempfile.gettempdir(), "leat_bench_wh")).replace(os.sep, "/")
HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS_JSON = os.path.join(HERE, "results_leat.json")
GOLD_PARQUET = os.path.join(HERE, "gold_leat.parquet")

GOLD_SCHEMA = pa.schema([
    ("country", pa.int64()), ("total_value", pa.int64()), ("row_count", pa.int64()),
])


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------
class Timer:
    def __enter__(self):
        self.t0 = time.perf_counter(); return self
    def __exit__(self, *a):
        self.dt = time.perf_counter() - self.t0


# ---------------------------------------------------------------------------
# Resource measurement (CPU-seconds + peak RSS)
# ---------------------------------------------------------------------------
# NOTE: Polars & DuckDB run their heavy compute on WORKER THREADS inside this
# same process (verified: p.children() is empty during a run). So
# psutil.Process().cpu_times() (user+system) on the main process already
# includes all worker-thread CPU. We still add p.children(recursive=True)
# defensively in case any future path spawns a child process (it will be 0 here).
_PROC = psutil.Process()


def _cpu_seconds() -> float:
    """Total CPU-seconds (user+system) consumed by this process AND any child
    processes so far. Thread CPU is included in the main process automatically."""
    ct = _PROC.cpu_times()
    total = ct.user + ct.system
    try:
        for ch in _PROC.children(recursive=True):
            try:
                cct = ch.cpu_times()
                total += cct.user + cct.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return total


def _rss_bytes() -> int:
    """Current resident set size of this process + any children, in bytes."""
    total = _PROC.memory_info().rss
    try:
        for ch in _PROC.children(recursive=True):
            try:
                total += ch.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return total


class ResourceMeter:
    """Context manager: captures wall-clock, CPU-seconds (user+system, incl.
    children), and peak RSS during a timed section. Peak RSS is sampled by a
    lightweight background thread polling every ~30ms."""

    def __init__(self, sample_interval: float = 0.03):
        self.sample_interval = sample_interval

    def __enter__(self):
        self._stop = threading.Event()
        self.peak_rss = _rss_bytes()          # seed with current RSS
        self._cpu0 = _cpu_seconds()
        self.t0 = time.perf_counter()
        self._sampler = threading.Thread(target=self._sample, daemon=True)
        self._sampler.start()
        return self

    def _sample(self):
        while not self._stop.is_set():
            try:
                rss = _rss_bytes()
                if rss > self.peak_rss:
                    self.peak_rss = rss
            except Exception:
                pass
            self._stop.wait(self.sample_interval)

    def __exit__(self, *a):
        self.dt = time.perf_counter() - self.t0
        self.cpu_s = _cpu_seconds() - self._cpu0
        self._stop.set()
        self._sampler.join(timeout=1.0)
        # final RSS check after the section completes
        rss = _rss_bytes()
        if rss > self.peak_rss:
            self.peak_rss = rss

    @property
    def peak_rss_mb(self) -> float:
        return self.peak_rss / (1024 * 1024)

    @property
    def eff_cores(self) -> float:
        return (self.cpu_s / self.dt) if self.dt > 0 else 0.0


# ---------------------------------------------------------------------------
# Gold: join silver.events delta x silver.users (full, deduped) -> per-country
# partial aggregate, then MERGE into running totals.
# ---------------------------------------------------------------------------
def dedupe_users(users_all: pa.Table) -> pa.Table:
    """silver.users semantics resolved globally: keep latest _offset per user_id.

    silver.users is an append-only stream of cleaned user rows (backfill + updates).
    The 'dedupe on user_id keeping latest _offset' is resolved here so the gold
    join sees exactly one country per user (the newest known)."""
    df = pl.from_arrow(users_all)
    return (df.sort("_offset")
              .group_by("user_id", maintain_order=False)
              .last()
              .select(["user_id", "country"])
              .to_arrow())


def gold_partial(events_delta: pa.Table, users_dim: pa.Table) -> pa.Table:
    """Join the events delta to the user dimension and aggregate the DELTA per
    country. This is the DuckDB-over-Arrow compute we want to showcase."""
    return sql(
        """
        SELECT d.country              AS country,
               SUM(e.value)::BIGINT   AS total_value,
               COUNT(*)::BIGINT       AS row_count
        FROM events e
        JOIN dim d ON e.user_id = d.user_id
        GROUP BY d.country
        ORDER BY d.country
        """,
        events=events_delta, dim=users_dim,
    ).cast(GOLD_SCHEMA)


def gold_merge(running: pa.Table, partial: pa.Table) -> pa.Table:
    """Additive merge of a per-country partial into the running gold totals.
    running + partial, grouped by country -> new running totals (overwrite gold)."""
    if running.num_rows == 0:
        return partial.sort_by("country")
    combined = pa.concat_tables([running, partial])
    return sql(
        """
        SELECT country,
               SUM(total_value)::BIGINT AS total_value,
               SUM(row_count)::BIGINT   AS row_count
        FROM g GROUP BY country ORDER BY country
        """,
        g=combined,
    ).cast(GOLD_SCHEMA)


def gold_fingerprint(gold: pa.Table) -> dict:
    """Deterministic summary of the final gold table for cross-engine parity.
    Sorted (country,total_value,row_count) tuples -> sha256, plus scalar totals."""
    g = gold.sort_by("country")
    rows = list(zip(g.column("country").to_pylist(),
                    g.column("total_value").to_pylist(),
                    g.column("row_count").to_pylist()))
    blob = "\n".join(f"{c},{v},{n}" for c, v, n in rows).encode()
    return {
        "sha256": hashlib.sha256(blob).hexdigest(),
        "num_countries": g.num_rows,
        "grand_total_value": int(sum(r[1] for r in rows)),
        "grand_row_count": int(sum(r[2] for r in rows)),
        "rows": rows,   # small table (<= N_COUNTRIES rows), embed it fully
    }


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
def run(cfg):
    print(f"\n=== leat medallion benchmark ===")
    print(f"warehouse : {WAREHOUSE}")
    print(f"config    : events={cfg['events']:,} users={cfg['users']:,} "
          f"countries={cfg['countries']} K={cfg['k']} "
          f"delta_events={cfg['delta_events']:,} delta_users={cfg['delta_users']:,}")
    print(f"gold      : incremental-delta merge (running totals, overwrite gold)\n")

    # fresh warehouse
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    lt = leat.connect(WAREHOUSE, checkpoint="sink")

    # --- create the 5 tables ---
    lt.create("bronze.events", G.EVENTS_SCHEMA)
    lt.create("bronze.users", G.USERS_SCHEMA)
    lt.create("silver.events", G.EVENTS_SCHEMA)
    lt.create("silver.users", G.USERS_SCHEMA)
    lt.create("gold.country_rollup", GOLD_SCHEMA)

    silver_events_tbl = lt.source("silver.events")
    silver_users_tbl = lt.source("silver.users")
    gold_tbl = lt.source("gold.country_rollup")

    # --- silver models (decorator API, pure Polars), start from earliest ---
    CAP = G.VALUE_CAP

    @lt.model(source="bronze.events", sink="silver.events", start="earliest")
    def silver_events(df):
        return df.filter((pl.col("value") > 0) & (pl.col("value") < CAP))

    @lt.model(source="bronze.users", sink="silver.users", start="earliest")
    def silver_users(df):
        # clean pass-through; global dedupe (latest _offset per user) is resolved
        # in the gold join's dimension read (dedupe_users).
        return df

    # --- gold consumer on silver.events (atomic sink checkpoint) ---
    from leat.consumer import Consumer
    from leat.checkpoint import SinkCheckpointStore
    gold_consumer = Consumer(silver_events_tbl, name="gold_country_rollup",
                             checkpoint=SinkCheckpointStore(gold_tbl), start="earliest")

    def gold_step():
        """Consume the silver.events delta, join to full silver.users dim,
        aggregate the delta, merge into running gold. Returns rows processed."""
        batch = gold_consumer.poll()
        if batch is None:
            return 0
        dim = dedupe_users(silver_users_tbl.read_all())
        partial = gold_partial(batch.arrow(), dim)
        running = gold_tbl.read_all().select(["country", "total_value", "row_count"])
        empty = running.num_rows == 0
        # NOTE: gold is overwrite semantics — we replace the whole (small) table.
        merged = gold_merge(running, partial)
        # atomic: write merged gold + advance the gold consumer's offset together.
        _overwrite_gold(gold_tbl, merged, offset=batch.offset, empty=empty)
        gold_consumer.seek(batch.offset)
        return batch.num_rows

    # ======================================================================
    # REGIME 1: FULL BACKFILL
    # ======================================================================
    print("[regime 1] full backfill")
    # load bronze (whole history)
    with Timer() as t_load:
        ev = G.gen_events(cfg["events"], offset_start=0,
                          n_users=cfg["users"], stream=1)
        us = G.gen_users(cfg["users"], offset_start=0,
                         n_countries=cfg["countries"], stream=2)
        lt.source("bronze.events").append(ev)
        lt.source("bronze.users").append(us)
    print(f"  bronze load       : {t_load.dt:8.2f}s  "
          f"(events={ev.num_rows:,} users={us.num_rows:,})")

    # Wrap the WHOLE backfill DAG in a ResourceMeter (CPU-seconds + peak RSS),
    # keeping the existing per-stage wall-clock Timers intact.
    with ResourceMeter() as rm_backfill:
        with Timer() as t_su:
            silver_users.run(once=True)
        with Timer() as t_se:
            silver_events.run(once=True)
        with Timer() as t_g:
            gold_rows = gold_step()

    backfill_total = t_su.dt + t_se.dt + t_g.dt
    se_rows = silver_events_tbl.read_all().num_rows
    su_rows = silver_users_tbl.read_all().num_rows
    print(f"  silver.users      : {t_su.dt:8.2f}s  (rows={su_rows:,})")
    print(f"  silver.events     : {t_se.dt:8.2f}s  (rows={se_rows:,})")
    print(f"  gold.country_rollup:{t_g.dt:8.2f}s  (delta rows joined={gold_rows:,})")
    print(f"  --- DAG total     : {backfill_total:8.2f}s (excludes bronze load)")
    print(f"  --- resources     : cpu={rm_backfill.cpu_s:6.2f}s  "
          f"peak_rss={rm_backfill.peak_rss_mb:7.1f}MB  "
          f"eff_cores={rm_backfill.eff_cores:4.2f}\n")

    # ======================================================================
    # REGIME 2: INCREMENTAL STEADY-STATE
    # ======================================================================
    print(f"[regime 2] incremental steady-state ({cfg['k']} cycles)")
    ev_offset = cfg["events"]     # next event offset (global monotonic)
    us_offset = cfg["users"]      # next user offset (global monotonic)
    cycle_times = []
    cycle_rows = []
    cycle_cpu_s = []
    cycle_peak_rss_mb = []
    for c in range(cfg["k"]):
        with ResourceMeter() as rm_c:
            with Timer() as tc:
                # append delta to bronze
                ed = G.gen_event_delta(c, cfg["delta_events"], ev_offset, n_users=cfg["users"])
                ud = G.gen_user_delta(c, cfg["delta_users"], us_offset,
                                      n_users=cfg["users"], n_countries=cfg["countries"])
                lt.source("bronze.events").append(ed)
                lt.source("bronze.users").append(ud)
                ev_offset += cfg["delta_events"]
                us_offset += cfg["delta_users"]
                # run incremental DAG (only the delta flows through)
                silver_users.run(once=True)
                silver_events.run(once=True)
                g_rows = gold_step()
        cycle_times.append(tc.dt)
        cycle_rows.append(g_rows)
        cycle_cpu_s.append(rm_c.cpu_s)
        cycle_peak_rss_mb.append(rm_c.peak_rss_mb)
        print(f"  cycle {c:2d}: {tc.dt*1e3:7.0f} ms  gold delta rows={g_rows:,}  "
              f"cpu={rm_c.cpu_s:5.2f}s  peak_rss={rm_c.peak_rss_mb:7.1f}MB  "
              f"eff_cores={rm_c.eff_cores:4.2f}")

    # incrementality proof: each cycle's gold rows should ~= surviving delta events
    # (delta_events minus the ~7% filtered by silver), NOT the growing total.
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
    cyc_cpu_mean = statistics.mean(cycle_cpu_s)
    cyc_peak_rss = max(cycle_peak_rss_mb)
    cyc_eff_cores = cyc_cpu_mean / (ps["mean_ms"] / 1e3) if ps["mean_ms"] > 0 else 0.0
    print(f"  per-cycle resources: cpu_mean={cyc_cpu_mean:.2f}s  "
          f"peak_rss(max over cycles)={cyc_peak_rss:.1f}MB  "
          f"eff_cores={cyc_eff_cores:.2f}")
    print(f"  gold rows/cycle: {cycle_rows}  (proves only the delta flows, "
          f"not the growing total)\n")

    # ======================================================================
    # Final gold + fingerprint + parity artifact
    # ======================================================================
    final_gold = gold_tbl.read_all().select(["country", "total_value", "row_count"]) \
        .sort_by("country")
    fp = gold_fingerprint(final_gold)
    pq.write_table(final_gold, GOLD_PARQUET)

    print(f"[gold] countries={fp['num_countries']} "
          f"grand_total_value={fp['grand_total_value']:,} "
          f"grand_row_count={fp['grand_row_count']:,}")
    print(f"[gold] fingerprint sha256={fp['sha256']}")

    # cross-check exactly-once: grand_row_count MUST equal the count of all silver
    # events fed through gold (backfill delta + each cycle delta).
    expected_rows = se_rows + sum(cycle_rows)
    exactly_once_ok = fp["grand_row_count"] == expected_rows
    print(f"[check] grand_row_count={fp['grand_row_count']:,} "
          f"expected(sum of gold deltas)={expected_rows:,} "
          f"-> exactly_once={'OK' if exactly_once_ok else 'FAIL'}\n")

    results = {
        "engine": "leat",
        "leat_version": leat.__version__,
        "gold_strategy": "incremental_delta_merge_running_totals_overwrite",
        "config": cfg,
        "full_backfill": {
            "bronze_load_s": t_load.dt,
            "silver_users_s": t_su.dt,
            "silver_events_s": t_se.dt,
            "gold_s": t_g.dt,
            "dag_total_s": backfill_total,
            "silver_events_rows": se_rows,
            "silver_users_rows": su_rows,
            "gold_delta_rows": gold_rows,
            # resource utilization for the whole backfill DAG (excl. bronze load)
            "cpu_seconds": rm_backfill.cpu_s,
            "peak_rss_mb": rm_backfill.peak_rss_mb,
            "wall_s": rm_backfill.dt,
            "eff_cores": rm_backfill.eff_cores,
        },
        "incremental": {
            "cycles": cfg["k"],
            "per_cycle_ms": [t * 1e3 for t in cycle_times],
            "gold_rows_per_cycle": cycle_rows,
            **ps,
            "total_delta_events": cfg["k"] * cfg["delta_events"],
            "total_delta_users": cfg["k"] * cfg["delta_users"],
            # resource utilization per cycle (CPU-seconds + peak RSS)
            "per_cycle_cpu_s": cycle_cpu_s,
            "per_cycle_peak_rss_mb": cycle_peak_rss_mb,
            "cpu_seconds_mean": cyc_cpu_mean,
            "peak_rss_mb_max": cyc_peak_rss,
            "eff_cores": cyc_eff_cores,
        },
        "gold_fingerprint": {k: v for k, v in fp.items() if k != "rows"},
        "gold_table": fp["rows"],
        "exactly_once_ok": exactly_once_ok,
    }
    with open(RESULTS_JSON, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[written] {RESULTS_JSON}")
    print(f"[written] {GOLD_PARQUET}")
    return results


# ---------------------------------------------------------------------------
def _overwrite_gold(tbl, data: pa.Table, offset: int, empty: bool):
    """Persist the (small) gold table and ride the offset on the same snapshot.
    First write (empty gold) is an append; subsequent writes overwrite all rows.
    snapshot_properties carry the gold consumer's offset atomically with the data
    (sink-checkpoint exactly-once)."""
    props = {"leat.offset.gold_country_rollup": str(offset)}
    it = tbl._table()
    if empty:
        it.append(data, snapshot_properties=props)
    else:
        it.overwrite(data, snapshot_properties=props)


PRESETS = {
    "small": dict(events=5_000_000, users=200_000, countries=200, k=5,
                  delta_events=500_000, delta_users=10_000),
    "full": dict(events=G.N_EVENTS, users=G.N_USERS, countries=G.N_COUNTRIES,
                 k=G.K_CYCLES, delta_events=G.DELTA_EVENTS, delta_users=G.DELTA_USERS),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preset", choices=list(PRESETS), default="full")
    ap.add_argument("--events", type=int)
    ap.add_argument("--users", type=int)
    ap.add_argument("--countries", type=int)
    ap.add_argument("--k", type=int)
    ap.add_argument("--delta-events", type=int, dest="delta_events")
    ap.add_argument("--delta-users", type=int, dest="delta_users")
    a = ap.parse_args()
    cfg = dict(PRESETS[a.preset])
    for key in ("events", "users", "countries", "k", "delta_events", "delta_users"):
        if getattr(a, key) is not None:
            cfg[key] = getattr(a, key)
    run(cfg)


if __name__ == "__main__":
    main()
