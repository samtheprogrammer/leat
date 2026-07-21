"""Materialize the SHARED benchmark dataset to neutral parquet.

This imports bench/gen.py (the SAME deterministic seeded generator leat uses) and
writes the exact same bronze tables + per-cycle deltas to parquet under bench/data/.
Spark then READS these parquet files as its source, so inputs are byte-identical to
what leat consumed -> identical gold is the whole point.

Config (row counts / K / seed / delta sizes) is taken from results_leat.json so it
matches whatever preset leat actually ran (currently the "small" preset:
5M events / 200k users / K=5 / 500k event-deltas / 10k user-deltas).

Files written to bench/data/:
  bronze_events_full.parquet   (offsets [0, events))
  bronze_users_full.parquet    (offsets [0, users))
  delta_events_c{N}.parquet    (per-cycle new events, global monotonic offsets)
  delta_users_c{N}.parquet     (per-cycle user UPDATES, global monotonic offsets)
"""
from __future__ import annotations
import json
import os

import pyarrow.parquet as pq

import gen as G   # bench/gen.py

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
RESULTS_LEAT = os.path.join(HERE, "results_leat.json")


def load_cfg() -> dict:
    with open(RESULTS_LEAT) as f:
        return json.load(f)["config"]


def main():
    cfg = load_cfg()
    os.makedirs(DATA, exist_ok=True)
    print(f"[materialize] config = {cfg}")
    print(f"[materialize] seed   = {G.SEED}")
    print(f"[materialize] out    = {DATA}")

    # --- full-history bronze (the backfill) ---
    # MIRROR leat_medallion.run() exactly:
    #   ev = G.gen_events(events, offset_start=0, n_users=users, stream=1)
    #   us = G.gen_users(users,  offset_start=0, n_countries=countries, stream=2)
    ev = G.gen_events(cfg["events"], offset_start=0,
                      n_users=cfg["users"], stream=1)
    us = G.gen_users(cfg["users"], offset_start=0,
                     n_countries=cfg["countries"], stream=2)
    pq.write_table(ev, os.path.join(DATA, "bronze_events_full.parquet"))
    pq.write_table(us, os.path.join(DATA, "bronze_users_full.parquet"))
    print(f"[materialize] bronze_events_full : {ev.num_rows:,} rows")
    print(f"[materialize] bronze_users_full  : {us.num_rows:,} rows")

    # --- per-cycle deltas (global monotonic offsets, mirroring the leat loop) ---
    ev_offset = cfg["events"]
    us_offset = cfg["users"]
    for c in range(cfg["k"]):
        ed = G.gen_event_delta(c, cfg["delta_events"], ev_offset,
                               n_users=cfg["users"])
        ud = G.gen_user_delta(c, cfg["delta_users"], us_offset,
                              n_users=cfg["users"], n_countries=cfg["countries"])
        pq.write_table(ed, os.path.join(DATA, f"delta_events_c{c}.parquet"))
        pq.write_table(ud, os.path.join(DATA, f"delta_users_c{c}.parquet"))
        ev_offset += cfg["delta_events"]
        us_offset += cfg["delta_users"]
        print(f"[materialize] cycle {c}: events={ed.num_rows:,} "
              f"users={ud.num_rows:,} "
              f"(ev_off->{ev_offset:,} us_off->{us_offset:,})")

    print("[materialize] done.")


if __name__ == "__main__":
    main()
