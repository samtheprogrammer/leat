"""Parity assertion: leat gold vs Spark gold.

Loads gold_leat.parquet and gold_spark.parquet, asserts they are identical
(same rows, same total_value/row_count per country) and that their sha256
fingerprints (computed the SAME way leat/Spark compute them) match. Also
cross-checks against results_leat.json / results_spark.json. Prints PASS/FAIL loudly.
"""
from __future__ import annotations
import hashlib
import json
import os
import sys

import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
GOLD_LEAT = os.path.join(HERE, "gold_leat.parquet")
GOLD_SPARK = os.path.join(HERE, "gold_spark.parquet")
RESULTS_LEAT = os.path.join(HERE, "results_leat.json")
RESULTS_SPARK = os.path.join(HERE, "results_spark.json")


def fingerprint(tbl) -> str:
    """EXACT replica of leat.gold_fingerprint's sha256 (sorted (country,total_value,
    row_count) -> 'c,v,n' newline-joined -> sha256)."""
    g = tbl.sort_by("country")
    rows = list(zip(g.column("country").to_pylist(),
                    g.column("total_value").to_pylist(),
                    g.column("row_count").to_pylist()))
    blob = "\n".join(f"{c},{v},{n}" for c, v, n in rows).encode()
    return hashlib.sha256(blob).hexdigest(), rows


def main():
    leat_tbl = pq.read_table(GOLD_LEAT)
    spark_tbl = pq.read_table(GOLD_SPARK)

    leat_sha, leat_rows = fingerprint(leat_tbl)
    spark_sha, spark_rows = fingerprint(spark_tbl)

    print("=" * 60)
    print("PARITY CHECK: leat gold  vs  Spark gold")
    print("=" * 60)
    print(f"leat  rows={len(leat_rows):3d}  sha256={leat_sha}")
    print(f"spark rows={len(spark_rows):3d}  sha256={spark_sha}")

    ok = True

    # 1) sha256 match
    sha_ok = leat_sha == spark_sha
    print(f"[sha256 match]        {'PASS' if sha_ok else 'FAIL'}")
    ok &= sha_ok

    # 2) row-by-row exact match
    rows_ok = leat_rows == spark_rows
    print(f"[row-by-row match]    {'PASS' if rows_ok else 'FAIL'}")
    ok &= rows_ok
    if not rows_ok:
        ld = {c: (v, n) for c, v, n in leat_rows}
        sd = {c: (v, n) for c, v, n in spark_rows}
        allc = sorted(set(ld) | set(sd))
        print("  --- diffs (country: leat -> spark) ---")
        for c in allc:
            if ld.get(c) != sd.get(c):
                print(f"    country {c}: leat={ld.get(c)} spark={sd.get(c)}")

    # 3) grand totals
    lt_tv = sum(v for _, v, _ in leat_rows); lt_rc = sum(n for _, _, n in leat_rows)
    sp_tv = sum(v for _, v, _ in spark_rows); sp_rc = sum(n for _, _, n in spark_rows)
    tot_ok = (lt_tv, lt_rc) == (sp_tv, sp_rc)
    print(f"[grand totals]        {'PASS' if tot_ok else 'FAIL'}  "
          f"leat=(tv={lt_tv:,}, rc={lt_rc:,}) spark=(tv={sp_tv:,}, rc={sp_rc:,})")
    ok &= tot_ok

    # 4) cross-check against results json fingerprints
    if os.path.exists(RESULTS_LEAT) and os.path.exists(RESULTS_SPARK):
        with open(RESULTS_LEAT) as f:
            rl = json.load(f)["gold_fingerprint"]["sha256"]
        with open(RESULTS_SPARK) as f:
            rs = json.load(f)["gold_fingerprint"]["sha256"]
        json_ok = rl == rs == leat_sha == spark_sha
        print(f"[results.json sha]    {'PASS' if json_ok else 'FAIL'}  "
              f"leat.json={rl[:16]}... spark.json={rs[:16]}...")
        ok &= json_ok

    print("=" * 60)
    if ok:
        print(">>> PARITY: PASS  (Spark reproduces leat's gold EXACTLY) <<<")
    else:
        print(">>> PARITY: FAIL  <<<")
    print("=" * 60)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
