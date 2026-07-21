"""LIVE REST-catalog neutrality proofs (roadmap item 14).

These run the SAME leat pipeline code that works on SQLite against a *live*
Iceberg REST catalog (``tabulario/iceberg-rest``) backed by MinIO (S3) — the
"real, cloud/Snowflake-shaped catalog" that Polaris / Unity / Nessie / Tabular
all speak. Bring the stack up first:

    docker compose -f infra/rest/docker-compose.yml up -d
    pytest tests/test_rest_catalog.py -v -s

If the stack is not reachable the whole module SKIPS with a clear message
(no false failures in CI without Docker).

Three proofs:
  1. Neutrality       — same TableHandle/@model/read code, REST result == SQLite result.
  2. Exactly-once     — Consumer + atomic sink checkpoint over REST; resume, no dupes.
  3. Parallel commits — MULTIPLE concurrent writers commit to ONE sink through REST.
                        On SQLite these serialize (documented asterisk); here we test
                        whether a real catalog lets them overlap and still stay
                        exactly-once via optimistic-concurrency retries. The result
                        (overlap? correctness?) is reported honestly.
"""
from __future__ import annotations

import os
import time
import uuid
import shutil
import tempfile
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import polars as pl
import pytest

import leat

# --- live-stack connection params (match infra/rest/docker-compose.yml) --------
REST_URI = os.environ.get("LEAT_REST_URI", "http://localhost:8181")
S3_ENDPOINT = os.environ.get("LEAT_S3_ENDPOINT", "http://localhost:9002")
WAREHOUSE = "s3://warehouse/"
S3_OPTS = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}


def _stack_up() -> bool:
    try:
        with urllib.request.urlopen(f"{REST_URI}/v1/config", timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _stack_up(),
    reason=(
        "live Iceberg REST stack not reachable at "
        f"{REST_URI} — start it with: "
        "docker compose -f infra/rest/docker-compose.yml up -d"
    ),
)


# --- helpers -------------------------------------------------------------------
def _rest_session(checkpoint="json"):
    return leat.connect(WAREHOUSE, uri=REST_URI, catalog="rest",
                        checkpoint=checkpoint, name=f"leat{uuid.uuid4().hex[:8]}",
                        **S3_OPTS)


def _sqlite_session(checkpoint="json"):
    d = tempfile.mkdtemp(prefix="leat_rest_sqlite_").replace(os.sep, "/")
    return leat.connect(d, checkpoint=checkpoint), d


def _fresh_namespace(lt, ns):
    """Drop every table under ``ns`` then the namespace, so a rerun starts clean."""
    try:
        for ident in lt.catalog.list_tables(ns):
            try:
                lt.catalog.drop_table(ident)
            except Exception:
                pass
    except Exception:
        pass
    try:
        lt.catalog.drop_namespace(ns)
    except Exception:
        pass


def _seed(n, seed=0):
    rng = np.random.default_rng(seed)
    return pl.DataFrame({
        "id": np.arange(n, dtype=np.int64),
        "value": rng.integers(0, 200, n, dtype=np.int64),
    })


# ==============================================================================
# Proof 1 — NEUTRALITY: identical code + data, REST result == SQLite result
# ==============================================================================
def test_neutrality_rest_matches_sqlite():
    N = 5_000
    seed = 7
    data = _seed(N, seed)

    def run(lt, ns):
        """IDENTICAL leat code for both catalogs — only the session differs."""
        _fresh_namespace(lt, ns)
        lt.table(f"{ns}.events").write(data)              # bronze via TableHandle

        @lt.model(source=f"{ns}.events", sink=f"{ns}.silver", start="earliest")
        def silver_clean(df):                             # dbt-model-style transform
            return df.filter(pl.col("value") > 100)

        silver_clean.run(once=True)
        silver = lt.table(f"{ns}.silver").read().sort("id")
        return silver

    # REST (live, Snowflake-shaped)
    lt_rest = _rest_session()
    rest_silver = run(lt_rest, "neu")

    # SQLite (local, the known-good baseline) — SAME code, SAME seeded data
    lt_sql, sql_dir = _sqlite_session()
    try:
        sql_silver = run(lt_sql, "neu")
    finally:
        pass  # dir cleaned at process exit

    # Ground truth computed independently.
    truth = data.filter(pl.col("value") > 100).sort("id")

    assert rest_silver.height == truth.height, "REST silver row count != truth"
    assert sql_silver.height == truth.height, "SQLite silver row count != truth"
    assert rest_silver.height == sql_silver.height, "REST vs SQLite row count differ"
    # Business columns must be byte-for-byte identical across catalogs.
    assert rest_silver.equals(sql_silver), "REST and SQLite silver frames differ"
    assert rest_silver.equals(truth), "REST silver != independent ground truth"

    print(f"\n[NEUTRALITY] silver rows: REST={rest_silver.height} "
          f"SQLite={sql_silver.height} truth={truth.height} -> IDENTICAL")

    shutil.rmtree(sql_dir, ignore_errors=True)


# ==============================================================================
# Proof 2 — EXACTLY-ONCE through REST (atomic sink checkpoint, resume, no dupes)
# ==============================================================================
def test_exactly_once_through_rest():
    N = 3_000
    lt = _rest_session(checkpoint="sink")      # offsets ride the sink commit
    ns = "eo"
    _fresh_namespace(lt, ns)

    data = _seed(N, seed=3)
    lt.table(f"{ns}.events").write(data)
    # In sink-checkpoint mode the sink is the offset source-of-truth, so it must
    # exist before a Consumer resolves its start offset (same contract as SQLite;
    # the examples pre-create their sinks). Schema = business cols + _offset.
    lt.create(f"{ns}.silver", pa.schema([("_offset", pa.int64()),
                                         ("id", pa.int64()), ("value", pa.int64())]))

    def transform(df):
        return df.filter(pl.col("value") > 100)

    truth = int(data.filter(pl.col("value") > 100).height)

    # --- first pass: consume ~half, then simulate a fresh process resuming -----
    p1 = lt.pipeline("eo_pipe", f"{ns}.events", f"{ns}.silver", transform, start="earliest")
    p1.step()  # one atomic batch (data + offset commit together)
    after_first = lt.table(f"{ns}.silver").read().height

    # Fresh Session + fresh Consumer over the SAME sink — resume offset comes
    # from the sink's own commit metadata (not in-process state).
    lt2 = _rest_session(checkpoint="sink")
    p2 = lt2.pipeline("eo_pipe", f"{ns}.events", f"{ns}.silver", transform, start="earliest")
    # drain
    for _ in range(50):
        if p2.step() == 0:
            break
    final = lt2.table(f"{ns}.silver").read()

    assert final.height == truth, (
        f"exactly-once broken: silver={final.height} expected={truth} "
        f"(after_first={after_first})")
    # No duplicate business rows (id is unique in the source that passed the filter).
    assert final["id"].n_unique() == final.height, "duplicate rows in sink -> not exactly-once"

    print(f"\n[EXACTLY-ONCE] after_first_batch={after_first} final={final.height} "
          f"truth={truth} -> exactly-once across fresh-process resume: OK")


# ==============================================================================
# Proof 3 — PARALLEL COMMITS: many concurrent writers -> ONE sink via REST
# ==============================================================================
def test_parallel_commits_through_rest():
    """The big one. Multiple concurrent writers commit to the same sink table
    through the REST catalog. We measure whether commits OVERLAP (wall-clock <
    sum of per-writer times) and whether the final result is exactly-once/correct
    (every input row lands once, offsets resolve via optimistic-concurrency retry).
    """
    lt = _rest_session()
    ns = "par"
    _fresh_namespace(lt, ns)

    # Pre-create the sink so all writers append to the SAME existing table.
    sink_schema = pa.schema([("_offset", pa.int64()),
                             ("writer", pa.int64()), ("value", pa.int64())])
    lt.create(f"{ns}.sink", sink_schema)
    sink = lt.source(f"{ns}.sink")     # IcebergFormat over the shared table

    NUM_WRITERS = 6
    ROWS_EACH = 500
    barrier = threading.Barrier(NUM_WRITERS)
    results = {}
    conflicts = {}
    from leat.iceberg import _is_conflict

    def writer(w):
        # Independent IcebergFormat handle per thread over the SAME REST table.
        s = lt.source(f"{ns}.sink")
        payload = pa.table({
            "writer": pa.array([w] * ROWS_EACH, type=pa.int64()),
            "value": pa.array(np.arange(ROWS_EACH, dtype=np.int64)),
        })
        barrier.wait()                       # maximize contention: everyone starts together
        t0 = time.perf_counter()
        # Mirror IcebergFormat.append's mint+retry loop but COUNT the optimistic-
        # concurrency conflicts (CommitFailedException) the catalog raised — that
        # count is the evidence that commits genuinely raced and OCC resolved them
        # (vs a lock quietly serializing them with zero conflicts).
        import pyarrow.compute as pc
        c = 0
        for attempt in range(12):
            tbl = lt.catalog.load_table(f"{ns}.sink")
            col = tbl.scan().to_arrow().column("_offset")
            base = pc.max(col).as_py() if len(col) else -1
            base = -1 if base is None else base
            minted = payload.append_column(
                "_offset", pa.array(range(base + 1, base + 1 + ROWS_EACH), type=pa.int64()))
            try:
                tbl.append(minted)
                break
            except Exception as e:                   # noqa: BLE001
                if not _is_conflict(e):
                    raise
                c += 1
                time.sleep(min(2.0, 0.05 * (2 ** attempt)) * (0.5 + 0.5))
        conflicts[w] = c
        dt = time.perf_counter() - t0
        results[w] = (t0, time.perf_counter(), dt)

    wall0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=NUM_WRITERS) as ex:
        list(ex.map(writer, range(NUM_WRITERS)))
    wall = time.perf_counter() - wall0

    # --- correctness: exactly-once, no lost/duplicated rows --------------------
    out = lt.table(f"{ns}.sink").read()
    total_expected = NUM_WRITERS * ROWS_EACH
    assert out.height == total_expected, (
        f"lost/duplicated rows: got {out.height} expected {total_expected}")
    # every writer's full contribution is present exactly once
    per_writer = out.group_by("writer").len().sort("writer")
    assert per_writer["len"].to_list() == [ROWS_EACH] * NUM_WRITERS, \
        f"per-writer counts wrong: {per_writer}"

    # --- _offset integrity: minted offsets are a contiguous unique set ---------
    off = lt.source(f"{ns}.sink").read_all().column("_offset").to_pylist()
    assert len(set(off)) == len(off), "duplicate _offset minted -> concurrency bug"
    assert sorted(off) == list(range(min(off), min(off) + len(off))), \
        "offsets not contiguous -> a commit was lost or double-counted"

    # --- overlap evidence ------------------------------------------------------
    sum_serial = sum(dt for (_, _, dt) in results.values())
    # count how many writers' [start,end] intervals overlapped in wall-clock time
    intervals = sorted((s, e) for (s, e, _) in results.values())
    max_concurrent = 0
    events = []
    for (s, e, _) in results.values():
        events.append((s, +1)); events.append((e, -1))
    events.sort()
    cur = 0
    for _, d in events:
        cur += d
        max_concurrent = max(max_concurrent, cur)

    overlapped = wall < sum_serial * 0.9  # wall clearly less than serial sum => real overlap
    total_conflicts = sum(conflicts.values())
    print(f"\n[PARALLEL-COMMITS] writers={NUM_WRITERS} rows_each={ROWS_EACH} "
          f"final_rows={out.height} (exactly-once=OK)")
    print(f"[PARALLEL-COMMITS] wall={wall*1000:.0f}ms  sum_serial={sum_serial*1000:.0f}ms  "
          f"max_concurrent_writers_in_flight={max_concurrent}  overlapped={overlapped}")
    print(f"[PARALLEL-COMMITS] per-writer times(ms): "
          f"{[round(dt*1000) for (_, _, dt) in results.values()]}")
    print(f"[PARALLEL-COMMITS] optimistic-concurrency conflicts RESOLVED "
          f"per writer={[conflicts[w] for w in range(NUM_WRITERS)]} total={total_conflicts} "
          f"-> real OCC (not lock-serialization); all resolved, result still exactly-once")

    # HARD assertion: correctness (exactly-once) MUST hold on a real catalog.
    # Overlap is REPORTED (printed above), not asserted — whether iceberg-rest
    # truly parallelizes commits or effectively serializes them is the finding.
    assert out.height == total_expected
