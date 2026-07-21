"""Deterministic data generator for the leat medallion benchmark.

All randomness comes from ``np.random.default_rng(SEED)`` seeded off a base seed +
a stable stream id, so every table (and every incremental delta) is 100%
reproducible run-to-run. That determinism is what lets a Spark run be asserted to
produce the byte-identical gold fingerprint.

Schemas (bronze):
  events : [_offset i64, user_id i64, value i64, ts i64]   (big fact table)
  users  : [_offset i64, user_id i64, country i64]         (small dimension)

The ``_offset`` column is leat's Kafka-style monotonic offset: it is globally
increasing across the full history AND across every appended delta, so the
incremental consumer sees each new row exactly once.
"""
from __future__ import annotations
import numpy as np
import pyarrow as pa

# ---------------------------------------------------------------------------
# Config (defaults; overridable via argparse in the harness or by kwargs here)
# ---------------------------------------------------------------------------
SEED = 20260720

# Full-backfill sizes
N_EVENTS = 20_000_000        # big/fast-growing fact table
N_USERS = 500_000            # smaller dimension
N_COUNTRIES = 200            # country codes 0..N_COUNTRIES-1

# Per-cycle incremental delta sizes
DELTA_EVENTS = 1_000_000     # new events per cycle
DELTA_USERS = 10_000         # user updates per cycle
K_CYCLES = 10                # number of incremental cycles

# Value distribution / silver cleaning knobs
VALUE_MAX = 1000             # values drawn uniformly [ -50 .. VALUE_MAX+outliers )
VALUE_CAP = 900              # silver.events keeps 0 < value < VALUE_CAP
NEG_FRACTION = 0.05          # ~5% of values are <= 0 (filtered out in silver)

# Arrow schemas ------------------------------------------------------------
EVENTS_SCHEMA = pa.schema([
    ("_offset", pa.int64()), ("user_id", pa.int64()),
    ("value", pa.int64()), ("ts", pa.int64()),
])
USERS_SCHEMA = pa.schema([
    ("_offset", pa.int64()), ("user_id", pa.int64()), ("country", pa.int64()),
])


def _rng(stream: int) -> np.random.Generator:
    """A stable, independent RNG per logical stream id (mixed with the base SEED)."""
    return np.random.default_rng(np.random.SeedSequence([SEED, stream]))


def _gen_values(rng: np.random.Generator, n: int) -> np.ndarray:
    """value column: mostly in [1, VALUE_MAX], with a ~NEG_FRACTION slice <= 0 and a
    small tail of large outliers (>= VALUE_CAP) so silver's outlier filter has work."""
    vals = rng.integers(1, VALUE_MAX + 1, n, dtype=np.int64)
    # inject non-positive values (filtered by value > 0)
    neg_mask = rng.random(n) < NEG_FRACTION
    vals[neg_mask] = rng.integers(-50, 1, neg_mask.sum(), dtype=np.int64)
    # inject a thin tail of outliers >= VALUE_CAP (filtered by value < VALUE_CAP)
    out_mask = rng.random(n) < 0.02
    vals[out_mask] = rng.integers(VALUE_CAP, VALUE_CAP + 500, out_mask.sum(), dtype=np.int64)
    return vals


# ---------------------------------------------------------------------------
# Full-history tables (the backfill)
# ---------------------------------------------------------------------------
def gen_events(n: int = N_EVENTS, offset_start: int = 0,
               n_users: int = N_USERS, stream: int = 1) -> pa.Table:
    """Full events fact table. offsets are [offset_start, offset_start+n)."""
    rng = _rng(stream)
    off = np.arange(offset_start, offset_start + n, dtype=np.int64)
    user_id = rng.integers(0, n_users, n, dtype=np.int64)
    value = _gen_values(rng, n)
    ts = (1_700_000_000 + off).astype(np.int64)      # monotone-ish synthetic epoch
    return pa.table({"_offset": off, "user_id": user_id, "value": value, "ts": ts},
                    schema=EVENTS_SCHEMA)


def gen_users(n: int = N_USERS, offset_start: int = 0,
              n_countries: int = N_COUNTRIES, stream: int = 2,
              user_id_start: int = 0) -> pa.Table:
    """Full users dimension. One row per user_id in [user_id_start, user_id_start+n).
    offsets are [offset_start, offset_start+n)."""
    rng = _rng(stream)
    off = np.arange(offset_start, offset_start + n, dtype=np.int64)
    user_id = np.arange(user_id_start, user_id_start + n, dtype=np.int64)
    country = rng.integers(0, n_countries, n, dtype=np.int64)
    return pa.table({"_offset": off, "user_id": user_id, "country": country},
                    schema=USERS_SCHEMA)


# ---------------------------------------------------------------------------
# Per-cycle incremental deltas
# ---------------------------------------------------------------------------
def gen_event_delta(cycle: int, n: int, offset_start: int,
                    n_users: int = N_USERS) -> pa.Table:
    """A cycle's new events. Deterministic per (cycle) via stream id."""
    rng = _rng(1000 + cycle)
    off = np.arange(offset_start, offset_start + n, dtype=np.int64)
    user_id = rng.integers(0, n_users, n, dtype=np.int64)
    value = _gen_values(rng, n)
    ts = (1_700_000_000 + off).astype(np.int64)
    return pa.table({"_offset": off, "user_id": user_id, "value": value, "ts": ts},
                    schema=EVENTS_SCHEMA)


def gen_user_delta(cycle: int, n: int, offset_start: int,
                   n_users: int = N_USERS, n_countries: int = N_COUNTRIES) -> pa.Table:
    """A cycle's user UPDATES: re-emit `n` existing user_ids with (possibly) new
    country codes and fresh (higher) offsets. silver.users dedupes on user_id
    keeping the latest _offset, so these override the backfilled rows."""
    rng = _rng(2000 + cycle)
    off = np.arange(offset_start, offset_start + n, dtype=np.int64)
    user_id = rng.integers(0, n_users, n, dtype=np.int64)   # updates to existing users
    country = rng.integers(0, n_countries, n, dtype=np.int64)
    return pa.table({"_offset": off, "user_id": user_id, "country": country},
                    schema=USERS_SCHEMA)


if __name__ == "__main__":
    # smoke test
    e = gen_events(1000)
    u = gen_users(100)
    print("events sample:", e.slice(0, 3).to_pylist())
    print("users sample:", u.slice(0, 3).to_pylist())
    print("events rows:", e.num_rows, "users rows:", u.num_rows)
