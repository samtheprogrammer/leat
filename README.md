# leat

[![tests](https://github.com/samtheprogrammer/leat/actions/workflows/ci.yml/badge.svg)](https://github.com/samtheprogrammer/leat/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

**Lightweight, engine-neutral incremental ETL over Iceberg & Delta.**
Kafka-style offsets. Runs in any DAG. No broker, no cluster.

`leat` gives you Kafka's consumer ergonomics — offsets, `earliest`/`latest`,
`seek`, exactly-once — but over **lakehouse tables** instead of a message broker.
The table *is* the log; a snapshot/version is the offset. So you get incremental
bronze → silver → gold on a single node, for a fraction of a Spark cluster's cost.

**Easy like Polars.** No schema wiring, no offset column — `leat` mints the
Kafka-style offset for you (like Kafka assigning offsets on produce). Your
transforms see and return only their business columns.

```python
import leat, polars as pl

lt = leat.connect("/data/leat")
lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],   # no schema, no _offset
                                          "value":   [50, 150, 250]}))

@lt.model(source="db.events", sink="db.silver", start="earliest")
def silver(df):                                    # df has NO _offset — just your columns
    return df.filter(pl.col("value") > 100)

silver.run(once=True)                              # incremental, exactly-once, done
print(lt.table("db.silver").read())                # Polars DataFrame, no _offset column
```

**Delta is the same one-liner** — pass `format="delta"` and the identical
`table()/@lt.model/read()` easy path runs on Delta Lake (delta-rs, no catalog);
an identifier like `"db.events"` maps to the path `<warehouse>/db/events`:

```python
lt = leat.connect("/data/leat", format="delta")    # Delta easy path (was: session() only)
```

Offsets are **invisible in your data but real and controllable** at the consumer
level — `start="earliest"|"latest"|<int>`, `position()`, `lag()`, `seek()`,
`commit()` are all first-class. `leat` assigns them for you; you still steer them.

```python
from leat import Consumer, JsonCheckpointStore
c = Consumer(lt.source("db.events"), name="silver", checkpoint=ckpt, start="earliest")
while (batch := c.poll()) is not None:               # changes since committed offset
    clean = batch.pl().filter(pl.col("value") > 100) # low-level path still sees _offset
    silver.append(clean.to_arrow())
    c.commit()                                       # advance + persist offset
```

## Why it exists

For the ~90% of medallion ETL that is small incremental batches (clean, cast,
dedupe, filter, aggregate), a distributed engine is overkill — you pay for a
cluster to do work that fits on one core. **Same result, ~2× faster at equal CPU —
AND ~4.6–9.4× less CPU.** Spark still wins the ~10% (huge batches, big shuffles,
ML) — use it there.

Measured on a realistic medallion DAG (2 bronze → 2 silver → 1 gold join+agg) with
**both** engines Docker-pinned to `--cpus=4`, matched thread caps, and `--memory=12g`
— a true apples-to-apples budget. Gold is **byte-identical** (same sha256) on both:

| size | leat backfill DAG | Spark backfill DAG | leat per-cycle | Spark per-cycle | leat CPU-s | Spark CPU-s | CPU ratio | parity |
|---|---|---|---|---|---|---|---|---|
| 5M  | **0.89 s** | 3.60 s (4.0×) | **372 ms** | 929 ms (2.5×) | ~3.8 s | 36.1 s | **~9.4×** | ✅ byte-identical |
| 20M | **3.01 s** | 5.95 s (2.0×) | **596 ms** | 990 ms (1.7×) | ~10.3 s | 47.7 s | **~4.6×** | ✅ byte-identical |

At equal CPU **leat wins wall-clock at every measured size** — the margin narrows as
data grows (4.0×→2.0× backfill, 2.5×→1.7× per-cycle) toward the ~35M-row crossover.
But the durable win is **cost**: leat produces the identical gold using **~4.6–9.4×
fewer CPU-seconds**, plus scale-to-zero (a triggered task vs an always-on cluster / a
~3 s JVM cold start every trigger) — structurally ~15–100× cheaper. Single-instance
leat used only **~1.5–1.8 of 4** cores while Spark burned **~2.5–2.8**: Spark only
wins big-job wall-clock by **burning more CPU, not by being more efficient**. The
~35M crossover is an artifact of single-instance leat idling cores — running N
instances (now unblocked by proven parallel commits) closes it for partitionable
work. Spark's real, durable edge is **shuffles** (giant joins that don't fit memory,
global sorts, cross-partition high-cardinality group-bys) and the single-node memory
ceiling — see [positioning](docs/positioning.md).

## Design

- **Control layer** — the table format (offsets, incremental read, commit,
  checkpoint). Pluggable: **Iceberg** (PyIceberg) and **Delta** (delta-rs), both
  first-class — same `Consumer`/`@lt.model` code, either format.
- **Compute layer** — **Polars** (silver transforms) and **DuckDB** (gold joins /
  windows / aggregations). Both Arrow-native, so handoffs are zero-copy.
- `leat` is thin orchestration gluing Arrow-native engines. The heavy compute is
  already Rust/C++ (Polars, DuckDB); your code stays pure Python.

## Where things live (catalog vs offsets vs data)

Three separate stores, easy to confuse. The **catalog** stores almost nothing — just
a pointer to each table's *current version*. Your offsets and your rows live
elsewhere:

```
CATALOG        db.silver → metadata/00048.json      "which table version is current"
  │            (external registry: Glue / Unity / REST / SQLite — leat speaks any of them)
  ▼
METADATA FILE  snapshot summary: leat.offset.silver = 4184685   ←  your OFFSETS ride here
  │            + the list of data files in this snapshot         (in object storage, not the catalog)
  ▼
DATA FILES     parquet                                           ←  your ROWS
```

Think of the catalog as a **library index card** ("Book X, latest edition → shelf 12").
It doesn't hold the book or your margin notes — it just says which shelf is current.

| store | what it holds | examples |
|---|---|---|
| **Catalog** | which table *version* is current (a pointer) | Glue, Unity, REST, Hive, SQLite |
| **Checkpoint store** | leat's *offsets* | the sink's own commit (default `checkpoint="sink"`), or a JSON side-file |
| **Coordination** (`ClaimStore`) | which *worker* owns which bucket (elastic scale-out) | etcd, SQLite |

**Exactly-once falls out of this.** A commit is one atomic step: write the new parquet,
stamp the offset into the new metadata's snapshot summary, then swap the catalog
pointer. The offset becomes "current" *only* when the data does — they ride the same
commit. So with `checkpoint="sink"` there is no separate offset store to fall out of
sync: the offset is *in* the table version the catalog points at.

## Kafka → leat

| Kafka | leat |
|---|---|
| topic | a table (Iceberg/Delta) |
| offset | monotonic offset column / snapshot sequence |
| consumer group | a named `Consumer` with its own checkpoint |
| `auto.offset.reset` | `start="earliest" \| "latest" \| <offset>` |
| `poll()` / commit | `poll()` / `commit()` |
| `seek()` | `seek()` / `reset()`, plus **time-travel replay** |
| retention | table time-travel window |

## Scale & deploy like a stateless service (not a stateful monolith)

This is where `leat` diverges hardest from Spark Structured Streaming. A Spark
streaming job is a **stateful monolith**: its state lives in a checkpoint dir owned
by one running application. Two consequences everyone who's operated it knows —
**scaling is a job resize** (reconfigure executors, restart) and there's **no clean
blue-green deploy** (a logic change breaks checkpoint compatibility, so upgrades are
stop-the-world).

`leat` gets the opposite, and it falls out of two design choices, not a feature bolt-on:
the offset lives in the **sink's own commit** (so a running worker holds *no* durable
state — it's disposable), and workers coordinate only through a **shared `ClaimStore`**
(so they're cattle, not pets).

**Scale = replica count.** Start more processes — `kubectl scale --replicas=8`, or just
run the script more times. Anonymous workers claim partitions/offset-ranges from the
shared store; a new worker joins by claiming free work, a dead worker's lease expires
and a survivor reclaims its partition, resuming from the sink offset — **exactly-once,
no restart, no rebalance event.** This is demonstrated end-to-end (scale up + kill a
worker mid-flight + exactly-once) in [`examples/elastic_demo.py`](examples/elastic_demo.py):

```python
from leat import run_worker, open_claim_store
store = open_claim_store("etcd://coord:2379")     # or "local" for one box
run_worker(source, sink, transform, name="silver", num_buckets=16, claim_store=store)
# start this N times → they self-distribute; start/kill any → the pool rebalances
```

**Blue-green deploys** are a deployment concern, not a state migration — because compute
is stateless. Two patterns the design supports directly: point *green* at a **separate
sink**, [parity-check](bench/parity_check.py) it against blue, then flip which sink
downstream reads; or roll green in while blue drains (the `ClaimStore` guarantees one
owner per partition, so they never collide).

**Bounded backfill** for the initial load is the same machinery, run to a fixed
high-water mark: `Backfill(source, sink, transform, num_shards=8).run("all")` — split
the catch-up N ways, then scale down to steady state. Failover comes free from the
`ClaimStore` (`local` SQLite on one box; **etcd** across machines, its lease/TTL *being*
the failover primitive) — or run the shards as Airflow/Dagster/K8s tasks and let the
orchestrator retry.

*(Scaling and failover are demonstrated and tested; the separate-sink blue-green cutover
is a straightforward consequence of stateless compute — see the [design journal §11](docs/design-journal.md).)*

## CLI

Operate pipelines without a runner script:

```bash
leat run pipeline.py --once      # single batch (a DAG task)
leat run pipeline.py             # continuous loop
leat status pipeline.py          # per-model position + lag
leat reset pipeline.py --model silver --to earliest
```

## Honest scope

**Micro-batch, not per-record** — because the table format commits in **batches**
(that floor applies to everyone writing Iceberg/Delta, even Flink). Freshness is
seconds-to-minutes, cheaply. Need milliseconds? Put Kafka in front for the hot
path and let `leat` do the cost-efficient table transform behind it.

**Append + inserts/updates today** (deletes on the roadmap). leat's incremental
cursor is a monotonic offset, so it captures *new rows* and *updates expressed as
appends* (a new higher-offset row per change). Two mutation-awareness features
build on that:

- **`mode="upsert"` handles inserts + updates** — `@lt.model(source, sink,
  mode="upsert", key=["id"])` reads incrementally (cheap, same cost as append)
  and **MERGES by business key** into the sink (update matching rows, insert new)
  instead of appending. The sink holds *current state*, and — because merge-by-key
  is **idempotent** — reprocessing the same batch is a no-op (no dupes), so
  exactly-once is more forgiving than append. Works on Iceberg (`Table.upsert`)
  and Delta (`MERGE`) with identical final state.
- **`on_change` surfaces unseen mutations** — append mode's offset cursor can't
  see *in-place* `UPDATE`/`DELETE`/overwrite on the source. The consumer now
  remembers the source's commit marker and, when a **row-changing** commit appears
  that the cursor would miss, it warns (default; `on_change="warn"`), raises
  (`"error"`, Spark-strict), or stays silent (`"ignore"`). Benign compaction
  (Iceberg `replace` / Delta `OPTIMIZE`) is correctly **not** flagged.

Still on the roadmap: capturing *deletes* / in-place mutation that never appends —
proven for **Delta via Change Data Feed** (`load_cdf` exposes insert/update/delete
rows); Iceberg needs a manifest-diff spike / a newer PyIceberg with incremental
scans. See [updates & CDC](docs/updates-and-cdc.md). So: safe for append-only,
CDC-log-shaped, or **insert/update (upsert)** sources today; delete capture is
roadmap, not silent.

## Docs

- [Why leat exists](docs/why.md) · [Positioning — not replacing Spark](docs/positioning.md) · [How it saves money](docs/cost.md) · [Use cases & boundaries](docs/use-cases.md) · [Updates & CDC](docs/updates-and-cdc.md) · [Architecture](docs/architecture.md)

## Status

Early but real. Working, with tests (full suite: 100 passed):

- **Iceberg** and **Delta** (delta-rs) source/sink — same `Consumer`/`@lt.model` code,
  either format, both **first-class** (incl. `connect(format="delta")`; full parity:
  incremental, atomic exactly-once, elastic worker, upsert — all work on Delta).
- Kafka-style `Consumer` (offsets, earliest/latest, seek, exactly-once).
- Incremental silver (Polars) + stateful gold joins/aggregation (DuckDB).
- **REST catalog neutrality proven live** (iceberg-rest + MinIO): same code, identical
  results; `connect(catalog="rest", ...)` is first-class.
- **Parallel multi-writer commits proven** on a real catalog (concurrent writers,
  Iceberg optimistic-concurrency resolves conflicts, exactly-once) — removes the prior
  SQLite commit-serialization limit for distributed use.
- **Backfill mode** (static sharding) with a pluggable `ClaimStore` (local / etcd-protobuf).
- **etcd-backed distributed failover proven live** (lease TTL = failover primitive;
  death→reclaim ~2.2–2.5 s, exactly-once across handoff).
- **Elastic workers** (`run_worker`): anonymous partition/offset-bucket claiming, scale
  by process count, failover from sink offset.
- **Mutation modes**: `mode="append"` (default) + `mode="upsert"` (merge-by-`key`,
  idempotent — handles inserts *and* updates); `on_change` surfaces source
  updates/deletes append mode can't see (`warn`/`error`/`ignore`, Spark-parity).
- **CLI** (`leat run / status / reset`).

Known caveats: the `etcd3` client needs `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`.

Roadmap: CDC deletes (equality deletes / Change Data Feed), high-cardinality gold
upsert, multi-instance leat vs Spark at scale (now unblocked), PyPI release,
benchmark blog post.

Apache-2.0.
