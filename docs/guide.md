# leat — developer guide

How to actually use `leat`: install it, write your first incremental pipeline,
pick a catalog/format, use transforms and modes, drop to the low-level API, and
scale across processes or machines.

If you want the *why* (cost, positioning, benchmark), read
[why.md](why.md) and [positioning.md](positioning.md) first. This guide is the
*how*.

`leat` gives you Kafka-style consumer semantics — offsets, `earliest`/`latest`,
`seek`, exactly-once — over lakehouse tables (Iceberg / Delta) instead of a
message broker. No broker, no cluster; it runs as one task inside a DAG.

---

## 1. Install

```bash
pip install leat            # core (Iceberg)
pip install "leat[delta]"   # + Delta Lake
pip install "leat[etcd]"    # + distributed coordination (etcd)
```

| extra | pulls in | when you need it |
|---|---|---|
| `[delta]` | `deltalake>=1.0` | Delta Lake source/sink (`format="delta"`) |
| `[etcd]` | `etcd3`, `protobuf>=4` | multi-machine coordination (`open_claim_store("etcd://...")`) |
| `[dev]` | pytest, deltalake, protobuf tooling, numpy | running the test suite |

**etcd gotcha:** the `etcd3` client ships protobuf stubs that the modern C/upb
protobuf backend refuses to load. Set the pure-Python backend **before** any
protobuf import:

```bash
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
```

(The repo's `conftest.py` sets this for the test suite; in your own program set
it at process start or in the environment.)

---

## 2. Core concepts (60 seconds)

- **A table is an append log.** New rows get higher offsets, exactly like a Kafka
  topic. `leat` owns a monotonic `_offset` column on each table and mints it for
  you on write — you never see it in your data.
- **The offset is a real, leat-managed cursor.** A `Consumer` reads *changes since
  a committed offset*. You steer it Kafka-style: `start="earliest"|"latest"|<int>`,
  `position()`, `lag()`, `seek()`, `commit()`.
- **Exactly-once = the offset is committed inside the sink's own snapshot.** When
  `checkpoint="sink"`, the offset rides the same atomic commit as the data it
  describes — so the offset advances *iff* the data landed. No separate offset
  store to fall out of sync.

Three separate stores (easy to confuse — one sentence each; see the README's
["Where things live"](../README.md#where-things-live-catalog-vs-offsets-vs-data)):

| store | holds | examples |
|---|---|---|
| **Catalog** | which table *version* is current (a pointer) | Glue, Unity, REST, SQLite |
| **Checkpoint store** | leat's *offsets* | the sink's own commit (`checkpoint="sink"`, default), or a JSON side-file (`checkpoint="json"`) |
| **Coordination** (`ClaimStore`) | which *worker* owns which bucket (scale-out) | SQLite, etcd |

---

## 3. Quickstart (Iceberg, local)

```python
import leat, polars as pl

lt = leat.connect("F:/leat")            # zero-config: local SQLite catalog + warehouse

# 1) SOURCE (bronze): write raw rows. No schema, no _offset — leat infers,
#    auto-creates the table, and mints _offset for you.
lt.table("db.events").write(pl.DataFrame({"user_id": [1, 2, 3],
                                          "value":   [50, 150, 250]}))

# 2) a model = SOURCE db.events -> SINK db.silver (auto-created on first output).
#    df is just your business columns — no _offset.
@lt.model(source="db.events", sink="db.silver", start="earliest")
def silver(df):
    return df.filter(pl.col("value") > 100)

silver.run(once=True)                   # one incremental batch, exactly-once
                                        # omit once= (or pass once=False) to loop

# 3) read the SINK back — Polars DataFrame, no _offset column.
print(lt.table("db.silver").read())     # rows with value > 100
```

**source vs sink.** In `@lt.model(source=..., sink=...)`, `source` is the table
you consume incrementally; `sink` is where the transformed rows land. Both are
string identifiers (`"db.events"`) resolved by the session, or format objects if
you built them yourself.

**`start=`** controls where a *brand-new* consumer begins (once an offset is
committed, it always resumes from there):

| `start` | meaning |
|---|---|
| `"latest"` (default) | only new data arriving *after* the pipeline starts |
| `"earliest"` | reprocess the whole table from the beginning |
| `<int>` | begin just after this offset |

**`checkpoint="sink"` (default) vs `"json"`.** `connect()` defaults to
`checkpoint="sink"` — each pipeline's offset lives in its sink table's own commit
metadata, atomic with the data, so it's **true exactly-once even across a crash**
(no side file, nothing to fall out of sync). Opt into `checkpoint="json"` for a
simple side JSON file instead (at-least-once under a crash between the append and
the offset write):

```python
lt = leat.connect("F:/leat")                    # default: atomic sink offsets (exactly-once)
lt = leat.connect("F:/leat", checkpoint="json") # opt into a side JSON offset file
```

> In `"sink"` mode a `Consumer` resolves its start offset from the sink's commit
> metadata, so the **sink must exist** before the first poll (the `@lt.model` easy
> path auto-creates it from the first transform output; if you drive a raw
> `Pipeline`/`Consumer` in sink mode, pre-create the sink with `lt.create(...)`).

`checkpoint` can also be an explicit path (any value other than `"sink"`/`"json"`
is treated as a JSON file path).

---

## 4. Using Iceberg — catalog options

The *same pipeline code* runs against any PyIceberg catalog; only the `connect()`
call changes.

**Local SQLite (default).** Zero setup — `warehouse` is a local directory:

```python
lt = leat.connect("F:/leat")            # SqlCatalog (SQLite), FsspecFileIO (Windows-safe)
```

**Postgres-backed SQL catalog.** Point `uri` at your Postgres:

```python
lt = leat.connect("F:/leat", uri="postgresql://user:pw@host:5432/catalog")
```

**REST catalog** (Snowflake-Polaris / Unity / Tabular / Nessie / iceberg-rest)
backed by S3-compatible storage — a one-liner. `warehouse` becomes the object-store
URI, `uri` the REST endpoint, and any extra `**opts` pass straight through to the
catalog:

```python
lt = leat.connect(
    "s3://warehouse/", uri="http://localhost:8181", catalog="rest",
    **{"s3.endpoint": "http://localhost:9002",
       "s3.access-key-id": "admin",
       "s3.secret-access-key": "password",
       "s3.path-style-access": "true"})
```

(This is the exact shape used in `tests/test_rest_catalog.py`, where identical
leat code produces byte-identical results on REST and SQLite.)

**Glue / Unity / Snowflake-Polaris** work through the same PyIceberg catalog
interface. If you'd rather build the catalog object yourself, use `leat.session`:

```python
from pyiceberg.catalog import load_catalog
from leat import session, JsonCheckpointStore

cat = load_catalog("prod", **your_props)
lt = session(cat, JsonCheckpointStore("offsets.json"), checkpoint_mode="sink")
```

---

## 5. Using Delta

Pass `format="delta"` and the identical easy path (`table()`, `@lt.model`,
`read()`, upsert, elastic workers) runs on Delta Lake. delta-rs is Rust-native —
**no JVM, no catalog**:

```python
lt = leat.connect("F:/leat", format="delta")

lt.table("db.events").write(pl.DataFrame({"id": [1, 2, 3], "value": [50, 150, 250]}))

@lt.model(source="db.events", sink="db.silver", start="earliest")
def silver(df):
    return df.filter(pl.col("value") > 100)

silver.run(once=True)
print(lt.table("db.silver").read())
```

**Identifier → path convention.** With Delta there is no catalog, so a string
identifier maps to a directory under `warehouse`: dots become path separators, so
`"db.events"` → `<warehouse>/db/events`. `catalog`/`uri` are ignored. Extra
`**opts` are passed to `DeltaFormat` as `storage_options` (S3/Azure creds, etc).

You can also use the format object directly (see the low-level section):

```python
from leat import DeltaFormat
events = DeltaFormat("F:/leat/db/events")
```

---

## 6. Transforms & modes

A pipeline is a Polars function plus a source and a sink:

```python
@lt.model(source="db.events", sink="db.silver",
          start="earliest", mode="append", key=None, on_change="warn")
def silver(df):                 # df: Polars DataFrame of business columns (no _offset)
    return df.filter(pl.col("value") > 100)   # return Polars (or Arrow)
```

Register multiple models and run them together:

```python
silver.run(once=True)
lt.run_all(once=True)           # run every registered model once
```

### `mode="append"` (default)

Inserts the transform output as new rows. This is the core incremental case:
new source rows → higher offsets → appended to the sink → exactly-once.

### `mode="upsert", key=[...]`

MERGES the transform output into the sink **by business key** (update matching
rows, insert new) instead of appending — so the sink holds *current state* and
reprocessing the same batch is a no-op (idempotent). This is how you handle
updates-expressed-as-appends / mutable data:

```python
@lt.model(source="db.events", sink="db.current", mode="upsert", key=["id"])
def current(df):
    return df                   # sink ends up holding the latest row per id
```

Within one batch, a repeated key is deduped to its highest-offset (latest) row
before the merge. Works on Iceberg (`Table.upsert`) and Delta (`MERGE`) with
identical final state. `key` accepts a string or a list.

### `on_change="warn" | "error" | "ignore"`

The offset cursor only sees *appends*. If the source gets an in-place
`UPDATE`/`DELETE`/overwrite (a row-changing commit the cursor can't see),
`on_change` surfaces it:

| value | behavior |
|---|---|
| `"warn"` (default) | logs a one-line warning once per detected change |
| `"error"` | raises `RuntimeError` (Spark-strict) |
| `"ignore"` | silent |

Benign compaction (Iceberg `replace` / Delta `OPTIMIZE`) is correctly **not**
flagged. See [updates & CDC](updates-and-cdc.md) for the full boundary — capturing
in-place deletes is roadmap (Delta CDF is the proven path), not shipped.

### The Kafka verbs are still there

Inside a transform the offset is invisible, but the pipeline still exposes the
Kafka-style controls:

```python
p = lt.pipeline("silver", "db.events", "db.silver", lambda df: df, start="earliest")
p.step()            # process exactly one batch (returns rows processed)
p.position()        # current committed offset
p.lag()             # rows behind latest
```

`seek()` / `commit()` live on the underlying `Consumer` (next section).

---

## 7. The low-level API

When you want the manual poll/commit loop, drop the ergonomic layer and wire the
pieces yourself: a `Consumer` over an `IcebergFormat`/`DeltaFormat`, plus a
checkpoint store.

```python
from leat import IcebergFormat, Consumer, JsonCheckpointStore
import polars as pl

events = IcebergFormat(lt.catalog, "db.events")     # source ("topic")
silver = IcebergFormat(lt.catalog, "db.silver")     # sink
ckpt   = JsonCheckpointStore("F:/leat/offsets.json")

c = Consumer(events, name="silver", checkpoint=ckpt, start="earliest")
while (batch := c.poll()) is not None:               # changes since committed offset
    kept = batch.pl().filter(pl.col("value") > 100)  # low-level path DOES see _offset
    silver.append(kept.to_arrow())
    c.commit()                                       # advance + persist the offset
```

Delta is identical — swap `IcebergFormat(catalog, id)` for
`DeltaFormat(path)`.

**`Consumer(source, name, checkpoint, start="latest", delivery="exactly_once",
on_change="warn")`** — `name` is the consumer group (its offset key). Methods:
`poll()` → `Batch | None`, `commit()`, `seek(offset)`, `reset()` (back to
earliest), `position()`, `lag()`.

**`Batch`** — `.pl()` (Polars), `.arrow()` (Arrow table), `.offset` (max offset in
the batch), `.num_rows`.

**Format adapters** (`IcebergFormat(catalog, identifier)` /
`DeltaFormat(table_uri, storage_options=None)`) share one surface:
`append(data, offsets=None)`, `upsert(data, keys, offsets=None)`,
`read_since(offset, hi=None)` → `(table, new_offset)`, `read_all()`,
`read_offsets()`, `latest_offset()` / `earliest_offset()`, plus the on-change
machinery `current_marker()` / `nonappend_ops_since(marker)`. Passing
`offsets={name: off}` embeds the offset in that commit's metadata (the atomic
exactly-once path). `append`/`upsert` auto-mint `_offset` when the caller doesn't
supply one.

**Checkpoint stores:** `JsonCheckpointStore(path)` keeps offsets in a JSON file;
`SinkCheckpointStore(sink_format)` reads/writes them from the sink's own commit
metadata (its `set` is a no-op — the offset is persisted atomically inside
`sink.append(offsets=...)`). `connect(checkpoint="sink")` selects the sink store;
anything else selects the JSON store.

---

## 8. Scaling & distributed

`leat` workers hold **no durable state** — the offset lives in the sink's commit
and coordination lives in a shared `ClaimStore` — so scaling is *replica count*,
not a cluster resize. Three ways to parallelize:

### 8a. Backfill mode (bounded initial load)

For the first full load of a large table, split the offset range into
`num_shards` disjoint buckets and run them to a fixed high-water mark. Each shard
is a checkpointed, run-to-completion, exactly-once job:

```python
from leat import Backfill

src = lt.source("db.events")
snk = lt.source("db.silver")

bf = Backfill(src, snk, transform, num_shards=8)
bf.run("all")            # run every shard in this process
# bf.run(3)             # or one shard  / bf.run([0, 1, 2]) a subset
print(bf.status())       # per-shard bookmark + completion
```

`Backfill(source, sink, transform, *, num_shards, until=None, shard_by="_offset",
checkpoint=None, claim_store=None, worker=None, name="backfill", ...)`. `until`
defaults to `source.latest_offset()` captured at construction. Pass a
`claim_store` to get lease-based failover across workers (a dead worker's shard is
reclaimed and resumed from its bookmark). When every shard is done, a steady-state
`Consumer` takes over from `until`.

### 8b. Elastic workers (anonymous, self-distributing)

`run_worker` is backfill's successor: **anonymous** workers each run the same loop
— find a bucket nobody live owns, claim it, drain it, move on. Start N copies of
the same script and they self-distribute; kill one and a survivor reclaims its
bucket exactly-once (resuming from the *sink* offset, not a bookmark):

```python
from leat import run_worker, open_claim_store

store = open_claim_store("local")        # one box; "etcd://..." across machines
stats = run_worker(
    lt.source("db.events"), lt.source("db.silver"), transform,
    name="silver", num_buckets=16, claim_store=store)
```

`run_worker(source, sink, transform, *, name, num_buckets, claim_store,
until=None, worker=None, ttl=30.0, batch_rows=200_000, idle_sleep=0.2,
on_event=None)`. Buckets are contiguous offset ranges `(lo, hi]`; `name` namespaces
the bucket keys (`f"{name}.bucket{i}"`); `on_event` gets a dict per
`claimed`/`committed`/`completed`/`failover` event (for logging). Returns a stats
dict.

The full money demo — real OS processes, scale-up mid-flight, kill a worker,
exactly-once verified against a single-process reference — is
[`examples/elastic_demo.py`](../examples/elastic_demo.py).

### 8c. ClaimStore — what it is and when you need it

A `ClaimStore` is the shared **"who owns which bucket right now"** table that lets
N workers coordinate and fail over. It manages *ownership/lease only* — never
progress (progress rides the sink offset). You only need it when you run **more
than one worker**. Build one with `open_claim_store(uri)`:

| URI | backend | scope | setup |
|---|---|---|---|
| `open_claim_store("local")` | SQLite (default file under the temp dir) | multi-process, **one machine** | none |
| `open_claim_store("sqlite:///F:/coord/claims.db")` | SQLite at an explicit path | multi-process, **one machine** | none |
| `open_claim_store("etcd://host:2379")` | etcd | **multi-machine** | run etcd + `[etcd]` extra |

**Local (SQLite) — the default.** One SQLite file shared by every process on the
box. Atomic claim = one conditional `UPDATE` guarded by a TTL, so SQLite's row
lock serializes racing claimers. Zero infrastructure; perfect for a multi-process
backfill or elastic run on a single node.

**etcd — for multiple machines.** etcd is a distributed key-value store with
**leases**: a key can be attached to a lease with a TTL, and while the holder is
alive it keepalives (refreshes) the lease. This lease **is** the failover
primitive — no reaper process, no liveness table:

> A worker claims a bucket by writing a key under an etcd lease. If the worker
> dies, it stops refreshing → the lease expires → **etcd auto-deletes the key** →
> the bucket frees itself → another worker claims it. Exactly-once holds because
> the replacement resumes from the sink's committed offset.

That auto-freeing across machines is why etcd is the pick for a real cluster.
Requires the `[etcd]` extra and `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`
(see Install).

The `ClaimStore` interface (`claim` / `renew` / `bookmark` / `get` / `complete` /
`release` / `list_claims` / `close`) is identical across backends
(`LocalClaimStore`, `EtcdClaimStore`), so worker code doesn't change when you move
from one box to a cluster.

### 8d. A concrete scaling config

The worker script is ~5 lines and identical everywhere — only the claim-store URI
(and, for multi-machine, the catalog + warehouse) change:

```python
# worker.py
import leat
from leat import run_worker, open_claim_store

lt = leat.connect("F:/leat")                      # (or a REST catalog + S3, see below)
store = open_claim_store("local")                 # swap for "etcd://coord:2379" on a cluster

run_worker(lt.source("db.events"), lt.source("db.silver"),
           lambda df: df.filter(pl.col("value") > 100),
           name="silver", num_buckets=16, claim_store=store)
```

**One machine — scale by running it more times.** Start the script 4×; they share
`open_claim_store("local")` (the same SQLite file) and self-distribute across the
16 buckets. Add or kill processes freely.

**Across a cluster — the three shared things.** For multi-machine, all replicas
must share:

1. **the catalog** — Postgres SQL catalog or a REST catalog (not local SQLite),
2. **the warehouse** — object storage (S3 / MinIO), and
3. **the claim store** — `open_claim_store("etcd://coord:2379")`.

Point every replica at the same three, then scale by replica count:

```python
lt = leat.connect("s3://warehouse/", uri="http://rest:8181", catalog="rest",
                  **{"s3.endpoint": "http://minio:9000",
                     "s3.access-key-id": "...", "s3.secret-access-key": "...",
                     "s3.path-style-access": "true"})
store = open_claim_store("etcd://coord:2379")
run_worker(lt.source("db.events"), lt.source("db.silver"), transform,
           name="silver", num_buckets=32, claim_store=store)
```

```bash
kubectl scale deployment/leat-silver --replicas=8   # scaling = replica count
```

### 8e. Stateless service note

Because compute holds no durable state, you deploy `leat` like a stateless
service, not a stateful monolith: scale out = add a replica, scale in = remove
one. Blue-green cutover falls out of the same property (point *green* at a
separate sink, parity-check, flip which sink downstream reads). **Honest status:**
scaling and failover are demonstrated and tested end-to-end
([`examples/elastic_demo.py`](../examples/elastic_demo.py)); the separate-sink
blue-green cutover is the deployment *pattern* that follows from stateless compute
(see the README's [scaling section](../README.md#scale--deploy-like-a-stateless-service-not-a-stateful-monolith)),
not a bundled feature.

---

## 9. CLI

Operate pipelines without writing a runner script. The CLI imports your `.py`
file, finds the `leat.connect(...)` session, and drives its `@lt.model` functions.

```bash
leat run pipeline.py --once             # process one batch then exit (a DAG task)
leat run pipeline.py                    # continuous incremental loop (default)
leat run pipeline.py --model silver     # run only this model
leat status pipeline.py                 # per-model: source, sink, position, lag
leat reset pipeline.py --model silver --to earliest   # earliest | latest | <offset>
leat --version
```

`leat run` accepts `--model NAME` (default: all), `--once` (single batch), and
`--loop` (continuous, the default). `leat reset --to` takes `earliest`, `latest`,
or an integer offset.

---

## 10. Gotchas & config reference

- **Windows: no-space warehouse paths.** Use `F:/leat`, not a path under
  `F:/Phyllax Engine`. `connect()` already sets `py-io-impl=FsspecFileIO` for the
  local Iceberg catalog (PyIceberg's default `file://` FileIO breaks on Windows
  drive letters); delta-rs takes plain forward-slash absolute paths.
- **`checkpoint="sink"` vs `"json"`.** `sink` = offset rides the sink's atomic
  commit (true exactly-once, no side file), but the sink must exist before the
  first poll. `json` (the `connect()` default) = a side JSON file; simpler, but
  there is a crash window between the data write and the offset write. The
  `@lt.model` easy path auto-creates sinks either way.
- **`start=` only applies to a fresh consumer.** Once an offset is committed under
  a given `name`, the consumer always resumes from it; `start` is ignored. Use
  `leat reset` (or `Consumer.seek`/`reset`) to move a committed offset.
- **etcd needs `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`** (see Install).
- **`num_buckets` / `num_shards` set the parallelism ceiling.** You can't have
  more concurrent workers than buckets/shards, so pick a count ≥ your max replicas.
- **Shipped vs roadmap.** Shipped: append + `mode="upsert"` (insert/update
  merge-by-key), `on_change` safety, elastic workers, backfill, etcd failover, REST
  catalog. Roadmap: capturing in-place **deletes / CDC** (Delta Change Data Feed is
  the proven path; Iceberg is blocked on PyIceberg incremental scans). See
  [updates & CDC](updates-and-cdc.md).
