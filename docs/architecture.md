# Architecture: how leat works

The whole design follows one idea: **the Iceberg table is already the log.** So
leat doesn't build a broker, a cluster, or a coordinator. It builds the thin
incremental loop on top of what Iceberg already gives you.

## Iceberg is the broker

In Kafka you have brokers, ZooKeeper, offsets, and consumer groups. leat maps all
of that onto the table format — no extra infrastructure:

| Kafka concept | leat / Iceberg equivalent |
|---|---|
| the log / broker storage | the Iceberg table on object storage |
| offset | a monotonic offset column (a snapshot sequence) |
| coordinator | the catalog (REST / Glue / Polaris / Unity / SQLite) |
| consumer group | a named consumer with its own committed offset |
| an atomic commit | an Iceberg snapshot |
| incremental read (CDC) | predicate pushdown on the offset column |

An incremental read is just "give me the rows whose offset is greater than my
last committed one." leat issues a `GreaterThan` predicate on the offset column,
and Iceberg prunes the data files it doesn't need using per-file min/max stats. So
only the files containing new rows get scanned.

One implementation note, honestly stated: PyIceberg 0.11 has no snapshot-diff
scan, so leat uses a **monotonic offset column plus predicate pushdown** rather
than a true snapshot-to-snapshot diff. It works and it prunes files, but it is
not yet snapshot-diff CDC.

## Borrowed native compute

leat writes almost no compute code. The transform is done by native, Arrow-native
engines that are already fast:

- **Polars** for silver row transforms (filter, cast, dedupe, clean).
- **DuckDB** SQL for joins, window functions, and gold aggregations.

Both speak Arrow, so handoffs between the table read, the transform, and the sink
write are **zero-copy**. leat is thin Python orchestration gluing these together;
the heavy work is in their Rust/C++ cores.

This is deliberate. As the phase profile shows (see [cost.md](cost.md)), compute
is ~1% of a batch's runtime — the time goes to table I/O. So there is no reason to
rewrite the core in Rust: the engines that matter already are, and the bottleneck
isn't compute anyway.

```python
# silver: Polars in, Polars out
transform=lambda df: df.filter(pl.col("value") > 100)

# gold / joins: DuckDB SQL over Arrow tables, zero-copy
enriched = sql(
    "SELECT b.*, d.region FROM batch b LEFT JOIN dim d ON b.customer_id = d.customer_id",
    batch=batch.arrow(), dim=dim,
)
```

## Micro-batch, and why

leat is micro-batch — freshness in seconds to minutes, not milliseconds. That is
not a shortcut; it is a property of the table format. Iceberg (and Delta) commit
in **batches**, so a per-record commit isn't available to anyone writing these
formats — the same floor applies to Flink writing Iceberg. leat accepts the floor
and makes the batches cheap.

## Offsets live in the sink

A consumer's committed offset is its "consumer group" position. In v0 it is stored
in a small JSON checkpoint file. The intended production design stores the offset
as a **property on the sink table's snapshot**, so the offset advances **atomically
with the data commit**. That gives crash-safe exactly-once with no external offset
store to run — if the write commits, the offset commits with it; if it doesn't,
you reprocess the same delta and get the same result.

## No infrastructure: a DAG task that scales to zero

Because there is no broker and no coordinator to keep running, leat is just a
Python process. It embeds as a single task in whatever orchestrator you already
use (Airflow, Dagster, cron, a serverless trigger). Call `run(once=True)` and it
processes one batch and exits — perfect for a scheduled DAG task:

```python
lt.pipeline(name="silver_clean", source="db.events", sink="db.silver",
            transform=my_transform, start="earliest").run(once=True)
```

Between runs it consumes nothing. That scale-to-zero property is what makes it
cheap (see [cost.md](cost.md)) and what makes it drop-in: no new system to
operate, just a task in the pipeline you already have.

## Neutrality by construction

leat only ever calls the standard PyIceberg **Catalog** interface
(`create_table` / `load_table` / `scan` / `append`). It does not depend on any
specific catalog. We proved this class-level: the identical pipeline ran unchanged
against two different catalog implementations (`SqlCatalog` and
`InMemoryCatalog`) and produced identical results with exactly-once in both. The
same interface is implemented by REST, Glue, Snowflake-Polaris, and Unity, so leat
is catalog-agnostic. (A live cloud REST + S3 proof is still on the roadmap; the
class-level proof is done.)

The control layer is a small `TableFormat` protocol, so adding **Delta**
(delta-rs) is an adapter, not a rewrite. That is the other half of the neutrality
story, and it is next on the roadmap.
