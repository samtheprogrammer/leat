# Use cases and boundaries

Read both lists. The boundaries matter as much as the use cases — knowing when
**not** to use leat is what keeps it honest.

## What leat is good for

**Incremental medallion ETL (bronze → silver → gold).**
The core case. Consume the delta since your last commit, transform it, append to
the sink, commit the offset. Exactly-once. This is proven and working today.

**Cost replacement for Spark Structured Streaming / Delta Live Tables on
small-to-medium pipelines.**
Same Iceberg tables, same DAG, but running as a triggered task that scales to
zero instead of an always-on cluster. See [cost.md](cost.md).

**CDC into the lakehouse.**
Land change data as incremental batches and apply it to silver/gold tables.
(Delete handling via Iceberg equality deletes is on the roadmap, not done yet.)

**Incrementally-maintained rollups / materialized views for BI.**
Stateful gold aggregation where the gold table itself is the state store — each
batch merges only its delta. We verified that incremental results match a
from-scratch recompute.

**Stream-table enrichment joins.**
Join each incoming delta against a dimension table using DuckDB's native hash
join over Arrow — no join engine to build, no cluster. See
`examples/silver_join.py`.

**Blue-green / multi-region.**
Independent named consumers each keep their own offset, so you can run parallel
consumers off the same table without them interfering.

**Parallel backfill / catch-up.**
Static sharding over a bounded batch — split the offset range across workers with
no coordinator needed.

## What leat is NOT for

Say no to these clearly:

**Millisecond / true real-time.**
Table formats commit in batches, so freshness is seconds-to-minutes for everyone
writing Iceberg — even Flink. If you need milliseconds, use **Kafka** (optionally
Flink) for the hot path, and let leat do the cost-efficient transform behind it.

**Stream-stream windowed joins.**
That is Flink's stateful streaming-join territory. Use **Flink**.

**Huge single batch / full reprocess.**
Above the ~35M-rows crossover, or anything larger than one node's memory, use
**Spark**. leat is for the bounded incremental delta, not the giant one-shot job.
(That crossover is an artifact of *single-instance* leat idling cores — at equal CPU
it uses only ~1.5–1.8 of 4 — not a fundamental ceiling; multi-instance leat closes
it for partitionable work. The genuinely-Spark cases below don't.)

**Big shuffles.**
Giant joins where neither side fits in memory, global sorts, cross-partition
high-cardinality group-bys — every worker must swap data with every other mid-job.
Independent leat instances don't talk to each other, so they can't shuffle. This is
Spark's real moat. Use **Spark**.

**Interactive / ad-hoc query or warehousing.**
leat builds pipelines; it is not a query engine. Query your Iceberg tables
directly with **DuckDB, Trino, or Snowflake**.

**ML training.**
Out of scope. Use **Spark** or a dedicated ML stack.

**Dynamic streaming rebalancing / live auto-failover.**
That requires a running coordinator, which means standing up infrastructure —
exactly the thing leat exists to avoid. It is deliberately declined. Use
**Flink** if you need it.

## Rule of thumb

If the work is a bounded incremental transform that fits on one node and
seconds-to-minutes freshness is fine, leat is a good fit. If it needs a
coordinator, a cluster, or millisecond latency, reach for the tool built for
that — and see [positioning.md](positioning.md) for the full decision table.
