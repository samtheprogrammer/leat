# Positioning: we are not replacing Spark

Let's be clear up front: **leat does not replace Spark.** It takes over the part
of medallion ETL that never needed a cluster, and leaves the rest to Spark.

## The 90/10 split

Most medallion ETL — call it ~90% — is small incremental batches: clean, cast,
dedupe, filter, light aggregate, enrich. Each run processes only the delta since
last time. That delta is bounded by construction and fits on one node. For this
work, a distributed engine is overhead you pay for and don't use.

The other ~10% genuinely needs distribution: huge single batches, big
non-co-partitioned shuffles and joins, high-cardinality aggregations, full
reprocesses larger than one node's memory, and ML. **Use Spark there.** leat has
nothing to offer that work and does not pretend to.

leat is the cost-efficient transform layer for the boring, high-volume middle.

## Where the line actually is (measured)

On a realistic medallion DAG (2 bronze → 2 silver → 1 gold join+agg) with **both**
engines Docker-pinned to the **same CPU budget** (`--cpus=4`, matched thread caps,
`--memory=12g`) — a true apples-to-apples comparison — leat wins wall-clock at every
measured size, and the margin narrows as data grows. Gold is **byte-identical** on
both (same sha256):

| size | leat backfill DAG | Spark backfill DAG | leat per-cycle | Spark per-cycle | leat CPU-s | Spark CPU-s | CPU ratio | parity |
|---|---|---|---|---|---|---|---|---|
| 5M  | **0.89 s** | 3.60 s (4.0×) | **372 ms** | 929 ms (2.5×) | ~3.8 s | 36.1 s | **~9.4×** | ✅ byte-identical |
| 20M | **3.01 s** | 5.95 s (2.0×) | **596 ms** | 990 ms (1.7×) | ~10.3 s | 47.7 s | **~4.6×** | ✅ byte-identical |

**This is equal-CPU, so it is not a parallelism trick.** At the same `--cpus=4` cap,
single-instance leat used only **~1.5–1.8 of 4** cores while Spark used **~2.5–2.8** —
yet leat still wins both wall-clock *and* CPU-seconds. So Spark does not win by being
more efficient: it burns **~4.6–9.4× the CPU-seconds** for the identical result, and
where it does win big-job wall-clock it does so by spreading *more* CPU across more
cores, not by doing less work. leat wins while leaving over half the CPU budget idle.

Two consequences:

- **The crossover (~35M rows) is an artifact of single-instance leat idling cores,
  not a ceiling.** The equal-CPU numbers show single-instance leat uses only
  ~1.5–1.8 of 4 cores, so it leaves over half the budget unused. leat's elastic
  engine runs N instances that split the work by partition and saturate that budget.
  Give leat the same cores Spark uses and, for partitionable work, the crossover
  largely disappears — you get leat's efficiency *and* Spark's parallelism.
  *(This multi-instance-vs-Spark comparison is the designed next step, now unblocked
  since parallel multi-writer commits on a REST catalog are proven; today's headline
  numbers are single-instance.)*
- **Cost wins even where wall-clock doesn't.** Above the crossover Spark finishes
  sooner, but by using more cores / more machines / more CPU — your bill goes up,
  not down. leat is still the cheaper way to get the same rows.

Two genuine ceilings that remain Spark's, regardless of instance count:

- **Shuffles.** Work where every worker must exchange data with every other
  mid-job — giant joins that don't fit in memory, global sorts, cross-partition
  high-cardinality aggregations — needs a coordinator moving data around.
  Independent leat instances don't talk to each other, so they can't shuffle.
  This is the real 10%.
- **Memory.** A single batch larger than one node's RAM (billions of rows in one
  shot) needs Spark regardless of speed.

## Decision table

| Your need | Use | Why |
|---|---|---|
| Incremental bronze/silver/gold, small-to-medium batches | **leat** | Fits one node, no cluster, cheap, embeds in your DAG |
| Append-only or CDC-log source (incl. updates as new-version rows) | **leat** | The offset cursor captures new rows; dedup-to-latest handles SCD ([updates & CDC](updates-and-cdc.md)) |
| Inserts + updates into current-state sink (merge by key) | **leat** | `mode="upsert", key=[...]` merges by business key — idempotent, current state ([updates & CDC](updates-and-cdc.md)) |
| Capturing **in-place** `UPDATE`/`DELETE` on the source | **leat: surfaces it, capture on roadmap** (Delta CDF proven; Iceberg blocked) | `on_change` warns/errors instead of silently missing; actual capture is the CDC path — CDC upstream or event-source the change ([updates & CDC](updates-and-cdc.md)) |
| Cut the bill on existing small/medium Spark or Delta Live Tables pipelines | **leat** | Same tables, same DAG, scale-to-zero (see [cost.md](cost.md)) |
| Huge single batch / full reprocess above ~35M rows or bigger than one node's RAM | **Spark** | Real distribution and shuffle needed |
| Big distributed joins, high-cardinality aggregations, ML training | **Spark** | Distributed compute and shuffle |
| Millisecond / true real-time hot path | **Kafka** (optionally Flink) | Table formats commit in batches; leat can't beat that floor |
| Unbounded streaming with dynamic rebalancing, live auto-failover | **Flink** | Needs a running coordinator — deliberately out of scope for leat |
| Stream-stream windowed joins | **Flink** | Stateful streaming join semantics |
| Interactive / ad-hoc queries, warehousing | **DuckDB / Trino / Snowflake** | leat builds pipelines, it is not a query engine |

## The short version

- **Kafka** owns the millisecond hot-path ingestion.
- **Flink** owns unbounded streaming with dynamic rebalancing and stream-stream joins.
- **Spark** owns the ~10%: huge batches, big shuffles, ML.
- **leat** owns the ~90% incremental transform middle — cheaply, on one node,
  inside your existing DAG.

If Kafka needs to sit in front for the hot path, that is fine: let Kafka handle
milliseconds and let leat do the cost-efficient table transform behind it.
