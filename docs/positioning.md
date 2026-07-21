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
measured size (5M, 20M and 50M). Gold is **byte-identical** on both (same sha256):

| size | leat backfill DAG | Spark backfill DAG | leat per-cycle | Spark per-cycle | leat CPU-s | Spark CPU-s | CPU ratio | parity |
|---|---|---|---|---|---|---|---|---|
| 5M  | **0.89 s** | 3.60 s (4.0×) | **372 ms** | 929 ms (2.5×) | ~3.8 s | 36.1 s | **~9.4×** | ✅ byte-identical |
| 20M | **3.01 s** | 5.95 s (2.0×) | **596 ms** | 990 ms (1.7×) | ~10.3 s | 47.7 s | **~4.6×** | ✅ byte-identical |
| 50M | **5.43 s** | 13.46 s (2.5×) | **832 ms** | 1053 ms (1.3×) | ~22.7 s | 74.1 s | **~3.3×** | ✅ byte-identical |

**This is equal-CPU, so it is not a parallelism trick.** At the same `--cpus=4` cap,
single-instance leat used only **~1.5–1.8 of 4** cores at 5M/20M (Spark ~2.5–2.8),
climbing to **~2.37** at 50M — yet leat still wins both wall-clock *and* CPU-seconds at
every size. So Spark does not win by being more efficient: it burns **~3.3–9.4× the
CPU-seconds** for the identical result, and where it does win big-job wall-clock it
does so by spreading *more* CPU across more cores, not by doing less work.

Two consequences:

- **There is no measured crossover on the realistic medallion through 50M.** The old
  "~35M-row crossover, Spark takes over past that" claim is **disproven** for
  join/agg medallion pipelines: it came from a trivial single-filter microbench
  (one leat instance vs all-core Spark, no expensive gold stage). On the realistic
  DAG the backfill margin did *not* collapse at 50M — it recovered to ~2.5×
  (4.0×→2.0×→2.5×) because Spark's join+agg gold stage balloons while leat's DuckDB
  gold stays cheap. In every equal-CPU benchmark we ran (5M/20M/50M) leat won; we
  haven't found the crossover on realistic medallion work. *(Caveat: for a
  pure-filter, shuffle-heavy, or much-larger-than-memory batch a crossover may still
  exist — we simply haven't measured one on medallion join/agg work. A multi-instance
  leat sequel, now unblocked since parallel multi-writer commits on a REST catalog are
  proven, would widen the win further; today's numbers are single-instance.)*
- **Cost wins even where wall-clock is close.** Even at 50M, where the per-cycle gap
  narrows to ~1.3×, leat still spends ~3.3× fewer CPU-seconds. Where Spark does win
  big-job wall-clock it does so by using more cores / more machines / more CPU — your
  bill goes up, not down. leat is still the cheaper way to get the same rows.

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
| Shuffle-heavy or full reprocess bigger than one node's RAM | **Spark** | Real distribution and shuffle needed (no measured size crossover on realistic medallion work through 50M) |
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
