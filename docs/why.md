# Why leat exists

## The problem

You have a medallion pipeline: bronze to silver to gold. Most of the work is
small and incremental — clean, cast, dedupe, filter, light aggregate, enrich.
Each run touches only the new rows since last time.

To do this, you are probably running Spark. And Spark, for this kind of work,
means paying for a cluster to do a job that fits on one machine. The cluster is
either always on (idling between micro-batches) or slow to start. Either way you
are paying for distributed-coordination machinery that these small batches never
use.

That is the problem leat is built for.

## Where the idea came from

We measured that a single-node incremental engine beats Spark on cost and
performance for ordinary incremental work — not big-job wall-clock at huge scale,
but at **equal CPU**: same result, ~2× faster per cycle and **~4.6–9.4× fewer
CPU-seconds** on a realistic medallion DAG. The single-node approach spends its
time moving data, not scheduling jobs.

We wanted that advantage **portably**. Kafka Streams is tied to Kafka. Databricks
has its own fast path (Project Lightspeed), but it is Databricks-only. We did not
want to trade one lock-in for another.

Meanwhile, Apache Iceberg has become the open, vendor-neutral table format that
everyone agrees on — Snowflake, Databricks, AWS Glue, and others all read and
write it. So the answer became: build a lightweight incremental transform layer
**over Iceberg**, so any engine and any catalog can use it, with no lock-in.

## What leat is

`leat` gives you Kafka's consumer ergonomics over lakehouse tables:

- A table is a "topic."
- A snapshot / monotonic offset column is the offset.
- A named consumer is a consumer group, with its own committed offset.
- `poll()` returns the changes since your last commit; `commit()` advances and
  persists the offset.
- `earliest` / `latest` / `seek` / exactly-once all work as you would expect.

You write a transform in Polars or SQL. leat handles the incremental read, the
commit, and the offset bookkeeping. It runs as a single Python process — as one
task in your existing orchestrator — with no broker and no cluster.

```python
import leat, polars as pl

lt = leat.connect("/data/leat")
lt.pipeline(
    name="silver_clean",
    source="db.events",
    sink="db.silver",
    transform=lambda df: df.filter(pl.col("value") > 100),
    start="earliest",
).run(once=True)   # incremental, exactly-once, done
```

## Who it's for

Data engineers who:

- Run incremental medallion pipelines on Iceberg (or want to).
- Are paying for Spark or Delta Live Tables on small-to-medium pipelines and
  want the bill to be smaller.
- Want to stay engine- and catalog-neutral — no rewrite, no lock-in.
- Are happy with seconds-to-minutes freshness (micro-batch), not milliseconds.

If you need millisecond latency, huge distributed shuffles, or ML training, leat
is not the tool — see [positioning.md](positioning.md) and
[use-cases.md](use-cases.md). We are honest about the boundaries because that is
what makes the rest trustworthy.
