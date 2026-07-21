# How leat saves money

## The core difference: always-on cluster vs triggered task

Spark Structured Streaming (and Delta Live Tables) keep a cluster **running 24/7**
so it is ready for the next micro-batch. You pay for it while it idles between
batches, which for incremental pipelines is most of the time.

leat runs as a **triggered task**. It starts, processes the delta, commits, and
exits. Between runs it uses nothing — it scales to zero. On a serverless runtime,
you pay for the seconds it actually ran.

That single structural difference is where most of the savings come from.

## The measured numbers

**Incremental per-cycle latency** (medallion DAG, both engines Docker-pinned to the
**same CPU budget** — `--cpus=4`, matched thread caps, `--memory=12g`):

| | leat | Spark |
|---|---|---|
| per-cycle mean (5M) | **372 ms** | 929 ms (2.5×) |
| per-cycle mean (20M) | **596 ms** | 990 ms (1.7×) |
| cold start | ~0 (import + connect) | **~2.8–3.1 s** every trigger (JVM + SparkSession + codegen) |

At equal CPU leat is roughly **~2× lower per-cycle latency** with a ~3 s cold-start
advantage on **every** trigger. Cold start is JVM-vs-Python by nature — and a real
Spark cluster cold-starts in **minutes** (cluster provisioning, on top of the local
session start).

**Cost** (streaming — the dominant win):

- Spark Structured Streaming holds an always-on cluster: roughly
  **$150–1000+/month**.
- leat runs triggered and scales to zero: roughly **$5–10/month**, or pennies on
  a serverless runtime.

That is on the order of **15–100x cheaper** for streaming-shaped incremental work.

## Why the compute is cheap: the phase profile

We measured where leat's per-batch time actually goes (200k rows, local
filesystem):

| phase | share of batch |
|---|---|
| read (scan + Parquet decode) | ~44% |
| sink (Parquet write + Iceberg commit) | ~53% |
| transform (Polars) | ~1% |
| checkpoint | ~1% |

leat spends ~97% of a batch on real table I/O — reading the delta and writing the
result. It is doing actual work, not coordinating.

Spark is different: it is overhead-bound. It pays a fixed planning/scheduling tax
per job plus a per-stage cost, and a **~2.8–3.1 s JVM cold start every trigger**. On
the equal-CPU medallion benchmark that overhead shows up as **~4.6–9.4× the
CPU-seconds** for the byte-identical gold. On single-node small batches, Spark's
distributed-coordination machinery is dead weight.

Because compute is only ~1% of runtime, there is nothing to gain from rewriting
it — the native engines (Polars, DuckDB) are already fast enough that I/O
dominates. The savings come from **not running a cluster**, not from faster math.

## The FinOps / zero-migration angle

Cutting data-platform spend (Snowflake, Databricks) is a real, budgeted concern.
leat's angle is that you can cut the bill on the ~90% incremental portion with
**zero migration**:

- It runs in the **DAG you already have**.
- Over the **Iceberg tables you already have**.
- With **no lock-in** — it only uses the standard PyIceberg catalog interface, so
  the same code runs against REST, Glue, Snowflake-Polaris, or Unity.

You are not replatforming. You are moving the cheap-to-move 90% off an always-on
cluster and leaving the 10% on Spark.

## Honest caveats — where Spark wins

- **Local mode is Spark's best case.** In production the gap usually widens in
  leat's favor for latency and cost, but be skeptical of any single number.
- **Big-job wall-clock at scale goes to Spark — but only by burning more.** Above the
  ~35M-rows crossover Spark finishes sooner, not by being more efficient but by
  spreading *more* CPU across more cores (and, on a real cluster, more machines → the
  bill goes up). At equal CPU leat still wins CPU-seconds ~4.6–9.4× at every measured
  size; the crossover is an artifact of single-instance leat idling cores, which
  multi-instance closes for partitionable work (see [positioning.md](positioning.md)).
- **Memory ceiling.** If a batch is bigger than one node's RAM, you need Spark
  regardless of cost.
- These numbers are from one developer machine on the local filesystem. On object
  storage (S3) the read and sink phases dominate even more; treat the figures as
  directional, not a guarantee for your workload.

The pitch is not "leat is faster." It is "for the 90% that doesn't need
distribution, leat is **cheaper, drop-in, and no lock-in**."
