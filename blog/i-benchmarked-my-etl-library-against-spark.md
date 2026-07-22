# I benchmarked my ETL library against Spark. It wasn't "faster." That's the interesting part.

*A write-up of [leat](https://github.com/samtheprogrammer/leat) — a lightweight, engine-neutral incremental ETL library over Apache Iceberg and Delta Lake — and what happened when I put it head-to-head with Spark on a realistic medallion pipeline.*

---

## The itch

Most of the data pipelines I've worked on share a shape: **bronze → silver → gold**, run incrementally, on schedule. Every run processes the *delta* — the handful of new rows since last time — cleans it, filters it, joins it to a dimension, rolls it up.

And almost all of that work is small. The delta is bounded by construction. It fits on one machine, easily. Yet the default tool for it is a distributed engine — a Spark cluster that has to be *up*, coordinating executors, running a JVM, ready to shuffle terabytes it will never see.

You're renting a cluster to filter two million rows.

So I built **leat**: Kafka's consumer ergonomics — offsets, `earliest`/`latest`, `seek`, exactly-once — but over lakehouse *tables* instead of a message broker. The table is the log; a snapshot is the offset. You get incremental ETL on a single node, embedded in whatever DAG you already have. No broker, no cluster.

Then I did the thing you're supposed to do: I benchmarked it against Spark. And I made myself a promise — **I would not cherry-pick.**

## How to benchmark honestly (which is harder than it sounds)

The easy, dishonest benchmark writes itself: run a filter on your single node, run it on Spark `local[*]`, watch Spark's JVM cold-start make you look 20× faster, publish the graph.

The problem is that graph is a lie in two ways:

1. It's a trivial op, not a real pipeline.
2. It pits **one** of my processes against Spark using **all** your cores.

So I built a real workload — a **medallion DAG**: two bronze tables → two silver tables (clean/filter) → one gold table (join + aggregate). And I made the fight fair:

- **Both engines pinned to the same CPU budget** with Docker `--cpus=4`.
- **Both capped to the same parallelism** — Spark on `local[4]`, leat with its thread pools capped to 4.
- **Same memory**, same seeded data, fed to both.
- And — the part that makes it trustworthy — **I asserted the outputs are byte-for-byte identical.** Same gold table, same SHA-256, on every run. If the answers don't match, the speed comparison is meaningless.

Here's what came out, at 5M and 20M input rows:

| size | leat (backfill) | Spark (backfill) | leat / cycle | Spark / cycle | **leat CPU-s** | **Spark CPU-s** | parity |
|---|---|---|---|---|---|---|---|
| 5M  | **0.89 s** | 3.60 s | **372 ms** | 929 ms | **~3.8 s** | 36.1 s | ✅ identical |
| 20M | **3.01 s** | 5.95 s | **596 ms** | 990 ms | **~10.3 s** | 47.7 s | ✅ identical |

## The result nobody puts in a headline

leat wins wall-clock at both sizes — but look at the trend: **the gap narrows as the data grows.** 4× → 2× on the backfill, 2.5× → 1.7× per incremental cycle. Extrapolate and you hit a crossover (around 35M rows per batch in my tests) where Spark's parallelism starts winning the clock.

So if I only measured wall-clock, the honest headline would be: **"at scale, it's roughly a wash."**

That's not a great headline. But it's the truth, and the *real* story is hiding in the column most speed benchmarks never show:

**CPU-seconds.** For the *byte-identical* result, Spark burned **~4.6–9.4× more CPU** than leat. That's the number that shows up on your cloud bill, because vCPU-seconds are what you pay for.

How can Spark finish *sooner* (at scale) while using *more* total CPU? Because wall-clock and total work are different things. Spark wins the clock by **keeping more cores busy at once** — it used ~2.5–2.8 of the 4 cores; leat used only ~1.5–1.8 and *still won*. Spark's whole machine — the scheduler, the shuffle service — exists to saturate cores. On a huge job that pays off in elapsed time. On the small, frequent jobs that make up most pipelines, it's just overhead you rent.

Spark doesn't win by being efficient. **It wins by throwing more hardware at the problem** — and in a real cluster, "more hardware" means *more machines*, which means your bill goes *up*, not down.

## And then there's the cold start

There's one more number. Every time Spark starts, it pays a **~3-second JVM + session cold start** before it does any work. leat's cold start is a Python `import`.

For a batch job that runs once a day, three seconds is nothing. For an **incremental** pipeline — the whole point of this exercise, the thing that runs every few minutes — it's decisive. Your options with Spark are: keep a cluster warm 24/7 (paying for idle time between triggers) or eat three seconds of cold start on every single trigger. leat starts, runs for ~0.4 seconds, and exits. It scales to zero.

Roughly: an every-5-minutes pipeline is a machine rented 24 hours a day versus a task that bills ~3 minutes a day. That's where the "15–100× cheaper" comes from — and it's *structural*, not a micro-optimization.

## Where you should absolutely still use Spark

If I stopped here I'd be doing exactly what I said I wouldn't — selling past the truth. So, plainly, the things leat is **not** for:

- **Shuffles.** Any job where every worker has to exchange data with every other worker mid-flight — a giant join where neither side fits in memory, a global sort, a cross-partition high-cardinality group-by — needs a coordinator moving data around. leat's model is independent workers that don't talk to each other; they can't shuffle. This is Spark's real, permanent moat.
- **Bigger than one machine's memory** in a single shot.
- **Sub-second real-time.** Writing to a Delta/Iceberg *table* commits in batches — that floor applies to Spark too, not just leat. True real-time means *not writing a table* in the hot path (Kafka/Flink to a serving sink). leat doesn't pretend to be that.

That's the honest ~10%. leat is built for the other ~90%.

## The takeaway

The instinct is to benchmark for speed and declare victory. But speed was the wrong axis. At equal hardware, for everyday incremental table-to-table work, leat is somewhat faster — and *much* cheaper: same answer, a fraction of the CPU, no cluster to keep warm.

**It's not a Spark killer. It's a Spark-is-overkill killer.**

If that resonates — if you've ever looked at a cluster bill and known, in your gut, that most of those jobs didn't need a cluster — the code, the benchmark (reproducible, with the parity checks), and the full design write-up are here:

**→ [github.com/samtheprogrammer/leat](https://github.com/samtheprogrammer/leat)**

Install it (`pip install leat`), point it at a table, and it reads like a Kafka consumer. I'd genuinely love to hear where it breaks for you.
