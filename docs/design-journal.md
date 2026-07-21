# leat — design journal

*The reasoning behind leat, decision by decision. Not API docs — the "why," including
the wrong turns and the corrections. Written to be mined for articles: each section is
roughly one blog post.*

---

## 0. The one-line thesis

**Most medallion ETL doesn't need a cluster.** For the ~90% of bronze→silver→gold work
that is small incremental batches — clean, cast, dedupe, filter, aggregate — a
distributed engine is overkill. You pay for a cluster to do work that fits on one core.

leat is Kafka's *consumer ergonomics* (offsets, `earliest`/`latest`, `seek`,
exactly-once) applied to **lakehouse tables** instead of a message broker. The table
is the log; a snapshot/offset is the offset. You get incremental ETL on a single node,
for a fraction of a Spark cluster's cost.

**Article angle:** "You're paying for a Spark cluster to filter 2 million rows." Lead
with the cost asymmetry: an always-on cluster vs. a triggered, scale-to-zero task that
runs the same transform 15–100× cheaper.

---

## 1. Why "Kafka semantics," and why over a table

The insight that started it: a lakehouse table already *is* an append log. Every commit
is a new snapshot. If you add a monotonic offset column, "read the changes since I last
looked" becomes "read rows where `_offset > last_committed`." That's a Kafka consumer,
except the topic is an Iceberg/Delta table and retention is the table's time-travel
window.

So the whole Kafka mental model ports over, one-to-one:

| Kafka | leat |
|---|---|
| topic | a table |
| offset | monotonic offset column / snapshot sequence |
| consumer group | a named `Consumer` with its own committed offset |
| `auto.offset.reset` | `start="earliest" \| "latest" \| <offset>` |
| `poll()` / commit | `poll()` / `commit()` |
| `seek()` | `seek()` / `reset()` + time-travel replay |

**Why this framing matters:** it means there's *nothing new to learn*. If you've used a
Kafka consumer, you already know leat's control surface. The novelty is entirely in
*where* the log lives (a warehouse table you already have), not in the API.

**Article angle:** "Your data warehouse is already a Kafka topic. You just haven't been
reading it like one."

---

## 2. Why micro-batch is a feature, not a limitation

The honest constraint: leat is micro-batch, not per-record. It doesn't do millisecond
streaming. But this isn't a shortcut — it's inherent to the substrate. **Table formats
commit in batches.** Iceberg and Delta both create discrete snapshots/commits; there is
no such thing as a per-row commit. That floor applies to *everyone* writing these
formats — including Flink. So leat isn't giving up latency that a "better"
implementation could recover; it's accepting a floor the format itself imposes.

The positioning that falls out: freshness is seconds-to-minutes, cheaply. Need
milliseconds? Put Kafka in front for the hot path and let leat do the cost-efficient
table transform behind it. leat is not competing with Flink; it's competing with
*running Spark for work that isn't big*.

**Article angle:** "Micro-batch isn't a compromise — it's the physics of table formats.
Here's why even Flink can't escape it."

---

## 3. The differentiator: neutrality, not speed

An early trap I had to be corrected on: over-indexing on raw speed. Spark wins big-job
wall-clock at scale — but only by *burning more CPU*, not by being more efficient (at
equal CPU, past a ~35M-rows/batch crossover, Spark's parallelism finishes the wall
clock sooner while still spending more CPU-seconds). Selling "we're faster than Spark"
is a mirage — the wall-clock win is only true below that crossover, and someone will
always out-benchmark you.

The durable differentiator isn't speed, it's **neutrality**:

- **Format-neutral** — a pluggable control layer (`TableFormat`) over Iceberg *and*
  Delta (delta-rs). Same `Consumer` code, either format.
- **Catalog-neutral** — SQLite, REST, Glue, Snowflake-Polaris; you bring the catalog.
- **Embeddable** — it's a library, not a platform. It runs inside whatever DAG you
  already have (Airflow, Dagster, a cron job, a Lambda).

"Another stream processor" is a crowded, losing category (that's Bytewax). "Lakehouse-
native incremental ETL that doesn't care which format/catalog/runner you use" is a
position with far less competition.

**Article angle:** "Stop benchmarking. The moat isn't speed, it's not caring which
table format you're on."

---

## 4. Don't rewrite the core in Rust — borrow one

A question that came up: should the library be in Rust for the stream joins? The answer
is no, and the reasoning is worth writing down because it's a common over-engineering
instinct.

leat is **thin Python orchestration** gluing Arrow-native engines. The heavy compute —
filters, joins, windows, aggregations — is delegated to **Polars** (silver transforms)
and **DuckDB** (gold joins/aggregations). Both are already Rust/C++, and both speak
Arrow, so the handoffs between them are zero-copy. The compute is ~1% of the code and
100% of the CPU work, and it's *already* native.

Rewriting leat's orchestration in Rust would harden the 1% that isn't the bottleneck
while throwing away Python's ecosystem reach. The right move is to be a great *conductor*
of native engines, not to re-implement them.

**Article angle:** "When *not* to reach for Rust: we wrote the glue in Python on purpose."

---

## 5. Where do offsets live? The checkpoint journey

This is the decision that quietly turned out to be the most important, so it deserves the
full arc.

**v0: a JSON file.** `{consumer_name: offset}`. Simple, obvious, works on one machine.
But it has a subtle correctness hole. The pipeline does two writes:

```
sink.append(data)      # commit #1: the data lands
checkpoint.set(offset) # commit #2: the offset advances
```

Crash *between* those two, and on restart the consumer re-reads from the stale offset and
re-processes a batch that already landed in the sink → **duplicate rows.** That's
at-least-once, not exactly-once.

**The fix: make the offset ride the sink's own commit.** Both Iceberg and Delta let you
attach arbitrary key/value metadata to a commit (Iceberg snapshot summary properties;
Delta commit custom metadata). So instead of a second write, you embed the offset *in
the same transaction as the data*:

```
sink.append(data, offsets={name: offset})   # ONE commit: data + offset together
```

Now the offset advances **if and only if** the append commits. There is no window. On
restart you read the offset back from the sink's latest commit. Exactly-once, and the
separate checkpoint store disappears entirely.

**The subtlety that makes it general:** a commit's metadata only carries the properties
set on *that* commit — the latest snapshot doesn't accumulate older snapshots' custom
props. So to recover offsets you scan commit history newest→oldest, taking the first
value seen per key. This looks like a detail, but it's exactly what makes the scheme work
for *many writers on one sink* later (see §7): each writer's key is independent, so you
just look back far enough to find each key's last commit.

**Why this became the linchpin:** everything elastic downstream (failover, scaling)
depends on a handoff being safe. A handoff is only safe if the offset and the data are
atomic. So this one checkpoint decision is load-bearing for the entire distributed model.

**Article angle:** "Exactly-once without a transaction log: how we hid the offset inside
the table's own commit." (This is the strongest standalone technical post.)

---

## 6. Coordination stores: the taxonomy that saved us

When distributed workers need to agree on "who's doing what," the instinct is to reach
for a fast embedded store — "LMDB is super fast, let's track workers there." This is
wrong, and articulating *why* is a genuinely useful piece of systems writing.

The requirement for coordination is **shared + multi-writer + atomic compare-and-swap.**
It is *not* speed — claims happen seconds-to-minutes apart, not in a hot loop.

That requirement immediately disqualifies a whole class of stores:

- **LMDB, DuckDB, SQLite** — embedded, single-writer, local. Blazing fast for one
  process on one machine, and *useless for coordination across machines*. A local mmap
  can't be shared; a single-writer file can't take concurrent atomic claims from many
  workers. (This is the trap: "fast" is the wrong axis. The axis is "shared + atomic.")

And it points at the right class:

- **Postgres / the catalog's own DB** — atomic conditional `UPDATE`, already in your
  stack. Top pick when you have it.
- **etcd** — purpose-built. Its lease/TTL primitive *is* the failover mechanism (see §8).
- **Redis** — `SET NX` + TTL.
- **S3 conditional writes** (If-None-Match/ETag) — zero-infra fallback, but slow per-op;
  the workaround path, not the hot path.

**The recurring principle:** *embedded/single-writer stores are for local one-writer
work; coordination needs shared, multi-writer, atomic CAS.* Once you internalize that,
the store choice is obvious every time.

leat makes this pluggable — a small `ClaimStore` interface with multiple backends, the
same "neutrality" philosophy as the format layer. The etcd backend serializes its claim
values as **protobuf** for a compact, fast hot path.

**Article angle:** "'Just use LMDB, it's fast' — and other ways to pick the wrong
coordination store." The fast-vs-shared distinction is the whole post.

---

## 7. Backfill vs steady-state: the unification (the best story)

This is the richest reasoning arc — four rounds of "wait, why is it built like that?"
Each round deleted machinery. Great blog material because it's a live demonstration of
*subtraction as design*.

**Round 1 — backfill as a separate class.** The obvious model: steady-state is one
consumer tailing the table forever; the initial load is too big for one node, so you add
a `Backfill` class that runs N sharded workers to a bounded high-water mark, then hands
off. Two concepts.

**Round 2 — "why is backfill a separate thing?"** Strip it down and a backfill worker
*is* a Consumer with two extra knobs: `until` (a stop offset) and `shard=(i, N)` (a
partition filter). Everything else — poll, transform, append, commit — is identical. In
Kafka there's no separate backfill API; you `seek(0)` and consume. So maybe backfill
isn't a class, it's two parameters.

**Round 3 — "I don't want to specify shards. Start N instances, let them figure it
out."** Correct, and sharper. Manually assigning `shard=i/N` is static and brittle. The
Kafka model is: start N *identical* workers with the same `group.id` (here: the same
`ClaimStore`), and they self-distribute by *claiming* work. No worker knows "I am 2 of
4." It just loops: claim any free-or-expired unit → process → claim the next. This is
**work-stealing**, and for a bounded work-list it needs only atomic claims + leases — no
coordinator, no leader, no membership protocol.

**Round 4 — "backfill and steady-state shouldn't be separate at all."** The real
scenario: you build a new gold table, start 8 workers to chew through the backlog, then
scale down to 2–3 for the steady tail. There's no *switch* — it's one job whose
parallelism changes. So "backfill" isn't a mode; it's just *"the buckets are deep right
now."* One elastic pool of anonymous workers, start to finish.

Each round removed a concept: a class, then a parameter, then static assignment, then the
mode distinction itself. What's left is a single idea — **a pool of workers claiming and
tailing units from a shared store** — that covers both the initial load and forever after.

**Article angle:** "We deleted our way to a distributed system: how a backfill feature
became two lines by asking 'why is this separate?' four times."

---

## 8. Scale-up without revocation: the new-partition trick

Unifying backfill and steady-state left one genuinely hard problem: **elastic scale-up.**
Scaling *down* is easy — a worker dies or leaves, its lease expires, survivors reclaim
its units. But scaling *up* seemed to require *revocation*: to feed a new worker, an
existing worker must drop a unit it's actively processing. Revocation means a handoff
window, which means duplicate risk, which means a fair-share/membership protocol to
coordinate the handoff. Complex, and it flirts with needing a real coordinator.

The trick that dissolved it: **tie scale-up to new partitions arriving.** A new worker
never steals in-flight work — it only ever claims an *uncontested* unit: a partition
nobody is processing. Where does uncontested work come from? New data. In a lakehouse,
data lands as new partitions/snapshots continuously. So:

- **Backfill:** many existing partitions are unclaimed → new workers grab those.
- **Steady-state:** new partitions keep arriving → new workers grab those.
- **Never:** a healthy worker forced to drop live work.

This **deletes the entire fair-share/membership/rebalancing layer.** Scale-up rides the
natural stream of new work instead of redistributing existing work. And because a
partition has exactly one owner for its whole processing life, exactly-once is preserved
*for free* — the only handoff that ever happens is death-triggered, which the atomic
checkpoint (§5) already makes safe.

**The boundary this clarified:** the thing that's genuinely out of scope isn't "dynamic
scaling" wholesale — it's *splitting a single in-flight partition across workers
mid-stream.* Everything else is coordinator-free.

**Article angle:** "The hardest part of our autoscaler was the part we deleted: scaling
up by waiting for new partitions instead of stealing old ones."

---

## 9. Partition as the unit — and the modern-table caveat

If partitions are the claim unit, what about a badly-partitioned table with 4 giant,
uneven partitions? Only 4 workers do anything and the fat one straggles.

First instinct: "well-designed tables *are* partitioned relevantly, so lean on partitions
and don't over-build." Mostly right — a pathological single-partition table punishes
*every* engine, not just leat, so it's not leat's job to paper over it.

But there's a real hole, and it's not "bad tables" — it's *modern* tables:

1. **Skew is often inherent and correct.** Partitioning is for *query pruning*, not
   compute balance. A table partitioned by `event_date` is well-designed *and* has a hot,
   growing "today" partition by definition.
2. **The 2026 best practice is drifting away from partitioning.** Delta/Databricks now
   say *don't partition tables under ~1TB — use liquid clustering* (often unpartitioned by
   design). Iceberg pushes hidden partitioning + sort/cluster-within to avoid the
   small-files problem. A *state-of-the-art* table may hand you **one** partition, and
   that's good design, not bad.

So the resolution: **don't hard-couple parallelism to physical partition count.** The
claim unit is *partition if partitions give enough units, else auto-subdivide by offset
range* — an automatic fallback triggered by low partition count, whose real motivating
case is good clustered/unpartitioned tables. Crucially, the offset-range sub-unit is
still deterministic, still claimed via the same lease, still exactly-once per unit — it's
*not* a new coordination layer, just a finer division. Partition-first (because
partition-equality is the most efficient pushdown Iceberg has — it prunes whole
manifests), offset-range only when needed.

**Article angle:** "'Just partition your tables' — except the best tables in 2026 aren't
partitioned. How we handle liquid-clustered lakehouses."

---

## 10. What we deliberately left out (and why that's the point)

Scope discipline is itself a design decision worth documenting:

- **Stream-stream windowed joins** (Flink's home turf) — out. Different problem.
- **Millisecond real-time** — out; it's the table-format floor (§2), not ours to fix.
- **Splitting one in-flight partition across workers mid-stream** — out; the one case that
  would need an always-on coordinator (§8).
- **A Rust core** — out; the native engines already are Rust (§4).

Everything we kept is coordinator-free or leans on infrastructure you already run
(your catalog's Postgres, your object store, your orchestrator). The through-line: *leat
can make coordination easy, but it won't conjure shared state from nothing* — cross-
machine agreement inherently needs a shared, network-reachable store, and the honest move
is to reuse the ones already in your stack rather than bundle a new one.

**Article angle:** "The features we said no to — and why a small library's scope is its
best feature."

---

## 11. The operational payoff: a stateless service, not a stateful monolith

This one we didn't set out to build. It fell out of §5 and §7–8, and it may be the
thing working engineers actually care about most — because it's the Spark pain they
live with.

**The Spark reality.** A Structured Streaming job is a *stateful monolith*. Its
streaming state lives in a checkpoint directory owned by one running application, with a
driver coordinating executors. Two consequences that everyone who has operated it knows
in their bones:

- **No blue-green deployment.** You can't stand "green" up beside "blue" — two apps can't
  share the checkpoint, and a logic change often breaks checkpoint compatibility. So
  upgrading the transform is *stop-the-world*: kill the job, start the new one, hope the
  state migrates. There is no clean cutover.
- **Scaling is a job resize, not a deployment.** To scale you reconfigure executors and
  usually restart the job (Structured Streaming never had good dynamic allocation). You
  scale *the one job*, coordinated by its driver — not `kubectl scale --replicas=8`.

Spark was designed as a cluster application, not a cloud-native service. That's the gap.

**What leat gets for free — from two decisions already made.** Not a bolted-on feature;
a direct consequence of the architecture:

1. **State lives in the sink's commit, not the compute process** (§5, the atomic
   checkpoint). The offset rides the table commit. So a running worker holds *no* durable
   state — it's disposable. Kill it, replace it, run ten of it; the truth is in the table.
2. **Workers are anonymous, coordinating only through a shared claim store** (§7). Cattle,
   not pets.

Put those together and you get the operational model Spark structurally can't:

- **Scale = replica count.** `kubectl scale --replicas=8`, or just start more processes.
  New workers claim unclaimed/new partitions. No job reconfig, no driver rebalance, no
  restart.
- **Blue-green = a deployment concern, not a state migration.** Because compute is
  stateless, two clean patterns work:
  - *True blue-green:* green writes a **separate sink**; you verify parity (the benchmark
    already gives us the parity tooling), then flip which sink downstream reads. A leat
    pipeline is just source→transform→sink, so a new sink is a one-line change.
  - *Rolling upgrade:* green claims new partitions while blue drains its held ones — the
    claim store guarantees one owner per partition, so they never collide.

**The honest caveats** (state them, or the claim isn't trustworthy):

- This is *architecturally enabled* by the pieces that exist (atomic checkpoint +
  ClaimStore), but the anonymous-worker **claim-and-tail loop that demonstrates it is the
  next build**, not a done thing. Don't claim `kubectl scale` works until the loop runs.
- *Same-sink logic skew:* during a rolling upgrade where blue and green write the **same**
  sink, they can emit different outputs during the overlap. True blue-green (separate sink
  → verify → swap) avoids this; leat makes that trivial, but *you* design the cutover
  semantics. Blue-green on the **output** is easy; blue-green on the **transform while
  sharing one sink** is the case to be careful with.

**Why this is the strongest "we're actually better" story:** the cost benchmark (§0, and
the measured ~4.6–9.4× CPU-seconds at equal CPU) says leat is *cheaper*. This says leat is *operationally
saner* — you deploy and scale it like any stateless service, which is exactly what Spark
streaming makes miserable. Cheaper is an argument; "I can finally blue-green my ETL and
scale it with a replica count" is a feeling, and feelings adopt tools.

**Article angle:** "Spark streaming is a stateful monolith. We made incremental ETL a
stateless service — scale it with `replicas=8`, deploy it blue-green." (Pair it with a
30-second clip of `2 → 8` workers absorbing load with no restart, and a green-sink
cutover. That demo is both the product proof and the credential.)

---

## 12. The honest benchmark: what we can claim without getting shot down

This section is the *positioning*, worked out the hard way — every time a claim sounded
too good, it got pushed on until only the defensible version survived. For a public post
this matters more than any feature: **an overclaim gets destroyed by one skeptic's
counter-benchmark; the narrow, honest claim is stronger because it holds up.**

The tempting headline is "faster and better than Spark." Don't. Here's the true picture,
measured on a real 2-bronze → 2-silver → 1-gold medallion DAG, **at equal CPU** (both
engines Docker-pinned to `--cpus=4`, matched thread caps, `--memory=12g`), parity-checked
so both engines produce byte-identical gold (same sha256 at 5M and 20M).

**Cost — always wins, say it loudly.** For the *identical* result, Spark burned **~4.6×
(20M) to ~9.4× (5M) the CPU-seconds** and a comparable-to-larger RSS footprint. Add
scale-to-zero: a leat task runs ~0.4–0.6s and exits; Spark needs a cluster *up* (idle
cost) or eats a **~3-second JVM cold start every trigger**. For an every-5-minutes job
that's ~15–100× cheaper, structurally. **Cost is the real win — not speed.**

**Speed — two different clocks, don't conflate them.**
- *Time to process one batch* (the benchmark's home turf): at equal CPU leat wins
  wall-clock at every measured size — backfill DAG **0.89 s vs 3.60 s (4.0×)** at 5M,
  **3.01 s vs 5.95 s (2.0×)** at 20M; per incremental cycle **372 ms vs 929 ms (2.5×)**
  → **596 ms vs 990 ms (1.7×)**. Roughly **~2× faster** on incremental table-to-table,
  with the margin narrowing as data grows. Real and measured.
- *Freshness floor* (minimum possible staleness): both leat and Spark-writing-a-table
  must **batch commits** — neither reaches milliseconds. Same wall. But since leat turns
  each batch faster with ~0 cold start, it can commit more often, so it's actually
  *fresher* than Spark-to-table, on top of cheaper.

**The sub-second trap (a mistake worth documenting).** It's tempting to concede "Spark
wins real-time latency." That's misleading: **if Spark writes to a Delta/Iceberg table it
hits the exact same floor leat does.** Sub-second real-time isn't a lakehouse-table job at
*all* — it's achieved by *not* writing a table in the hot path (streaming to Kafka, Redis,
a serving DB). Flink and Spark-SS can target those non-table sinks; leat is table-to-table
by design. So leat doesn't *lose* latency to Spark on table work — they're floored the
same and leat is faster within it. Spark only "wins" sub-second by doing a fundamentally
different job with a different sink. (Why the floor exists: a commit is write-parquet +
write-metadata + swap-the-catalog-pointer — you can't do that per row, so you batch. The
offset is a free rider in the metadata; it costs ~nothing and is *not* the cause.)

**Large scale — Spark wins wall-clock only by burning more.** Single-instance leat has a
crossover (~35M rows/batch) where Spark finishes sooner. But that's an artifact of
*single-instance leat idling cores* — at the equal `--cpus=4` cap leat used only
**~1.5–1.8 of 4** cores while Spark used **~2.5–2.8**. Spark wins big-job wall-clock
through **parallelism, not efficiency**: it uses *more* total CPU (~4.6–9.4× the
CPU-seconds here), just spread wider (and a real cluster adds machines → the bill goes
*up*). Run **multiple leat instances** (the elastic engine) on the same cores to
saturate the idle budget and you get efficiency *and* parallelism — for partitionable
work the crossover largely disappears. This is now the unblocked next experiment, since
parallel multi-writer commits on a REST catalog are proven.

**The one real exception — the shuffle (Spark's actual moat).** Operations where every
worker must swap data with every other mid-job — a giant join where neither side fits in
memory, a global sort, a cross-partition high-cardinality group-by — need a *shuffle*.
Independent leat instances don't talk to each other, so they can't. That coordination
machinery is dead weight on small jobs but *necessary* here. This is the honest 10% leat
deliberately doesn't chase.

**The claim that survives a skeptic:**
> "Same results for a fraction of the cost, ~2× faster on everyday incremental
> table-to-table, and far easier to run and scale — for the ~90% of ETL that's small
> frequent batches. Not for giant shuffles, and not for sub-second real-time (which isn't
> a table job for Spark either). **Not a Spark killer — a Spark-is-overkill killer.**"

**Article angle:** "I benchmarked my tool against Spark at *equal CPU*. It was ~2× faster
on everyday incremental batches — and, more durably, used ~4.6–9.4× fewer CPU-seconds for
byte-identical output." A post that *leads with the caveats* reads as far more trustworthy
than one that leads with a win — and the cost/CPU numbers land harder precisely because you
didn't oversell the speed. (The sequel to measure: multi-instance leat vs Spark on a
50–100M job over a REST catalog — now unblocked by proven parallel commits, the experiment
that turns "should beat Spark at scale" into a number.)

---

## Appendix: the reasoning style behind all of this

A pattern runs through every decision above, and it might be the meta-article:

1. **Ask "why is this separate/complex?" repeatedly.** Half the design came from deleting
   things, not adding them (§7).
2. **Pick the right axis.** "Fast" was the wrong axis for coordination; "shared + atomic"
   was the right one (§6). "Faster than Spark" was the wrong axis for positioning;
   "neutral" was the right one (§3).
3. **Lean on the substrate.** The table is already a log (§1); the commit is already
   atomic (§5); partitions are already the physical unit (§9). Most of leat is *noticing*
   what the lakehouse already gives you and not re-inventing it.
4. **Be honest about the floor.** Micro-batch, partition-count parallelism, coordinator
   for one edge case — naming the real limits made the rest of the design trustworthy.
