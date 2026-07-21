# leat docs

`leat` is a lightweight, engine-neutral tool for incremental ETL over Apache
Iceberg and Delta Lake — both first-class, including the `connect(format="delta")`
easy path. It gives you Kafka-style consumer semantics — offsets,
`earliest`/`latest`, `seek`, exactly-once — over lakehouse tables instead of a
message broker. No broker, no cluster. It runs as one task inside the DAG you
already have.

Start here:

- [why.md](why.md) — Why leat exists: the cost insight behind it, the problem it solves, who it's for.
- [positioning.md](positioning.md) — We are not replacing Spark. The 90/10 split and a decision table (leat vs Spark vs Flink vs Kafka vs the warehouse).
- [cost.md](cost.md) — How it saves money: always-on cluster vs triggered task, the measured numbers, and where Spark still wins.
- [use-cases.md](use-cases.md) — What leat covers, and — just as important — what it does not.
- [architecture.md](architecture.md) — How it works: Iceberg as the log, borrowed native compute, micro-batch, offsets in the sink, scale-to-zero.

The headline benchmark numbers in these docs come from a realistic medallion DAG
(2 bronze → 2 silver → 1 gold join+agg) with **both** engines Docker-pinned to the
**same CPU budget** (`--cpus=4`, matched thread caps, `--memory=12g`) against Spark
3.5.3 — a true apples-to-apples comparison, gold byte-identical on both. A real
Spark cluster widens most gaps (cold start alone is minutes). We say so wherever
it matters.

Status: early but real (full test suite: 88 passing). Working today: **Iceberg and
Delta** source/sink (Delta now first-class, full parity — same `connect()` easy
path via `format="delta"`), Kafka-style `Consumer`,
incremental silver (Polars), stateful gold aggregation (DuckDB), stream-table joins,
exactly-once, **REST catalog neutrality proven live**, **parallel multi-writer
commits proven**, **etcd-backed distributed failover proven**, and **elastic workers**
(anonymous claim-and-tail, scale by process count). Apache-2.0.
