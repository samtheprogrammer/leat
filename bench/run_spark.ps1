# Run the Spark medallion benchmark inside the apache/spark:3.5.3 container.
#
# Reuses the proven pxbench Docker invocation pattern:
#   -u root                         : avoid Windows/Docker uid permission issues
#   --conf spark.jars.ivy=/tmp/.ivy2: writable Ivy cache (root-owned tmp)
#   --packages iceberg-spark-runtime-3.5_2.12:1.7.1 : Iceberg runtime (kept for
#       parity with pxbench even though this run reads neutral parquet directly).
#
# Mounts bench/ (this dir) to /work so spark_medallion.py can read data/*.parquet
# and write results_spark.json + gold_spark.parquet back to the host.
#
# NOTE: run materialize_data.py on the HOST first (needs numpy/pyarrow + gen.py).
#
# RESOURCE MEASUREMENT: instead of calling spark-submit directly, we run it as a
# CHILD of spark_launch.py, which reads resource.getrusage(RUSAGE_CHILDREN) after
# spark-submit exits to capture WHOLE-PROCESS (JVM-inclusive) CPU-seconds + peak
# RSS. This is equivalent to `/usr/bin/time -v` but needs no apt install (the
# `time` package is NOT in apache/spark:3.5.3, and installing it needs network on
# every run). The launcher merges resource_whole_run into results_spark.json.

$ErrorActionPreference = "Stop"
$BENCH = $PSScriptRoot
Write-Host "[run_spark] bench dir = $BENCH"

# Memory: the full preset (20M events, K=10 x 1M deltas) needs more headroom than
# the small preset. Under local[*] all compute runs in the driver JVM, so we raise
# spark.driver.memory to 10g and give the container a matching --memory ceiling
# (host has ample RAM). Bumped for the full-scale run (was 6g / no --memory).
docker run --rm -u root `
  --memory=14g `
  --shm-size=2g `
  -v "${BENCH}:/work" `
  -e BENCH_WORK=/work `
  apache/spark:3.5.3 `
  python3 /work/spark_launch.py -- `
    --master "local[*]" `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.driver.memory=10g `
    --conf spark.driver.maxResultSize=2g `
    /work/spark_medallion.py
