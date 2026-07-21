# =============================================================================
# Equal-CPU head-to-head benchmark: leat vs Spark at the SAME --cpus=N budget.
#
# The old bench compared ONE leat process (~1.6 cores) against Spark local[*]
# (all cores) -> unfair on parallelism. This script fixes that:
#   * BOTH engines run in Docker with a hard --cpus=N CFS cap.
#   * BOTH internal thread pools capped to N:
#       leat  -> POLARS_MAX_THREADS=N + duckdb config threads=N (run_leat_capped.py)
#       Spark -> master local[N]  (SPARK_MASTER env, read by spark_medallion.py)
#   * BOTH get the same container --memory ceiling; Spark driver memory noted.
#   * SAME generated data (materialized once per size) feeds both.
#   * Parity (sha256 of gold) asserted at every size.
#
# Sweep: events sizes 5M, 20M, (50M optional). For each size + engine we collect
# wall (backfill total + per-stage + per-cycle stats), CPU-seconds (backfill +
# per-cycle), peak RSS, effective cores, Spark cold start, parity sha256 + PASS.
#
# Output: bench/results_equalcpu.json (+ per-size result copies), console table.
#
# Re-runnable. Long job (Docker build + multiple Spark runs). Use a long timeout.
# =============================================================================
$ErrorActionPreference = "Stop"
$BENCH = $PSScriptRoot
$REPO  = Split-Path $BENCH -Parent
Write-Host "[equal-cpu] bench=$BENCH repo=$REPO"

# ---- knobs ------------------------------------------------------------------
$CPUS       = if ($env:EQ_CPUS)   { [int]$env:EQ_CPUS }   else { 4 }      # --cpus=N on BOTH
$MEM        = if ($env:EQ_MEM)    { $env:EQ_MEM }         else { "12g" }  # --memory on BOTH
$SPARK_DRV  = if ($env:EQ_SPARKMEM) { $env:EQ_SPARKMEM }  else { "8g" }   # spark.driver.memory
$LEAT_IMG   = "leat-bench:latest"

# Sizes: "label:events:users:k:delta_events:delta_users:countries"
# users scale ~1/40th of events; countries fixed 200; K deltas of delta_events each.
$SIZES = if ($env:EQ_SIZES) { $env:EQ_SIZES -split ";" } else { @(
    "5M:5000000:200000:5:500000:10000:200",
    "20M:20000000:500000:5:1000000:10000:200"
) }
# 50M is opt-in (EQ_SIZES) - may OOM at --cpus=4 / --memory=12g; see report.

Write-Host "[equal-cpu] CPUS=$CPUS  MEM=$MEM  SPARK_DRIVER_MEM=$SPARK_DRV"
Write-Host "[equal-cpu] sizes: $($SIZES -join '  |  ')"

# ---- build the leat image ---------------------------------------------------
Write-Host "`n[equal-cpu] building leat image ($LEAT_IMG) ..."
docker build -f "$BENCH/docker/Dockerfile.leat" -t $LEAT_IMG "$BENCH"
if ($LASTEXITCODE -ne 0) { throw "leat image build failed" }

# ---- accumulate results -----------------------------------------------------
$all = [ordered]@{}
$meta = [ordered]@{
    cpus_cap          = $CPUS
    container_memory  = $MEM
    spark_driver_mem  = $SPARK_DRV
    leat_thread_caps  = "POLARS_MAX_THREADS=$CPUS, duckdb threads=$CPUS"
    spark_master      = "local[$CPUS]"
    leat_execution    = "docker --cpus=$CPUS (cgroup CFS cap), thread-capped"
    spark_execution   = "docker --cpus=$CPUS (cgroup CFS cap), local[$CPUS]"
    host              = "Windows 11 / Docker Desktop WSL2, 32 host cores, 125GB host RAM, Docker VM 15.6GB"
    spark_image       = "apache/spark:3.5.3"
    generated_seed    = "bench/gen.py SEED (deterministic)"
    timestamp         = (Get-Date).ToString("s")
}

foreach ($spec in $SIZES) {
    $p = $spec -split ":"
    $label = $p[0]; $events = $p[1]; $users = $p[2]; $k = $p[3]
    $de = $p[4]; $du = $p[5]; $countries = $p[6]
    Write-Host "`n============================================================"
    Write-Host "[size $label] events=$events users=$users K=$k delta_events=$de delta_users=$du"
    Write-Host "============================================================"

    # ---------------------------------------------------------------------
    # 1) leat in Docker at --cpus=N, thread-capped. Warehouse is CONTAINER-LOCAL
    #    (/tmp) via LEAT_BENCH_DIR; results + gold written to mounted /repo/bench.
    # ---------------------------------------------------------------------
    Write-Host "`n[size $label] --- leat (docker --cpus=$CPUS, POLARS=$CPUS/duckdb=$CPUS) ---"
    docker run --rm --cpus=$CPUS --memory=$MEM --shm-size=2g `
        -v "${REPO}:/repo" `
        -w /repo `
        -e LEAT_BENCH_DIR=/tmp/leat_bench_wh `
        -e LEAT_THREADS=$CPUS `
        -e PYTHONPATH=/repo:/repo/bench `
        $LEAT_IMG `
        python bench/run_leat_capped.py --events $events --users $users --k $k `
            --delta-events $de --delta-users $du --countries $countries
    if ($LASTEXITCODE -ne 0) { throw "leat run failed at size $label" }

    # snapshot leat artifacts for this size
    Copy-Item "$BENCH/results_leat.json" "$BENCH/results_leat_$label.json" -Force
    Copy-Item "$BENCH/gold_leat.parquet" "$BENCH/gold_leat_$label.parquet" -Force

    # ---------------------------------------------------------------------
    # 2) materialize the SAME data to neutral parquet (reads config from the
    #    just-written results_leat.json) - run inside the leat image so numpy/
    #    pyarrow/gen are available and deterministic.
    # ---------------------------------------------------------------------
    Write-Host "`n[size $label] --- materialize shared parquet ---"
    docker run --rm --memory=$MEM `
        -v "${REPO}:/repo" -w /repo -e PYTHONPATH=/repo:/repo/bench `
        $LEAT_IMG `
        python bench/materialize_data.py
    if ($LASTEXITCODE -ne 0) { throw "materialize failed at size $label" }

    # ---------------------------------------------------------------------
    # 3) Spark in Docker at --cpus=N, local[N], matching memory ceiling.
    # ---------------------------------------------------------------------
    Write-Host "`n[size $label] --- spark (docker --cpus=$CPUS, local[$CPUS], driver=$SPARK_DRV) ---"
    docker run --rm -u root --cpus=$CPUS --memory=$MEM --shm-size=2g `
        -v "${BENCH}:/work" `
        -e BENCH_WORK=/work `
        -e SPARK_MASTER="local[$CPUS]" `
        apache/spark:3.5.3 `
        python3 /work/spark_launch.py -- `
            --master "local[$CPUS]" `
            --conf spark.jars.ivy=/tmp/.ivy2 `
            --conf spark.driver.memory=$SPARK_DRV `
            --conf spark.driver.maxResultSize=2g `
            --conf spark.sql.shuffle.partitions=$CPUS `
            /work/spark_medallion.py
    if ($LASTEXITCODE -ne 0) { throw "spark run failed at size $label" }

    Copy-Item "$BENCH/results_spark.json" "$BENCH/results_spark_$label.json" -Force
    Copy-Item "$BENCH/gold_spark.parquet" "$BENCH/gold_spark_$label.parquet" -Force

    # ---------------------------------------------------------------------
    # 4) parity check (sha256 byte-parity of gold). Non-fatal to the loop but
    #    recorded as PASS/FAIL; we STOP-and-report if it FAILS.
    # ---------------------------------------------------------------------
    Write-Host "`n[size $label] --- parity check ---"
    docker run --rm -v "${BENCH}:/work" -w /work -e PYTHONPATH=/work `
        $LEAT_IMG python parity_check.py
    $parityPass = ($LASTEXITCODE -eq 0)
    Write-Host "[size $label] parity PASS=$parityPass"

    # ---------------------------------------------------------------------
    # 5) fold both engines' JSON into the combined result for this size.
    # ---------------------------------------------------------------------
    $lj = Get-Content "$BENCH/results_leat_$label.json" -Raw | ConvertFrom-Json
    $sj = Get-Content "$BENCH/results_spark_$label.json" -Raw | ConvertFrom-Json
    $all[$label] = [ordered]@{
        config       = $lj.config
        parity_pass  = $parityPass
        leat_sha256  = $lj.gold_fingerprint.sha256
        spark_sha256 = $sj.gold_fingerprint.sha256
        leat         = $lj
        spark        = $sj
    }

    if (-not $parityPass) {
        Write-Host "[size $label] *** PARITY FAILED - stopping sweep, see report ***" -ForegroundColor Red
        break
    }
}

# ---- write combined results -------------------------------------------------
$out = [ordered]@{ meta = $meta; sizes = $all }
# Write BOM-less UTF-8 so Python's json.load reads it directly (PS5.1 Out-File
# -Encoding utf8 emits a BOM that breaks json.load at char 0).
$json = $out | ConvertTo-Json -Depth 40
[System.IO.File]::WriteAllText("$BENCH/results_equalcpu.json", $json, (New-Object System.Text.UTF8Encoding($false)))
Write-Host "`n[equal-cpu] wrote $BENCH/results_equalcpu.json"
Write-Host "[equal-cpu] DONE."
