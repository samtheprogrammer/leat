"""End-to-end tests for the leat CLI (python -m leat.cli).

Each test writes a small runner file that builds a fresh warehouse under a
NO-SPACE path (the repo may live under a path with a space — importing from
there is fine, but PyIceberg warehouses must be space-free on Windows), defines
a @lt.model silver pipeline, and drives it via subprocess.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import uuid

import pyarrow as pa
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _no_space_dir() -> str:
    """A fresh, cross-platform, space-free warehouse dir under the OS temp dir."""
    base = os.path.join(tempfile.gettempdir(), "pxleat_cli_" + uuid.uuid4().hex[:8]).replace(os.sep, "/")
    assert " " not in base, f"warehouse path must be space-free: {base}"
    return base


def _write_runner(tmp_path, warehouse: str) -> str:
    """Create a runner .py that seeds bronze data and defines a silver model."""
    runner = os.path.join(str(tmp_path), "runner.py")
    src = textwrap.dedent(f"""
        import shutil
        import numpy as np
        import pyarrow as pa
        import polars as pl
        import leat

        WAREHOUSE = {warehouse!r}
        shutil.rmtree(WAREHOUSE, ignore_errors=True)

        lt = leat.connect(WAREHOUSE)
        schema = pa.schema([("_offset", pa.int64()), ("value", pa.int64())])
        events = lt.create("db.events", schema)
        lt.create("db.silver", schema)

        events.append(pa.table({{
            "_offset": np.arange(1000, dtype=np.int64),
            "value": np.arange(1000, dtype=np.int64),
        }}))

        @lt.model(source="db.events", sink="db.silver", start="earliest")
        def silver_clean(df):
            return df.filter(pl.col("value") > 100)
    """)
    with open(runner, "w") as f:
        f.write(src)
    return runner


def _seed_only(warehouse: str) -> str:
    """Runner that only seeds data once; reused across CLI invocations without
    wiping the warehouse (so committed offsets persist between calls)."""
    return textwrap.dedent(f"""
        import numpy as np
        import pyarrow as pa
        import polars as pl
        import leat

        WAREHOUSE = {warehouse!r}
        lt = leat.connect(WAREHOUSE)

        @lt.model(source="db.events", sink="db.silver", start="earliest")
        def silver_clean(df):
            return df.filter(pl.col("value") > 100)
    """)


def _run_cli(*cli_args) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "leat.cli", *cli_args]
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)


def _silver_rows(warehouse: str) -> int:
    import leat
    lt = leat.connect(warehouse)
    return lt.source("db.silver").read_all().num_rows


@pytest.fixture
def warehouse():
    wh = _no_space_dir()
    yield wh
    shutil.rmtree(wh, ignore_errors=True)


def test_run_once_populates_sink(tmp_path, warehouse):
    runner = _write_runner(tmp_path, warehouse)
    proc = _run_cli("run", runner, "--once")
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"
    # value > 100 out of 0..999 -> 899 rows
    assert _silver_rows(warehouse) == 899, proc.stdout + proc.stderr


def test_status_reports_model(tmp_path, warehouse):
    runner = _write_runner(tmp_path, warehouse)
    assert _run_cli("run", runner, "--once").returncode == 0
    # a non-wiping runner so the committed offset survives
    status_runner = os.path.join(str(tmp_path), "status_runner.py")
    with open(status_runner, "w") as f:
        f.write(_seed_only(warehouse))

    proc = _run_cli("status", status_runner)
    assert proc.returncode == 0, f"stdout={proc.stdout}\nstderr={proc.stderr}"
    assert "silver_clean" in proc.stdout
    # after a full --once run the position advanced to 999 and lag is 0
    assert re.search(r"\b999\b", proc.stdout), proc.stdout


def test_reset_moves_position(tmp_path, warehouse):
    runner = _write_runner(tmp_path, warehouse)
    assert _run_cli("run", runner, "--once").returncode == 0

    status_runner = os.path.join(str(tmp_path), "status_runner.py")
    with open(status_runner, "w") as f:
        f.write(_seed_only(warehouse))

    before = _run_cli("status", status_runner)
    assert before.returncode == 0
    assert re.search(r"\b999\b", before.stdout), before.stdout

    # reset back to earliest -> position becomes 'earliest', lag jumps to 1000
    reset = _run_cli("reset", status_runner, "--model", "silver_clean", "--to", "earliest")
    assert reset.returncode == 0, f"stdout={reset.stdout}\nstderr={reset.stderr}"

    after = _run_cli("status", status_runner)
    assert after.returncode == 0
    assert "earliest" in after.stdout, after.stdout
    # lag should now reflect all 1000 rows behind
    assert re.search(r"\b1000\b", after.stdout), after.stdout


def test_reset_to_offset(tmp_path, warehouse):
    runner = _write_runner(tmp_path, warehouse)
    assert _run_cli("run", runner, "--once").returncode == 0

    status_runner = os.path.join(str(tmp_path), "status_runner.py")
    with open(status_runner, "w") as f:
        f.write(_seed_only(warehouse))

    reset = _run_cli("reset", status_runner, "--model", "silver_clean", "--to", "500")
    assert reset.returncode == 0, f"stdout={reset.stdout}\nstderr={reset.stderr}"

    after = _run_cli("status", status_runner)
    assert after.returncode == 0
    assert re.search(r"\b500\b", after.stdout), after.stdout
