"""Compute layer: DuckDB SQL over Arrow tables, zero-copy.

Joins, window functions, and heavy gold aggregations go here — DuckDB does the
native C++ execution; leat just hands it Arrow and gets Arrow back. No engine to run.
"""
from __future__ import annotations
import duckdb
import pyarrow as pa


def sql(query: str, **tables: pa.Table) -> pa.Table:
    """Run a DuckDB query over named Arrow tables and return Arrow.

    Each keyword becomes a queryable table:
        sql("SELECT b.*, d.region FROM batch b JOIN dim d ON b.cid = d.cid",
            batch=batch_arrow, dim=dim_arrow)
    """
    con = duckdb.connect()
    try:
        for name, tbl in tables.items():
            con.register(name, tbl)
        return con.execute(query).fetch_arrow_table()
    finally:
        con.close()
