# asset.description = Manual summary materialized from sandbox seed numbers
# asset.depends = raw.sandbox_seed_numbers
import polars as pl

import bdp


def manual_summary() -> None:
    seed_numbers = bdp.table("raw.sandbox_seed_numbers")
    summary = seed_numbers.select(
        pl.len().alias("row_count"),
        pl.col("value").sum().alias("total_value"),
    )
    with bdp.db_connection() as conn:
        conn.execute("create schema if not exists raw")
        conn.register("summary", summary)
        conn.execute(
            "create or replace table raw.sandbox_manual_summary "
            "as select * from summary"
        )
