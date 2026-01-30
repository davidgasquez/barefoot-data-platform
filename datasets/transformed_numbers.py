# dataset.name = transformed_numbers
# dataset.schema = raw
# dataset.depends = raw.base_numbers
import polars as pl

import bdp


def transformed_numbers() -> pl.DataFrame:
    source_numbers = bdp.table("raw.base_numbers")
    return source_numbers.select(pl.sum("value").alias("total"))
