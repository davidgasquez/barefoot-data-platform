# dataset.name = base_numbers
# dataset.schema = raw
import polars as pl


def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({"value": [1, 2, 3, 4]})
