# asset.description = Seed numbers for sandbox assets
# asset.not_null = value
# asset.not_null = label
# asset.unique = value
import polars as pl


def seed_numbers() -> pl.DataFrame:
    return pl.DataFrame({
        "value": [10, 20, 30],
        "label": ["ten", "twenty", "thirty"],
    })
