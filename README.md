# Barefoot Data Platform

Minimal, local-first data platform. Assets are Python or SQL files in `assets/`
that materialize into DuckDB.

## Quickstart

- `uv run bdp list`
- `uv run bdp check`
- `uv run bdp materialize`
- `uv run bdp materialize raw.base_numbers`
- `uv run bdp materialize main.enriched_numbers`

DuckDB lives at `bdp.duckdb` in the current working directory. Override with
`BDP_DB_PATH`.

## Assets

- Assets live in `assets/` and its subdirectories.
- Supported suffixes are `.py` and `.sql`.
- The first folder under `assets/` is the schema.
- Remaining folders become table-name prefixes joined with `_`.
- The file name is the final table-name segment.
- The asset key is derived as `schema.table_name`.
- Files directly under `assets/` are invalid.
- Files starting with `_` are ignored.
- The runner searches for the nearest `assets/` directory from the current
  working directory.

Examples:

- `assets/raw/base_numbers.py` -> `raw.base_numbers`
- `assets/raw/alt/base_numbers.py` -> `raw.alt_base_numbers`
- `assets/main/enriched_numbers.sql` -> `main.enriched_numbers`

## Metadata

Metadata is an optional leading comment block at the top of each asset file.

- Python uses `#`
- SQL uses `--`

Supported keys:

- `asset.description` optional free text stored as a table comment
- `asset.depends` optional comma-separated `schema.table` references and may be
  repeated

Other comment lines in the metadata block are ignored.

## Asset Types

### Python

Define a function named after the file name. It must return a
`polars.DataFrame`.

```python
# asset.description = Base numbers for demos
import polars as pl


def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({"value": [1, 2, 3]})
```

Python assets can use the public API to read dependencies or run SQL.

### SQL

The file is a SQL query with metadata.

```sql
-- asset.description = Base numbers for demos

select 1 as value
```

The runner executes it as:

```sql
create or replace table schema.table_name as <sql>
```

## CLI

- `bdp list`
- `bdp check`
- `bdp materialize`
- `bdp materialize schema.table [schema.table ...]`
- `bdp docs --out index.html`

## Python API

```python
import bdp

bdp.sql("create or replace table raw.example as select 1 as value")
frame = bdp.table("raw.example")
```

Utilities:

- `bdp.table` returns a `polars.DataFrame`
- `bdp.sql` executes SQL
- `bdp.db_connection` gives a DuckDB connection
