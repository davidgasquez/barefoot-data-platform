# Repository Guidelines

The Barefoot Data Platform is a minimalistic and functional open data platform to help get, transform and publish datasets in the age of agents.

## Principles

- Minimal, simple/UNIXy, and opinionated
- Functional and idempotent transformations/pipelines
- Modular, declarative, independent, composable steps
- Low abstractions, no frameworks
- Everything is text/code, everything is versioned
- Colocated metadata and documentation
- Quick feedback cycles
  - Run any asset locally and immediately see results
  - Easy to debug
- No backward compatibility constraints
- Assets can fail without taking down the whole run

## Opinions

- Each asset is responsible for its own materialization and dependencies
- Datasets are files without any glue code
- Use full refresh pipelines as default

## Code

- Always `make run` after changing code
- Check README.md is up to date
- Always use `uv`
  - `uv run file.py`
  - `uv add`
  - `uv --help`

## Writing Assets

- Write assets (`.py`, `.sql`, `.sh` files) inside the `assets/` folder and subdirectories
- File name must match `asset.name`
- Metadata block at file top as language comments
  - Required `asset.name`, `asset.schema`
  - Optional `asset.description`
  - Optional `asset.depends` (can be repeated)
- Run checks (`uv run bdp check`) after writing assets

### Python assets

- Define a callable function named `asset.name` with no arguments
- Return a `polars.DataFrame` or `None`
- Use `bdp.table("schema.table")` to read dependencies
- Use `bdp.sql("sql query")` to run arbitrary SQL against the database

### SQL assets

- File content is a SQL query only
- Runner executes `create or replace table schema.table as <sql>`

### Bash assets

- Handles its own materialization
- Environment variables injected: `BDP_DB_PATH`, `BDP_SCHEMA`, `BDP_TABLE`.
- Table existence checked after script.
