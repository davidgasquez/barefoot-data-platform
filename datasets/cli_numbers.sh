#!/usr/bin/env bash
# dataset.name = cli_numbers
# dataset.schema = raw
# dataset.depends = raw.base_numbers

set -euo pipefail

duckdb "${BDP_DB_PATH}" <<SQL
create or replace table ${BDP_SCHEMA}.${BDP_TABLE} as
select value * 10 as value
from raw.base_numbers
SQL
