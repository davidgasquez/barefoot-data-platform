from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl

DATASETS_DIR_NAME = "datasets"
DEFAULT_DB_PATH = Path(os.environ.get("BDP_DB_PATH", "bdp.duckdb"))


@contextmanager
def db_connection(
    db_path: Path | str | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
    with duckdb.connect(path) as conn:
        yield conn


def query(
    statement: str,
    params: list[object] | None = None,
    *,
    db_path: Path | str | None = None,
) -> None:
    cleaned = statement.strip()
    with db_connection(db_path) as conn:
        if params is None:
            conn.execute(cleaned)
            return
        conn.execute(cleaned, params)


def sql(
    statement: str,
    params: list[object] | None = None,
    *,
    db_path: Path | str | None = None,
) -> None:
    query(statement, params, db_path=db_path)


def table(name: str, *, db_path: Path | str | None = None) -> pl.DataFrame:
    with db_connection(db_path) as conn:
        arrow_table = conn.execute(f"select * from {name}").fetch_arrow_table()
    return pl.DataFrame(arrow_table)


def find_datasets_root() -> Path:
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidate = parent / DATASETS_DIR_NAME
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("datasets directory not found")
