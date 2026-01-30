from __future__ import annotations

import importlib.util
import os
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from types import ModuleType
from typing import Literal

import polars as pl

from bdp.api import DEFAULT_DB_PATH, db_connection, find_datasets_root

AssetKind = Literal["python", "sql", "bash"]
ASSET_SUFFIXES: dict[str, AssetKind] = {
    ".py": "python",
    ".sql": "sql",
    ".sh": "bash",
}
COMMENT_PREFIXES: dict[AssetKind, str] = {"python": "#", "sql": "--", "bash": "#"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
METADATA_LINE_RE = re.compile(
    r"dataset\.(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)"
)


@dataclass(frozen=True)
class Asset:
    name: str
    schema: str
    table: str
    path: Path
    kind: AssetKind
    source: str
    depends: tuple[str, ...]


def materialize(
    names: Iterable[str] | None = None,
    *,
    all_assets: bool = False,
) -> None:
    datasets_root = find_datasets_root()
    assets = discover_assets(datasets_root)
    deps_map = asset_dependencies(assets)
    selected = resolve_selection(names, all_assets, assets, deps_map)
    graph = {name: deps_map[name] for name in selected}
    try:
        order = list(TopologicalSorter(graph).static_order())
    except CycleError as exc:
        raise ValueError(f"Dependency cycle detected: {exc}") from exc
    for name in order:
        asset = assets[name]
        if asset.kind == "sql":
            materialize_sql(asset)
            continue
        if asset.kind == "python":
            materialize_python(asset)
            continue
        materialize_bash(asset)


def discover_assets(datasets_root: Path) -> dict[str, Asset]:
    assets: dict[str, Asset] = {}
    for path in asset_files(datasets_root):
        asset = asset_from_path(path)
        if asset.name in assets:
            raise ValueError(f"Duplicate asset name: {asset.name}")
        assets[asset.name] = asset
    return assets


def asset_dependencies(assets: dict[str, Asset]) -> dict[str, list[str]]:
    deps_map: dict[str, list[str]] = {}
    for name, asset in assets.items():
        deps: list[str] = []
        for dep_name in asset.depends:
            if dep_name == name:
                raise ValueError(f"Asset {name} depends on itself")
            if dep_name not in assets:
                raise ValueError(
                    f"Unknown dependency '{dep_name}' referenced in {asset.path}"
                )
            deps.append(dep_name)
        deps_map[name] = sorted(set(deps))
    return deps_map


def resolve_selection(
    names: Iterable[str] | None,
    all_assets: bool,
    assets: dict[str, Asset],
    deps_map: dict[str, list[str]],
) -> set[str]:
    if all_assets:
        selected = set(assets)
    else:
        if not names:
            raise ValueError("Pass --all or asset names to materialize.")
        requested = list(names)
        unknown = sorted(set(requested) - set(assets))
        if unknown:
            raise ValueError(f"Unknown assets: {', '.join(unknown)}")
        selected = set(requested)
    stack = list(selected)
    while stack:
        name = stack.pop()
        for dep in deps_map[name]:
            if dep not in selected:
                selected.add(dep)
                stack.append(dep)
    return selected


def asset_files(datasets_root: Path) -> list[Path]:
    asset_paths: list[Path] = []
    for path in datasets_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("_"):
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix not in ASSET_SUFFIXES:
            continue
        asset_paths.append(path)
    return sorted(asset_paths)


def asset_from_path(path: Path) -> Asset:
    kind = ASSET_SUFFIXES[path.suffix]
    source = path.read_text(encoding="utf-8")
    schema, table, depends = parse_dataset_metadata(path, kind, source)
    name = f"{schema}.{table}"
    return Asset(
        name=name,
        schema=schema,
        table=table,
        path=path,
        kind=kind,
        source=source,
        depends=depends,
    )


def parse_dataset_metadata(
    path: Path,
    kind: AssetKind,
    source: str,
) -> tuple[str, str, tuple[str, ...]]:
    prefix = COMMENT_PREFIXES[kind]
    lines = extract_metadata_lines(source, prefix)
    if not lines:
        raise ValueError(f"Missing dataset metadata in {path}")
    metadata = parse_metadata_lines(lines, path)
    schema = single_metadata_value(metadata, "schema", path)
    table = single_metadata_value(metadata, "name", path)
    validate_identifier(schema, "schema", path)
    validate_identifier(table, "table", path)
    depends = parse_dependencies(metadata.get("depends", []), path)
    return schema, table, tuple(depends)


def extract_metadata_lines(source: str, prefix: str) -> list[str]:
    lines: list[str] = []
    for line in source.splitlines():
        stripped = line.lstrip()
        if not stripped:
            if not lines:
                continue
            continue
        if stripped.startswith(prefix):
            content = stripped[len(prefix) :].lstrip()
            if content:
                lines.append(content)
            continue
        break
    return lines


def parse_metadata_lines(lines: list[str], path: Path) -> dict[str, list[str]]:
    metadata: dict[str, list[str]] = {}
    for line in lines:
        if "dataset." not in line:
            continue
        match = METADATA_LINE_RE.fullmatch(line)
        if match is None:
            raise ValueError(f"Invalid dataset metadata line in {path}: {line}")
        key = match.group("key")
        value = match.group("value").strip()
        metadata.setdefault(key, []).append(value)
    return metadata


def single_metadata_value(metadata: dict[str, list[str]], key: str, path: Path) -> str:
    if key not in metadata or not metadata[key]:
        raise ValueError(f"Missing dataset.{key} in {path}")
    if len(metadata[key]) != 1:
        raise ValueError(f"dataset.{key} must appear once in {path}")
    value = metadata[key][0]
    if not value:
        raise ValueError(f"dataset.{key} must have a value in {path}")
    return value


def parse_dependencies(values: list[str], path: Path) -> list[str]:
    deps: list[str] = []
    for raw in values:
        if not raw:
            continue
        parts = [part.strip() for part in raw.split(",")]
        for part in parts:
            if not part:
                continue
            validate_asset_reference(part, path)
            deps.append(part)
    return deps


def validate_asset_reference(value: str, path: Path) -> None:
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid dependency '{value}' in {path}. Expected schema.table."
        )
    schema, table = parts
    validate_identifier(schema, "schema", path)
    validate_identifier(table, "table", path)


def validate_identifier(value: str, label: str, path: Path) -> None:
    if IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} name '{value}' from {path}")


def materialize_sql(asset: Asset) -> None:
    sql = asset.source.strip()
    if not sql:
        raise ValueError(f"SQL asset is empty: {asset.path}")
    with db_connection() as conn:
        conn.execute(f"create schema if not exists {asset.schema}")
        conn.execute(
            f"create or replace table {asset.schema}.{asset.table} as {sql}"
        )


def materialize_python(asset: Asset) -> None:
    module = load_module(asset.path)
    func = getattr(module, asset.table, None)
    if func is None or not callable(func):
        raise ValueError(
            f"Python asset {asset.path} must define callable {asset.table}"
        )
    result = func()
    if not isinstance(result, pl.DataFrame):
        raise TypeError("Python assets must return polars.DataFrame")
    write_frame(asset.schema, asset.table, result)


def materialize_bash(asset: Asset) -> None:
    with db_connection() as conn:
        conn.execute(f"create schema if not exists {asset.schema}")
    env = dict(os.environ)
    env.setdefault("BDP_DB_PATH", str(DEFAULT_DB_PATH))
    env["BDP_SCHEMA"] = asset.schema
    env["BDP_TABLE"] = asset.table
    subprocess.run(["bash", asset.path.as_posix()], check=True, env=env)
    ensure_table_exists(asset.schema, asset.table, asset.path)


def load_module(module_path: Path) -> ModuleType:
    module_name = module_name_from_path(module_path)
    module_spec = importlib.util.spec_from_file_location(module_name, module_path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Unable to load asset module: {module_path}")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def write_frame(schema: str, table: str, df: pl.DataFrame) -> None:
    with db_connection() as conn:
        conn.execute(f"create schema if not exists {schema}")
        conn.register("df", df)
        conn.execute(f"create or replace table {schema}.{table} as select * from df")


def ensure_table_exists(schema: str, table: str, path: Path) -> None:
    with db_connection() as conn:
        row = conn.execute(
            "select 1 from information_schema.tables "
            "where table_schema = ? and table_name = ? limit 1",
            [schema, table],
        ).fetchone()
    if row is None:
        raise ValueError(f"Asset {path} did not create {schema}.{table}")


def module_name_from_path(module_path: Path) -> str:
    sanitized = module_path.as_posix().replace("/", "_").replace(".", "_")
    return f"bdp_asset_{sanitized}"
