from collections.abc import Callable
from pathlib import Path

import pytest

from bdp.api import table
from bdp.materialize import (
    asset_from_path,
    materialize_asset,
    parse_dependencies,
    parse_not_null,
    parse_unique,
    validate_python_asset_source,
)


def test_asset_from_path_parses_repeated_single_value_metadata(tmp_path: Path) -> None:
    assets_root = tmp_path / "assets"
    asset_path = assets_root / "raw" / "example.py"
    asset_path.parent.mkdir(parents=True)
    asset_path.write_text(
        "\n".join([
            "# asset.depends = raw.base_numbers",
            "# asset.depends = raw.other_numbers",
            "# asset.not_null = value",
            "# asset.not_null = label",
            "# asset.unique = value",
            "# asset.unique = label",
            "# asset.assert = value > 0",
            "import polars as pl",
            "",
            "def example() -> pl.DataFrame:",
            '    return pl.DataFrame({"value": [1], "label": ["one"]})',
            "",
        ]),
        encoding="utf-8",
    )

    asset = asset_from_path(asset_path, assets_root)

    assert asset.python_materialization == "dataframe"
    assert asset.depends == ("raw.base_numbers", "raw.other_numbers")
    assert asset.tests.not_null == ("value", "label")
    assert asset.tests.unique == ("value", "label")
    assert asset.tests.assertions == ("value > 0",)


@pytest.mark.parametrize(
    ("parser", "value", "message"),
    [
        (parse_dependencies, "raw.base_numbers, raw.other_numbers", "one dependency"),
        (parse_not_null, "value, label", "one column"),
        (parse_unique, "value, label", "one column"),
    ],
)
def test_comma_separated_metadata_is_rejected(
    parser: Callable[[list[str], Path], list[str]],
    value: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        parser([value], Path("asset.py"))


def test_python_asset_can_return_none_when_it_materializes_itself(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assets_root = tmp_path / "assets"
    asset_path = assets_root / "raw" / "sandbox" / "manual_numbers.py"
    asset_path.parent.mkdir(parents=True)
    db_path = tmp_path / "bdp.duckdb"
    monkeypatch.setenv("BDP_DB_PATH", str(db_path))
    asset_path.write_text(
        "\n".join([
            "# asset.description = Self materialized numbers",
            "import bdp",
            "",
            "def manual_numbers() -> None:",
            '    bdp.sql("create schema if not exists raw")',
            (
                '    bdp.sql("create or replace table raw.sandbox_manual_numbers '
                "as select 1 as value, 'one' as label\")"
            ),
            "",
        ]),
        encoding="utf-8",
    )

    asset = asset_from_path(asset_path, assets_root)

    assert asset.python_materialization == "manual"
    materialize_asset(asset)

    result = table("raw.sandbox_manual_numbers", db_path=db_path)
    assert result.to_dicts() == [{"value": 1, "label": "one"}]


def test_python_asset_requires_explicit_supported_return_type(tmp_path: Path) -> None:
    asset_path = tmp_path / "assets" / "raw" / "broken.py"
    asset_path.parent.mkdir(parents=True)
    asset_path.write_text(
        "\n".join([
            "def broken():",
            "    return 1",
            "",
        ]),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"must declare return type pl\.DataFrame or None",
    ):
        validate_python_asset_source(asset_path)
