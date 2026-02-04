from __future__ import annotations

import html
from pathlib import Path

import duckdb

from bdp.api import db_connection, find_assets_root
from bdp.materialize import Asset, discover_assets, metadata_from_source


def generate_docs(out_path: Path | str, sample_rows: int = 10) -> None:
    output_path = Path(out_path)
    assets_root = find_assets_root()
    assets = discover_assets(assets_root)
    if not assets:
        raise ValueError("No assets found.")
    sorted_assets = [assets[key] for key in sorted(assets)]
    rendered_assets: list[str] = []
    with db_connection() as conn:
        for asset in sorted_assets:
            source = asset.path.read_text(encoding="utf-8")
            metadata, _ = metadata_from_source(asset.path, asset.kind, source)
            if not table_exists(conn, asset.schema, asset.name):
                raise ValueError(
                    f"Missing table {asset.schema}.{asset.name}. "
                    "Run `bdp materialize`."
                )
            columns = conn.execute(
                "select column_name, data_type "
                "from information_schema.columns "
                "where table_schema = ? and table_name = ? "
                "order by ordinal_position",
                [asset.schema, asset.name],
            ).fetchall()
            row_count = conn.execute(
                f"select count(*) from {asset.schema}.{asset.name}"
            ).fetchone()[0]
            sample_columns, sample_values = fetch_sample_rows(
                conn,
                asset.schema,
                asset.name,
                sample_rows,
            )
            rendered_assets.append(
                render_asset_section(
                    asset,
                    assets_root,
                    metadata,
                    columns,
                    row_count,
                    sample_columns,
                    sample_values,
                )
            )
    html_doc = render_document(rendered_assets, sorted_assets)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_doc, encoding="utf-8")


def table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = conn.execute(
        "select 1 from information_schema.tables "
        "where table_schema = ? and table_name = ? limit 1",
        [schema, table],
    ).fetchone()
    return row is not None


def fetch_sample_rows(
    conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    limit: int,
) -> tuple[list[str], list[tuple[object, ...]]]:
    cursor = conn.execute(
        f"select * from {schema}.{table} limit {limit}"
    )
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return columns, rows


def render_document(rendered_assets: list[str], assets: list[Asset]) -> str:
    index_lines = ["<ul>"]
    for asset in assets:
        key = html.escape(asset.key)
        index_lines.append(f"  <li><a href=\"#{key}\">{key}</a></li>")
    index_lines.append("</ul>")
    index_html = "\n".join(index_lines)
    sections_html = "\n".join(rendered_assets)
    return "\n".join(
        [
            "<!doctype html>",
            "<html lang=\"en\">",
            "<head>",
            "  <meta charset=\"utf-8\">",
            "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
            "  <title>BDP Docs</title>",
            "  <link rel=\"icon\" href=\"data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 viewBox=%270 0 100 100%27%3E%3Crect width=%27100%27 height=%27100%27 fill=%27white%27/%3E%3Ctext x=%2750%27 y=%2762%27 font-size=%2748%27 text-anchor=%27middle%27 fill=%27black%27 font-family=%27monospace%27%3Ebdp%3C/text%3E%3C/svg%3E\">",
            "  <style>",
            "    :root { color-scheme: light }",
            "    body { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace }",
            "    body { background: #fff }",
            "    body { color: #111 }",
            "    body { margin: 0 }",
            "    body { padding: 24px }",
            "    body { line-height: 1.5 }",
            "    .layout { display: grid }",
            "    .layout { grid-template-columns: 240px 1fr }",
            "    .layout { gap: 24px }",
            "    .layout { align-items: start }",
            "    main { max-width: 980px }",
            "    main { min-width: 0 }",
            "    h1 { font-size: 24px }",
            "    h1 { margin: 0 0 8px }",
            "    h2 { font-size: 16px }",
            "    h2 { margin: 20px 0 8px }",
            "    h3 { font-size: 14px }",
            "    h3 { margin: 16px 0 6px }",
            "    p { margin: 0 0 12px }",
            "    .hero { margin: 0 0 24px }",
            "    .intro { color: #444 }",
            "    .intro { max-width: 720px }",
            "    aside { position: sticky }",
            "    aside { top: 24px }",
            "    ul { list-style: none }",
            "    ul { padding: 0 }",
            "    ul { margin: 8px 0 0 }",
            "    li { margin: 4px 0 }",
            "    a { color: inherit }",
            "    a { text-decoration: none }",
            "    a { border-bottom: 1px solid #ddd }",
            "    a:hover { border-bottom-color: #111 }",
            "    section { background: #fafafa }",
            "    section { border: 1px solid #eee }",
            "    section { border-radius: 6px }",
            "    section { padding: 16px }",
            "    section { margin: 0 0 16px }",
            "    table { border-collapse: collapse }",
            "    table { width: 100% }",
            "    table { margin: 8px 0 16px }",
            "    th, td { text-align: left }",
            "    th, td { padding: 4px 6px }",
            "    th, td { border-bottom: 1px solid #eee }",
            "    th, td { vertical-align: top }",
            "    th { font-weight: 600 }",
            "    .small { color: #666 }",
            "    .small { font-size: 12px }",
            "    code { background: #f6f6f6 }",
            "    code { padding: 2px 4px }",
            "    code { border-radius: 4px }",
            "    pre { background: #f6f6f6 }",
            "    pre { padding: 8px }",
            "    pre { overflow-x: auto }",
            "    @media (max-width: 900px) {",
            "      .layout { grid-template-columns: 1fr }",
            "      aside { position: static }",
            "    }",
            "  </style>",
            "</head>",
            "<body>",
            "<div class=\"layout\">",
            "  <aside>",
            "    <div class=\"small\">Assets</div>",
            f"    {index_html}",
            "  </aside>",
            "  <main>",
            "    <div class=\"hero\">",
            "      <div class=\"small\">Barefoot Data Platform</div>",
            "      <h1>Asset docs</h1>",
            "      <p class=\"intro\">Generated documentation for materialized assets. Run bdp docs after materialize.</p>",
            "    </div>",
            f"    {sections_html}",
            "  </main>",
            "</div>",
            "</body>",
            "</html>",
        ]
    )


def render_asset_section(
    asset: Asset,
    assets_root: Path,
    metadata: dict[str, list[str]],
    columns: list[tuple[str, str]],
    row_count: int,
    sample_columns: list[str],
    sample_values: list[tuple[object, ...]],
) -> str:
    description = asset.description
    rel_path = asset.path.relative_to(assets_root)
    meta_html = render_metadata_table(metadata)
    columns_html = render_columns_table(columns)
    sample_html = render_sample_table(sample_columns, sample_values)
    if description:
        description_html = f"<p>{html.escape(description)}</p>"
    else:
        description_html = "<div class=\"small\">No description.</div>"
    return "\n".join(
        [
            f"<section id=\"{html.escape(asset.key)}\">",
            f"  <h2>{html.escape(asset.key)}</h2>",
            f"  <div class=\"small\">{html.escape(asset.kind)} · {html.escape(rel_path.as_posix())}</div>",
            f"  {description_html}",
            "  <h3>Metadata</h3>",
            f"  {meta_html}",
            "  <h3>Columns</h3>",
            f"  {columns_html}",
            f"  <div class=\"small\">Rows: {row_count}</div>",
            "  <h3>Sample</h3>",
            f"  {sample_html}",
            "</section>",
        ]
    )


def render_metadata_table(metadata: dict[str, list[str]]) -> str:
    if not metadata:
        return "<div class=\"small\">No metadata.</div>"
    rows = []
    preferred = ["name", "schema", "description", "depends"]
    remaining = sorted(key for key in metadata if key not in preferred)
    for key in [*preferred, *remaining]:
        if key not in metadata:
            continue
        value_html = render_metadata_value(key, metadata[key])
        rows.append(
            "\n".join(
                [
                    "    <tr>",
                    f"      <td><code>asset.{html.escape(key)}</code></td>",
                    f"      <td>{value_html}</td>",
                    "    </tr>",
                ]
            )
        )
    body = "\n".join(rows)
    return "\n".join(
        [
            "  <table>",
            "    <thead>",
            "      <tr><th>Key</th><th>Value</th></tr>",
            "    </thead>",
            "    <tbody>",
            body,
            "    </tbody>",
            "  </table>",
        ]
    )


def render_columns_table(columns: list[tuple[str, str]]) -> str:
    if not columns:
        return "<div class=\"small\">No columns.</div>"
    rows = []
    for name, dtype in columns:
        rows.append(
            "\n".join(
                [
                    "    <tr>",
                    f"      <td>{html.escape(name)}</td>",
                    f"      <td>{html.escape(dtype)}</td>",
                    "    </tr>",
                ]
            )
        )
    body = "\n".join(rows)
    return "\n".join(
        [
            "  <table>",
            "    <thead>",
            "      <tr><th>Column</th><th>Type</th></tr>",
            "    </thead>",
            "    <tbody>",
            body,
            "    </tbody>",
            "  </table>",
        ]
    )


def render_sample_table(
    columns: list[str],
    rows: list[tuple[object, ...]],
) -> str:
    if not columns:
        return "<div class=\"small\">No sample available.</div>"
    if not rows:
        return "<div class=\"small\">No rows.</div>"
    head_cells = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(
            f"<td>{html.escape(format_value(value))}</td>" for value in row
        )
        body_rows.append(f"    <tr>{cells}</tr>")
    body_html = "\n".join(body_rows)
    return "\n".join(
        [
            "  <table>",
            "    <thead>",
            f"      <tr>{head_cells}</tr>",
            "    </thead>",
            "    <tbody>",
            body_html,
            "    </tbody>",
            "  </table>",
        ]
    )


def render_metadata_value(key: str, values: list[str]) -> str:
    if key == "depends":
        return render_depends_value(values)
    return html.escape(format_metadata_value(values))


def render_depends_value(values: list[str]) -> str:
    deps: list[str] = []
    for raw in values:
        if not raw:
            continue
        parts = [part.strip() for part in raw.split(",")]
        for part in parts:
            if not part:
                continue
            escaped = html.escape(part)
            deps.append(f"<a href=\"#{escaped}\">{escaped}</a>")
    return ", ".join(deps)


def format_metadata_value(values: list[str]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    return ", ".join(values)


def format_value(value: object) -> str:
    if value is None:
        return "null"
    return str(value)
