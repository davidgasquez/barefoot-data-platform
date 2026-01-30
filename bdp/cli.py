from __future__ import annotations

import argparse
import sys

from bdp.materialize import materialize

SHORT_HELP = """Barefoot Data Portal CLI.

Usage:
  bdp materialize --all
  bdp materialize ASSET [ASSET...]

Run "bdp --help" for more.
"""


def _materialize(args: argparse.Namespace) -> None:
    if not args.all and not args.assets:
        raise SystemExit(
            "Usage: bdp materialize [--all] [ASSET...]\n"
            "\n"
            "Pass --all or asset names. "
            "Run 'bdp materialize --help' for more."
        )
    materialize(
        args.assets,
        all_assets=args.all,
    )


def main() -> None:
    if len(sys.argv) == 1:
        print(SHORT_HELP)
        raise SystemExit(0)

    parser = argparse.ArgumentParser(
        prog="bdp",
        description="Barefoot Data Portal CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="Materialize assets into DuckDB.",
    )
    materialize_parser.add_argument(
        "assets",
        nargs="*",
        metavar="ASSET",
        help="Asset names to materialize.",
    )
    materialize_parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Materialize all assets.",
    )
    materialize_parser.set_defaults(func=_materialize)

    args = parser.parse_args()
    args.func(args)
