from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from typing import Any

DEFAULT_SEARCH_FIELDS = ("uuid", "data_type", "site", "species", "inlet", "start_date", "end_date")


def _add_repeated_search_value(search_kwargs: dict[str, Any], key: str, value: str) -> None:
    """Add a search value, preserving repeated keys as OR-list searches."""
    existing = search_kwargs.get(key)
    if existing is None:
        search_kwargs[key] = value
    elif isinstance(existing, list):
        existing.append(value)
    else:
        search_kwargs[key] = [existing, value]


def _parse_search_terms(terms: Sequence[str]) -> dict[str, Any]:
    """Parse ``KEY=VALUE`` CLI search terms into OpenGHG search kwargs."""
    search_kwargs: dict[str, Any] = {}

    for term in terms:
        key, separator, value = term.partition("=")
        if separator == "" or key == "":
            raise ValueError(f"Search terms must use KEY=VALUE format: {term!r}")
        _add_repeated_search_value(search_kwargs, key.replace("-", "_"), value)

    return search_kwargs


def _parse_search_fields(fields: str | None) -> list[str]:
    """Parse comma-separated output fields."""
    if fields is None:
        return list(DEFAULT_SEARCH_FIELDS)
    parsed_fields = [field.strip() for field in fields.split(",") if field.strip()]
    if not parsed_fields:
        raise ValueError("Please specify at least one field.")
    return parsed_fields


def _search_kwargs_from_args(args: argparse.Namespace) -> dict[str, Any]:
    """Build OpenGHG search kwargs from parsed CLI args."""
    search_kwargs = _parse_search_terms(args.terms)

    for key in ("store", "start_date", "end_date"):
        value = getattr(args, key)
        if value is not None:
            search_kwargs[key] = value

    if args.data_type:
        search_kwargs["data_type"] = args.data_type[0] if len(args.data_type) == 1 else args.data_type

    search_kwargs["add_new_store"] = False
    return search_kwargs


def _metadata_rows(
    metadata: dict[str, dict[str, Any]], fields: Sequence[str], limit: int | None
) -> list[dict[str, Any]]:
    """Convert SearchResults metadata into selected CLI output rows."""
    rows: list[dict[str, Any]] = []
    for uuid, values in metadata.items():
        if limit is not None and len(rows) >= limit:
            break
        row = {field: uuid if field == "uuid" else values.get(field, "") for field in fields}
        rows.append(row)
    return rows


def _print_search_table(rows: Sequence[dict[str, Any]], fields: Sequence[str], total: int) -> None:
    """Print search rows as a Rich table."""
    from rich.console import Console
    from rich.table import Table

    console = Console()
    if total == 0:
        console.print("No results found.")
        return

    table = Table(title=f"OpenGHG search results ({len(rows)} of {total})")
    for field in fields:
        table.add_column(field)

    for row in rows:
        table.add_row(*(str(row.get(field, "")) for field in fields))

    console.print(table)


def _run_search_command(args: argparse.Namespace, search_func: Callable[..., Any] | None = None) -> None:
    """Run the search subcommand."""
    actual_search_func = search_func
    if search_func is None:
        from openghg.retrieve import search as _search

        actual_search_func = _search

    assert actual_search_func is not None
    fields = _parse_search_fields(args.fields)
    search_kwargs = _search_kwargs_from_args(args)
    results = actual_search_func(**search_kwargs)
    metadata = results.metadata
    total = len(metadata)
    rows = _metadata_rows(metadata=metadata, fields=fields, limit=args.limit)

    if args.json:
        print(json.dumps({"count": len(rows), "total": total, "results": rows}, indent=2, default=str))
    else:
        _print_search_table(rows=rows, fields=fields, total=total)


def _build_parser() -> argparse.ArgumentParser:
    """Build the OpenGHG command line parser."""
    parser = argparse.ArgumentParser(
        prog="OpenGHG CLI",
        description="The OpenGHG Command Line Interface helps you get OpenGHG setup on your local machine.",
        epilog="Text at the bottom of help",
    )
    parser.add_argument(
        "--default-config", action="store_true", help="Get OpenGHG setup with default a configuration"
    )
    parser.add_argument("--quickstart", action="store_true", help="Run the quickstart setup process")
    parser.add_argument(
        "--register-store",
        nargs="+",
        metavar=("STORE_NAME", "STORE_PATH"),
        help=(
            "Register a new data store. "
            "You can specify just a path, or both a store name and path. "
            "Examples:\n"
            "  --register-store /path/to/store\n"
            "  --register-store my_store /path/to/store"
        ),
    )
    parser.add_argument("--version", action="store_true", help="Print the version information about OpenGHG")

    subparsers = parser.add_subparsers(dest="command")
    search_parser = subparsers.add_parser("search", help="Search OpenGHG object store metadata")
    search_parser.add_argument("terms", nargs="*", metavar="KEY=VALUE", help="Metadata search term")
    search_parser.add_argument("--store", help="Readable store name or direct object store path")
    search_parser.add_argument(
        "--data-type",
        action="append",
        help="Data type to search. Repeat this option to search multiple data types.",
    )
    search_parser.add_argument("--start-date", help="Start date for date-overlap filtering")
    search_parser.add_argument("--end-date", help="End date for date-overlap filtering")
    search_parser.add_argument("--fields", help="Comma-separated fields to print")
    search_parser.add_argument("--limit", type=int, default=20, help="Maximum number of results to print")
    search_parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")

    return parser


def cli(argv: Sequence[str] | None = None) -> None:
    from openghg.util._user import create_config, handle_direct_store_path
    from openghg.util._versions import show_versions

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "search":
        _run_search_command(args)
    elif args.default_config:
        create_config(silent=True)
    elif args.quickstart:
        create_config()
    elif args.version:
        show_versions()
    elif args.register_store:
        if len(args.register_store) == 1:
            store_name = None
            store_path = args.register_store[0]
        elif len(args.register_store) == 2:
            # Name and path provided
            store_name, store_path = args.register_store
        else:
            raise ValueError("Too many arguments for --register-store.")

        handle_direct_store_path(path=store_path, name=store_name, add_new_store=True)
    else:
        parser.print_help()
