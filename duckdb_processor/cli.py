"""Command-line interface for DuckDB CSV Processor.

Parsed by :func:`main` and exposed via ``python -m duckdb_processor``.
"""
from __future__ import annotations

import argparse
import sys

from .analyzer import list_analyzers, run_analyzers
from .config import ProcessorConfig
from .formatters import (  # @MX:NOTE: Formatter integration for CLI output (REQ-004, REQ-008)
    OutputConfig,
    RichFormatter,
    SimpleFormatter,
)
from .loader import load
from .processor import Processor


def build_arg_parser() -> argparse.ArgumentParser:
    """Construct and return the CLI argument parser."""
    description = (
        "DuckDB CSV Processor — structured data analysis toolkit.\n"
        "Loads CSV (file/stdin), auto-detects format, and exposes\n"
        "a clean Processor API for business logic.\n"
    )
    epilog = (
        "Analyst workflow:\n"
        "  1. Load:         python -m duckdb_processor data.csv\n"
        "  2. Run analysis: python -m duckdb_processor data.csv --run demo\n"
        "  3. Interactive:  python -m duckdb_processor data.csv --interactive\n"
        "  4. List tools:   python -m duckdb_processor --list-analyzers\n"
    )
    ap = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    ap.add_argument("file", nargs="?", help="CSV file path (omit for stdin)")

    # Header options
    hg = ap.add_mutually_exclusive_group()
    hg.add_argument(
        "--header",
        dest="header",
        action="store_true",
        default=None,
        help="Force: first row is a header",
    )
    hg.add_argument(
        "--no-header",
        dest="header",
        action="store_false",
        help="Force: no header row",
    )

    # K:V options
    kg = ap.add_mutually_exclusive_group()
    kg.add_argument(
        "--kv",
        dest="kv",
        action="store_true",
        default=None,
        help="Force: key:value pair format",
    )
    kg.add_argument(
        "--no-kv",
        dest="kv",
        action="store_false",
        help="Force: plain flat CSV",
    )

    ap.add_argument(
        "--col-names",
        type=str,
        default=None,
        help="Comma-separated column names (when --no-header and --no-kv)",
    )
    ap.add_argument(
        "--table", default="data", help="DuckDB table name (default: data)"
    )
    ap.add_argument(
        "--run",
        type=str,
        default=None,
        help="Run named analyzer(s), comma-separated (e.g. --run demo,step2)",
    )
    ap.add_argument(
        "--list-analyzers",
        action="store_true",
        help="List all registered analyzers and exit",
    )
    ap.add_argument(
        "--interactive",
        action="store_true",
        help="Drop into interactive SQL REPL after loading",
    )

    # Output formatting options (REQ-062, REQ-064, REQ-065)
    ap.add_argument(
        "--format",
        type=str,
        default="rich",
        choices=["rich", "simple"],
        help="Output format: rich (colored, styled) or simple (plain text)",
    )
    ap.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored output (REQ-064)",
    )
    ap.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress indicators (REQ-018)",
    )

    return ap


def interactive_repl(p: Processor) -> None:
    """Minimal interactive SQL REPL.

    Special commands:
    * ``EXIT`` / ``QUIT`` / ``\\q`` — quit the REPL.
    * ``\\schema`` — show column names and types.
    * ``\\coverage`` — show column fill rates.
    * Everything else is executed as SQL.
    """
    print("\n── Interactive SQL REPL ─────────────────────────────────")
    print(
        f"  Table: '{p.table}'  |  Type EXIT to quit  |  "
        r"\schema for columns"
    )
    print("─" * 58)

    while True:
        try:
            query = input("\nsql> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not query:
            continue
        if query.upper() in ("EXIT", "QUIT", "\\Q"):
            print("Bye.")
            break
        if query == "\\schema":
            print(p.schema().to_string(index=False))
            continue
        if query == "\\coverage":
            print(p.coverage().to_string(index=False))
            continue

        try:
            print(p.sql(query).to_string(index=False))
        except Exception as e:
            print(f"  {e}")


def main(argv: list[str] | None = None) -> Processor | None:
    """CLI entry point — parse args, load data, optionally run analyzers.

    Parameters
    ----------
    argv:
        Argument list (defaults to ``sys.argv[1:]``).  Useful for
        testing without touching ``sys.argv``.

    Returns
    -------
    Processor or None
        The loaded processor (or ``None`` if ``--list-analyzers`` was
        requested and exited early).
    """
    args = build_arg_parser().parse_args(argv)

    # ── --list-analyzers: show registry and exit ──────────────
    if args.list_analyzers:
        analyzers = list_analyzers()
        if not analyzers:
            print("No analyzers registered.")
        else:
            print("Available analyzers:")
            for a in analyzers:
                print(f"  {a['name']:20s} {a['description']}")
        return None

    # ── Build config from CLI args ────────────────────────────
    col_names = (
        [c.strip() for c in args.col_names.split(",")] if args.col_names else None
    )

    config = ProcessorConfig(
        file=args.file,
        header=args.header,
        kv=args.kv,
        col_names=col_names,
        table=args.table,
        interactive=args.interactive,
    )

    # ── Create output formatter (REQ-004, REQ-008) ───────────────
    output_config = OutputConfig.from_args(args)
    if output_config.formatter_type == "rich":
        formatter = RichFormatter(output_config.__dict__)
    else:
        formatter = SimpleFormatter(output_config.__dict__)

    # ── Load data ─────────────────────────────────────────────
    try:
        p = load(config, formatter=formatter)  # @MX:NOTE: Pass formatter to loader (REQ-011)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── Show info banner ──────────────────────────────────────
    p.print_info()

    # ── Run requested analyzers ───────────────────────────────
    if args.run:
        names = [n.strip() for n in args.run.split(",")]
        run_analyzers(p, names)

    # ── Interactive REPL ──────────────────────────────────────
    if args.interactive:
        interactive_repl(p)

    return p
