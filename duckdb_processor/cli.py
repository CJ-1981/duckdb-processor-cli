"""Command-line interface for DuckDB CSV Processor.

Parsed by :func:`main` and exposed via ``python -m duckdb_processor``.
"""
from __future__ import annotations

import argparse
import readline
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path

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
    ap.add_argument(
        "--output",
        "-o",
        type=str,
        nargs="?",
        const="",
        help="Save output to file (default: duckdb_output_YYYYMMDD_HHMMSS.txt if no filename specified)",
    )

    return ap


def _save_history_silent(history_file: Path) -> None:
    """Save readline history to file, ignoring errors.

    Args:
        history_file: Path to history file
    """
    try:
        readline.write_history_file(str(history_file))
    except (PermissionError, OSError):
        # Silently fail if we can't write history (e.g., read-only filesystem)
        pass


def interactive_repl(p: Processor) -> None:
    """Enhanced interactive SQL REPL with readline support.

    Features:
    - Arrow keys for cursor movement and inline editing
    - Command history (up/down arrows)
    - Special commands: \\schema, \\coverage, \\tables, \\help

    Special Commands:
    * ``EXIT`` / ``QUIT`` / ``\\q`` — quit the REPL.
    * ``\\schema`` — show column names and types.
    * ``\\coverage`` — show column fill rates.
    * ``\\tables`` — list all tables in database.
    * ``\\help`` — show help message.
    """
    print("\n── Interactive SQL REPL ─────────────────────────────────")
    print(
        f"  Table: '{p.table}'  |  Type \\help for commands  |  "
        r"Type EXIT to quit"
    )
    print("─" * 58)
    print("Tips: Use arrow keys for history and cursor movement")

    # Clear any existing readline state to prevent character retention
    readline.clear_history()

    # Setup readline for better keyboard support
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set show-all-if-ambiguous on")
    readline.parse_and_bind("set editing-mode emacs")

    # Initialize history file for persistent history across sessions
    history_file = Path.home() / ".duckdb_processor_history"
    try:
        readline.read_history_file(str(history_file))
    except (FileNotFoundError, PermissionError):
        # No history file yet or permission denied - start fresh
        pass

    while True:
        try:
            query = input("\nsql> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            # Save history before exiting
            _save_history_silent(history_file)
            break

        if not query:
            continue
        if query.upper() in ("EXIT", "QUIT", "\\Q"):
            print("Bye.")
            # Save history before exiting
            _save_history_silent(history_file)
            break
        if query == "\\schema":
            print(p.schema().to_string(index=False))
            continue
        if query == "\\coverage":
            print(p.coverage().to_string(index=False))
            continue
        if query == "\\tables":
            try:
                tables = p.sql("SHOW TABLES")
                print(tables.to_string(index=False))
            except Exception as e:
                print(f"  {e}")
            continue
        if query == "\\help":
            print("\nAvailable commands:")
            print("  \\schema   - Show column names and types")
            print("  \\coverage - Show column fill rates")
            print("  \\tables   - List all tables")
            print("  \\help     - Show this help message")
            print("  EXIT       - Exit REPL")
            print("\nKeyboard shortcuts:")
            print("  Arrow Up/Down - Navigate command history")
            print("  Arrow Left/Right - Move cursor in current line")
            print("  Ctrl+A - Move to beginning of line")
            print("  Ctrl+E - Move to end of line")
            print("\nHistory:")
            print(f"  History file: {history_file}")
            print("  Commands are saved automatically and restored across sessions")
            continue

        # Add to readline history after execution (successful or not)
        readline.add_history(query)

        try:
            result = p.sql(query)
            if result is not None and not result.empty:
                print(result.to_string(index=False))
            else:
                print("Query executed successfully (no results)")
        except Exception as e:
            print(f"  Error: {e}")

        # Auto-save history after each command
        _save_history_silent(history_file)


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

    # ── Handle output file redirection ────────────────────────
    if args.output is not None:
        capture_output_to_file(p, args.output)
        # Return early since output is already handled
        return p

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


def capture_output_to_file(p: Processor, output_file: str) -> None:
    """Capture processor output and write to file (plain text, no Rich formatting).

    Args:
        p: Processor instance
        output_file: Path to output file
    """
    from .formatters import SimpleFormatter

    # Generate default filename with timestamp if not specified
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"duckdb_output_{timestamp}.txt"

    output_path = Path(output_file)

    # Capture plain text output to file using SimpleFormatter
    old_formatter = p.formatter
    captured_output = StringIO()

    try:
        # Temporarily switch to SimpleFormatter for plain text output
        p.formatter = SimpleFormatter()

        # Capture to string
        old_stdout = sys.stdout
        sys.stdout = captured_output
        p.print_info()
        sys.stdout = old_stdout

    finally:
        p.formatter = old_formatter  # Restore original formatter

    # Write captured output to file
    output_path.write_text(captured_output.getvalue())

    # Print confirmation and show output on console using original formatter
    print(f"Output saved to: {output_path}")
    print()
    # Show the info banner on console with original formatting
    p.print_info()

