"""Command-line interface for DuckDB CSV Processor.

Parsed by :func:`main` and exposed via ``python -m duckdb_processor``.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path

from .analyzer import get_analyzer, list_analyzers, run_analyzers
from .config import ProcessorConfig
from .formatters import (  # @MX:NOTE: Formatter integration for CLI output (REQ-004, REQ-008)
    OutputConfig,
    RichFormatter,
    SimpleFormatter,
)
from .loader import load
from .processor import Processor
from .repl import interactive_repl
from .utils import prompt_file_dialog


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
        nargs="+",
        help="Column names, comma or space separated (when --no-header and --no-kv)",
    )
    ap.add_argument(
        "--table", default="data", help="DuckDB table name (default: data)"
    )
    ap.add_argument(
        "--run",
        nargs="+",
        help="Run named analyzer(s), comma or space separated (e.g. --run demo,step2)",
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
    ap.add_argument(
        "--export-format",
        type=str,
        default=None,
        choices=["json", "xlsx", "parquet", "csv"],
        help="Export query results to file format (json, xlsx, parquet, csv)",
    )

    return ap



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

    # ── If no file given and running interactively, offer a file dialog ───
    if args.file is None and sys.stdin.isatty():
        selected = prompt_file_dialog()
        if selected:
            args.file = selected
        else:
            print("No input file selected. Exiting.")
            sys.exit(0)

    # ── Build config from CLI args ────────────────────────────
    col_names = None
    if args.col_names:
        col_names = []
        for item in args.col_names:
            if "," in item:
                col_names.extend([c.strip() for c in item.split(",") if c.strip()])
            else:
                col_names.append(item.strip())

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
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        print(f"Hint: Check that the file path is correct and the file exists.", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    # ── Handle output file redirection ────────────────────────
    output_file = None
    if args.output is not None:
        # Generate default filename with timestamp if not specified
        if not args.output:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"duckdb_output_{timestamp}.txt"
        else:
            output_file = args.output
        capture_output_to_file(p, output_file)

    # ── Show info banner (skip if output was redirected to file) ──────
    if args.output is None:
        p.print_info()

    # ── Run requested analyzers ───────────────────────────────
    if args.run:
        names = []
        for item in args.run:
            if "," in item:
                names.extend([n.strip() for n in item.split(",") if n.strip()])
            else:
                # Skip if it looks like a file extension
                if not any(item.lower().endswith(ext) for ext in ['.csv', '.tsv', '.parquet', '.xlsx', '.json', '.txt']):
                    names.append(item.strip())

        # Filter out any remaining files that might have slipped through
        names = [n for n in names if not '.' in n or n.startswith('-')]

        # ── Run analyzers and export results individually ──────────
        for name in names:
            try:
                analyzer = get_analyzer(name)
            except KeyError as e:
                print(f"\n❌ Error: {e}", file=sys.stderr)
                print(f"\n💡 Tip: Use --list-analyzers to see all available analyzers", file=sys.stderr)
                print(f"   Example: python -m duckdb_processor data.csv --list-analyzers\n", file=sys.stderr)
                continue

            desc = analyzer.description or ""
            print(f"\n{'─' * 58}")
            print(f"  [{name}] {desc}")
            print(f"{'─' * 58}")

            analyzer.run(p)

            # ── Export after each analyzer if requested ──────────
            if args.export_format and p.last_result is not None and not p.last_result.empty:
                # Generate export filename with analyzer name
                if output_file:
                    # Use output filename as base, add analyzer name, change extension
                    base_name = Path(output_file).stem
                    export_filename = f"{base_name}_{name}.{args.export_format}"
                else:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    export_filename = f"duckdb_export_{name}_{timestamp}.{args.export_format}"

                try:
                    if args.export_format == "csv":
                        p.last_result.to_csv(export_filename, index=False)
                    elif args.export_format == "json":
                        p.last_result.to_json(export_filename, orient="records", indent=2)
                    elif args.export_format == "parquet":
                        p.last_result.to_parquet(export_filename, index=False)
                    elif args.export_format == "xlsx":
                        try:
                            import openpyxl  # noqa: F401
                        except ImportError:
                            print("Error: openpyxl not installed. Install with: pip install openpyxl")
                            print("Or install with: pip install duckdb-processor[export]")
                            return p
                        p.last_result.to_excel(export_filename, index=False, engine="openpyxl")
                    print(f"✓ Results exported → {export_filename}")
                except Exception as e:
                    print(f"Error exporting results: {e}")

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
