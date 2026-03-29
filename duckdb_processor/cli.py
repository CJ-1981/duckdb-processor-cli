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

from .analyzer import get_analyzer, list_analyzers, run_analyzers
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


def _prompt_file_dialog() -> str | None:
    """Ask the user if they want to open a file dialog, then show one if yes.

    Only called when no input file is provided and stdin is an interactive
    terminal (not a pipe).  Returns the selected file path, or ``None`` if
    the user declines or no file is chosen.
    """
    try:
        answer = input("No input file specified. Open file dialog? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return None

    if answer not in ("y", "yes"):
        return None

    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()          # Hide the empty root window
        root.attributes("-topmost", True)  # Bring dialog to front
        selected = filedialog.askopenfilename(
            title="Select input file",
            filetypes=[
                ("CSV files", "*.csv"),
                ("TSV files", "*.tsv"),
                ("Parquet files", "*.parquet"),
                ("All files", "*.*"),
            ],
        )
        root.destroy()
        return selected if selected else None
    except Exception as exc:  # tkinter may be unavailable in headless envs
        print(f"File dialog unavailable: {exc}", file=sys.stderr)
        print("Hint: Pass the file path as an argument, e.g.  duckdb-processor data.csv",
              file=sys.stderr)
        return None


def interactive_repl(p: Processor) -> None:
    """Enhanced interactive SQL REPL with readline support and multi-line queries.

    Features:
    - Arrow keys for cursor movement and inline editing
    - Command history (up/down arrows)
    - Multi-line SQL queries (semicolon or empty line to terminate)
    - Special commands: \\schema, \\coverage, \\tables, \\export, \\help

    Special Commands:
    * ``EXIT`` / ``QUIT`` / ``\\q`` — quit the REPL.
    * ``\\schema`` — show column names and types.
    * ``\\coverage`` — show column fill rates.
    * ``\\tables`` — list all tables in database.
    * ``\\export <file> <format>`` — export last query result.
    * ``\\help`` — show help message.

    Multi-line Queries:
    * End with semicolon (;) to execute
    * Or press Enter on empty line to execute
    * Continuation prompt (...>) shows incomplete query
    """
    print("\n── Interactive SQL REPL ─────────────────────────────────")
    print(
        f"  Table: '{p.table}'  |  Type \\help for commands  |  "
        r"Type EXIT to quit"
    )
    print("─" * 58)
    print("Tips: Use arrow keys for history and cursor movement")
    print("      End multi-line queries with semicolon (;) or empty line")

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
        query_lines = []
        prompt = "sql>"

        while True:
            try:
                line = input(f"\n{prompt} ").rstrip()
            except (EOFError, KeyboardInterrupt):
                if query_lines:
                    # User cancelled mid-query
                    print("\nQuery cancelled.")
                    break
                else:
                    # User wants to exit
                    print("\nBye.")
                    _save_history_silent(history_file)
                    return

            # Check for exit commands on first line
            if not query_lines and line.upper() in ("EXIT", "QUIT", "\\Q"):
                print("Bye.")
                _save_history_silent(history_file)
                return

            # Check for special commands (only on first line)
            if not query_lines and line.startswith("\\"):
                if line == "\\schema":
                    print(p.schema().to_string(index=False))
                elif line == "\\coverage":
                    print(p.coverage().to_string(index=False))
                elif line == "\\tables":
                    try:
                        tables = p.sql("SHOW TABLES")
                        print(tables.to_string(index=False))
                    except Exception as e:
                        print(f"  {e}")
                elif line == "\\help":
                    print("\nAvailable commands:")
                    print("  \\schema   - Show column names and types")
                    print("  \\coverage - Show column fill rates")
                    print("  \\tables   - List all tables")
                    print("  \\export <file> <format> - Export last query result to file")
                    print("                        Formats: json, csv, xlsx, parquet")
                    print("                        Example: \\export results.json xlsx")
                    print("  \\help     - Show this help message")
                    print("  EXIT       - Exit REPL")
                    print("\nMulti-line queries:")
                    print("  End with semicolon (;) or press Enter on empty line")
                    print("  Example: SELECT *")
                    print("           FROM data")
                    print("           WHERE price > 100;")
                    print("\nKeyboard shortcuts:")
                    print("  Arrow Up/Down - Navigate command history")
                    print("  Arrow Left/Right - Move cursor in current line")
                    print("  Ctrl+A - Move to beginning of line")
                    print("  Ctrl+E - Move to end of line")
                    print("\nHistory:")
                    print(f"  History file: {history_file}")
                    print("  Commands are saved automatically and restored across sessions")
                elif line.startswith("\\export"):
                    parts = line.split(maxsplit=3)
                    if len(parts) < 3:
                        print("Usage: \\export <filename> <format>")
                        print("Formats: json, csv, xlsx, parquet")
                        print("Example: \\export results.json xlsx")
                    else:
                        _, filename, format_type = parts
                        try:
                            if p.last_result is None or p.last_result.empty:
                                print("  No query result to export. Run a query first.")
                            elif format_type == "csv":
                                p.last_result.to_csv(filename, index=False)
                                print(f"Exported \u2192 {filename}")
                            elif format_type == "json":
                                p.last_result.to_json(filename, orient="records", indent=2)
                                print(f"Exported \u2192 {filename}")
                            elif format_type == "parquet":
                                p.last_result.to_parquet(filename, index=False)
                                print(f"Exported \u2192 {filename}")
                            elif format_type == "xlsx":
                                try:
                                    import openpyxl  # noqa: F401
                                except ImportError:
                                    print("  Error: openpyxl not installed. Install with: pip install openpyxl")
                                    continue
                                p.last_result.to_excel(filename, index=False, engine="openpyxl")
                                print(f"Exported \u2192 {filename}")
                            else:
                                print(f"  Error: Unsupported format '{format_type}'")
                        except Exception as e:
                            print(f"  Error: {e}")
                break  # Exit inner loop for special commands

            # Handle empty lines
            if not line:
                if query_lines:
                    # Empty line terminates multi-line query
                    break
                else:
                    # Empty input at prompt - just continue
                    continue

            # Add line to query
            query_lines.append(line)

            # Check if query is complete
            query_so_far = " ".join(query_lines)
            # Check for semicolon at end
            if line.endswith(";"):
                break
            # Check for complete single-statement keywords
            upper_query = query_so_far.upper().strip()
            complete_starts = (
                "SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ",
                "ALTER ", "GRANT ", "REVOKE ", "SHOW ", "DESCRIBE ", "EXPLAIN ",
                "WITH ", "PRAGMA "
            )
            # If query looks complete and doesn't end with incomplete syntax
            if upper_query.endswith(";") or (
                any(upper_query.startswith(s) for s in complete_starts)
                and not any(upper_query.endswith(w) for w in ("AND", "OR", "WHERE", "WHEN", "CASE"))
            ):
                # Could be complete, but continue reading if semicolon not found
                if ";" in upper_query:
                    break

            # Continue reading multi-line query
            prompt = "...>"

        # Skip if no query to execute (e.g., special commands)
        if not query_lines:
            # Save history and continue to next iteration
            _save_history_silent(history_file)
            continue

        # Join query lines and remove trailing semicolon
        query = " ".join(query_lines)
        if query.endswith(";"):
            query = query[:-1].strip()

        # Add to readline history
        readline.add_history(query)

        # Execute query
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

    # ── If no file given and running interactively, offer a file dialog ───
    if args.file is None and sys.stdin.isatty():
        selected = _prompt_file_dialog()
        if selected:
            args.file = selected
        else:
            print("No input file selected. Exiting.")
            sys.exit(0)

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
