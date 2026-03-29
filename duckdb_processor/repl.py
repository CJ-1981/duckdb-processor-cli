"""
Enhanced interactive REPL for DuckDB CSV Processor.

This module provides an enhanced REPL with command history,
tab completion, multi-line query support, and proper history management.
"""

import atexit
import readline
import sys
from pathlib import Path
from typing import Optional


def _save_history_silent(history_file: Path) -> None:
    """Save readline history to file, ignoring errors.

    Args:
        history_file: Path to history file
    """
    try:
        readline.write_history_file(str(history_file))
    except (PermissionError, OSError):
        pass


class EnhancedREPL:
    """
    Enhanced interactive REPL with history and auto-completion.

    Features:
    - Command history persistence across sessions
    - Tab auto-completion for SQL keywords
    - Multi-line query support
    """

    def __init__(self, processor):
        """
        Initialize enhanced REPL.

        Args:
            processor: DuckDB Processor instance
        """
        self.processor = processor
        self.history_file = Path.home() / '.duckdb_processor_history'
        self._setup_history()

        # Clear any existing readline state to prevent character retention
        readline.clear_history()

        self._setup_completion()

    def _setup_history(self):
        """Setup command history persistence."""
        try:
            readline.read_history_file(str(self.history_file))
        except (FileNotFoundError, PermissionError):
            pass

    def _save_history(self):
        _save_history_silent(self.history_file)

    def _setup_completion(self):
        """Setup tab auto-completion."""
        readline.parse_and_bind("tab: complete")
        readline.parse_and_bind("set show-all-if-ambiguous on")
        readline.parse_and_bind("set editing-mode emacs")
        readline.set_completer(self._completer)

        # SQL keywords for completion
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT',
            'INNER', 'OUTER', 'GROUP', 'ORDER', 'BY', 'HAVING',
            'LIMIT', 'OFFSET', 'INSERT', 'UPDATE', 'DELETE',
            'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'WITH', 'PRAGMA'
        ]

    def _completer(self, text: str, state: int) -> Optional[str]:
        """Auto-completion function."""
        options = [k for k in self.sql_keywords if k.startswith(text.upper())]
        if state < len(options):
            return options[state]
        return None

    def _print_help(self):
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
        print("\nKeyboard shortcuts:")
        print("  Arrow Up/Down - Navigate command history")
        print("  Ctrl+A - Move to beginning of line")
        print("  Ctrl+E - Move to end of line")
        print("\nHistory:")
        print(f"  History file: {self.history_file}")

    def _handle_special_commands(self, line: str) -> bool:
        """Handle \\commands. Returns True if handled, False otherwise."""
        if line == "\\schema":
            print(self.processor.schema().to_string(index=False))
            return True
        elif line == "\\coverage":
            print(self.processor.coverage().to_string(index=False))
            return True
        elif line == "\\tables":
            try:
                tables = self.processor.sql("SHOW TABLES")
                print(tables.to_string(index=False))
            except Exception as e:
                print(f"  {e}")
            return True
        elif line == "\\help":
            self._print_help()
            return True
        elif line.startswith("\\export"):
            parts = line.split(maxsplit=3)
            if len(parts) < 3:
                print("Usage: \\export <filename> <format>")
                print("Formats: json, csv, xlsx, parquet")
            else:
                _, filename, format_type = parts
                try:
                    if self.processor.last_result is None or self.processor.last_result.empty:
                        print("  No query result to export. Run a query first.")
                    else:
                        self.processor.export(filename, format_type)
                except Exception as e:
                    print(f"  Error: {e}")
            return True
        return False

    def run(self):
        """Start the interactive REPL."""
        print("\n── Enhanced Interactive SQL REPL ─────────────────────────")
        print(f"  Table: '{self.processor.table}'  |  Type \\help for commands  |  Type EXIT to quit")
        print("──────────────────────────────────────────────────────────")
        print("Tips: Use arrow keys for history and cursor movement")
        print("      End multi-line queries with semicolon (;) or empty line")

        while True:
            query_lines = []
            prompt = "sql>"

            while True:
                try:
                    line = input(f"\n{prompt} ").rstrip()
                except (EOFError, KeyboardInterrupt):
                    if query_lines:
                        print("\nQuery cancelled.")
                        break
                    else:
                        print("\nBye.")
                        self._save_history()
                        return

                if not query_lines and line.upper() in ("EXIT", "QUIT", "\\Q"):
                    print("Bye.")
                    self._save_history()
                    return

                if not query_lines and line.startswith("\\"):
                    self._handle_special_commands(line)
                    break

                if not line:
                    if query_lines:
                        break
                    else:
                        continue

                query_lines.append(line)
                query_so_far = " ".join(query_lines)

                if line.endswith(";"):
                    break
                
                upper_query = query_so_far.upper().strip()
                complete_starts = ("SELECT ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "ALTER ", "WITH ", "PRAGMA ")
                
                if upper_query.endswith(";") or (
                    any(upper_query.startswith(s) for s in complete_starts)
                    and not any(upper_query.endswith(w) for w in ("AND", "OR", "WHERE", "WHEN", "CASE"))
                ):
                    if ";" in upper_query:
                        break

                prompt = "...>"

            if not query_lines:
                self._save_history()
                continue

            query = " ".join(query_lines)
            if query.endswith(";"):
                query = query[:-1].strip()

            readline.add_history(query)

            try:
                result = self.processor.sql(query)
                if result is not None and not result.empty:
                    print(result.to_string(index=False))
                else:
                    print("Query executed successfully (no results)")
            except Exception as e:
                print(f"  Error: {e}")

            self._save_history()

def interactive_repl(p):
    """Entry point for the REPL."""
    repl = EnhancedREPL(p)
    repl.run()
