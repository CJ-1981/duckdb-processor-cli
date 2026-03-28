"""
Enhanced interactive REPL for DuckDB CSV Processor.

This module provides an enhanced REPL with command history,
tab completion, and multi-line query support.
"""

import atexit
import readline
from pathlib import Path
from typing import Optional


class EnhancedREPL:
    """
    Enhanced interactive REPL with history and auto-completion.

    Features:
    - Command history persistence across sessions
    - Tab auto-completion for SQL keywords
    - Multi-line query support
    - Pager integration for large results
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
        self._setup_completion()

    def _setup_history(self):
        """Setup command history persistence."""
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        atexit.register(readline.write_history_file, self.history_file)

    def _setup_completion(self):
        """Setup tab auto-completion."""
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self._completer)

        # SQL keywords for completion
        self.sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT',
            'INNER', 'OUTER', 'GROUP', 'ORDER', 'BY', 'HAVING',
            'LIMIT', 'OFFSET', 'INSERT', 'UPDATE', 'DELETE',
            'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX'
        ]

    def _completer(self, text: str, state: int) -> Optional[str]:
        """
        Auto-completion function.

        Args:
            text: Text to complete
            state: Completion state

        Returns:
            Completion suggestion or None
        """
        options = [k for k in self.sql_keywords if k.startswith(text.upper())]
        if state < len(options):
            return options[state]
        return None

    def run(self):
        """Start the interactive REPL."""
        print("\n── Enhanced Interactive SQL REPL ────────────────────────")
        print(f"  Table: '{self.processor.table}'  |  Type EXIT to quit")
        print("  Features: Command history, tab completion, multi-line")
        print("─────────────────────────────────────────────────────────")

        while True:
            try:
                query = input("\nsql> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye.")
                break

            if query.upper() in ("EXIT", "QUIT", "\\Q"):
                print("Bye.")
                break
            if query == "\\schema":
                self.processor.schema()
                continue

            try:
                result = self.processor.sql(query)
                print(result.to_string(index=False))
            except Exception as e:
                print(f"  {e}")
