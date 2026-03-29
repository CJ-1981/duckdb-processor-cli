"""
Rich library formatter for enhanced terminal output.

This module provides the RichFormatter class that integrates
the Rich library for professional terminal formatting.
"""

import sys
from typing import Any, Dict, Optional

import pandas as pd

from .base import BaseFormatter
from .utils import truncate_dataframe


class RichFormatter(BaseFormatter):
    """
    Rich library-based formatter with colors and progress bars.

    This formatter provides enhanced terminal output including:
    - Type-aware table formatting
    - Color-coded messages
    - Progress bars
    - Terminal width detection
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize RichFormatter.

        Args:
            config: Optional configuration dictionary
        """
        try:
            from rich.console import Console
            self.console = Console()
            self.rich_available = True
        except ImportError:
            self.console = None
            self.rich_available = False

        self.color_enabled = config.get('color_enabled', True) if config else True
        self.max_rows = config.get('max_rows', 50) if config else 50

    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """
        Display DataFrame with type-aware styling.

        Args:
            df: DataFrame to display
            max_rows: Maximum rows to display (None for unlimited)
        """
        if not self.rich_available:
            # Fallback to simple format
            from .simple_formatter import SimpleFormatter
            SimpleFormatter().format_dataframe(df, max_rows)
            return

        from rich.table import Table

        table = Table(show_header=True, header_style="bold magenta")

        # Add columns with type-aware styling
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                table.add_column(col, justify="right", style="cyan")
            else:
                table.add_column(col, style="white")

        # Add rows using shared truncation utility
        max_display_rows = max_rows or self.max_rows
        display_df = truncate_dataframe(df, max_display_rows)

        for _, row in display_df.iterrows():
            table.add_row(*[str(x) for x in row.tolist()])

        self.console.print(table)

    def format_info(self, metadata: Dict[str, Any]) -> None:
        """
        Display dataset information banner.

        Args:
            metadata: Dictionary with source, rows, columns info
        """
        if not self.rich_available:
            from .simple_formatter import SimpleFormatter
            SimpleFormatter().format_info(metadata)
            return

        from rich.panel import Panel

        info_text = "\n".join([
            f"Source: {metadata.get('source', 'N/A')}",
            f"Rows: {metadata.get('rows', 'N/A')}",
            f"Columns: {', '.join(metadata.get('columns', []))}"
        ])

        panel = Panel(info_text, title="Dataset Info", border_style="blue")
        self.console.print(panel)

    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """
        Display color-coded error message.

        Args:
            error: Exception to display
            context: Context where error occurred
            severity: Error level (ERROR, WARNING, INFO)
        """
        if not self.rich_available or not self.color_enabled:
            from .simple_formatter import SimpleFormatter
            SimpleFormatter().format_error(error, context, severity)
            return

        color_map = {
            "ERROR": "red",
            "WARNING": "yellow",
            "INFO": "blue"
        }
        color = color_map.get(severity, "white")

        # Rich Console doesn't support file parameter, use stderr console for errors
        if severity == "ERROR":
            from rich.console import Console
            error_console = Console(file=sys.stderr)
            error_console.print(f"[{color}]{severity}: {error}[/{color}]")
        else:
            self.console.print(f"[{color}]{severity}: {error}[/{color}]")

    def format_progress(self, message: str, current: int, total: int) -> None:
        """
        Display progress bar.

        Args:
            message: Progress message
            current: Current progress value
            total: Total value for completion
        """
        if not self.rich_available:
            from .simple_formatter import SimpleFormatter
            SimpleFormatter().format_progress(message, current, total)
            return

        from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn

        with Progress(
            SpinnerColumn(),
            f"[progress.description]{message}",
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task(message, total=total)
            progress.update(task, completed=current)
