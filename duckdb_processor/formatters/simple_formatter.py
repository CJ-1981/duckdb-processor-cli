"""
Simple formatter for legacy output compatibility.

This module provides the SimpleFormatter class that preserves
the exact legacy output format using pandas string representation.
"""

import sys
from typing import Any, Dict, Optional

import pandas as pd

from .base import BaseFormatter
from .utils import calculate_progress_percent, truncate_dataframe


class SimpleFormatter(BaseFormatter):
    """
    Legacy compatibility wrapper using pandas string representation.

    This formatter maintains exact backward compatibility with existing
    output by using pandas DataFrame.to_string() method.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize SimpleFormatter.

        Args:
            config: Optional configuration dictionary
        """
        self.max_rows = config.get('max_rows', 50) if config else 50

    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """
        Display DataFrame using pandas to_string() format.

        Args:
            df: DataFrame to display
            max_rows: Maximum rows to display (None for unlimited)
        """
        max_display_rows = max_rows or self.max_rows
        display_df = truncate_dataframe(df, max_display_rows)
        print(display_df.to_string(index=False))

    def format_info(self, metadata: Dict[str, Any]) -> None:
        """
        Display dataset information using legacy format.

        Args:
            metadata: Dictionary with source, rows, columns info
        """
        from .terminal import print_banner, print_section_divider

        # Build info lines (matching legacy format)
        info_lines = [
            f"  Source      : {metadata.get('source', 'N/A')}",
            f"  Rows loaded : {metadata.get('rows', 'N/A')}",
            f"  Columns     : {', '.join(metadata.get('columns', []))}",
        ]

        # Print banner
        print_banner("DuckDB CSV Processor")
        for line in info_lines:
            print(line)
        print_section_divider()

    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """
        Display error message in legacy format.

        Args:
            error: Exception to display
            context: Context where error occurred
            severity: Error level (ERROR, WARNING, INFO)
        """
        print(f"{severity}: {error}", file=sys.stderr)

    def format_progress(self, message: str, current: int, total: int) -> None:
        """
        Display simple progress indicator.

        Args:
            message: Progress message
            current: Current progress value
            total: Total value for completion
        """
        percent = calculate_progress_percent(current, total)
        print(f"{message}: {current}/{total} ({percent:.1f}%)")
