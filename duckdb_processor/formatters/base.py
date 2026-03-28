"""
Base formatter interface for DuckDB CSV Processor output.

This module defines the abstract base class for all output formatters.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pandas as pd


class BaseFormatter(ABC):
    """
    Abstract base class for output formatters.

    All formatters must implement these methods to provide consistent
    output formatting across different display modes.
    """

    @abstractmethod
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize formatter with configuration.

        Args:
            config: Optional configuration dictionary
        """
        pass

    @abstractmethod
    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """
        Display a DataFrame with appropriate formatting.

        Args:
            df: DataFrame to display
            max_rows: Maximum rows to display (None for unlimited)
        """
        pass

    @abstractmethod
    def format_info(self, metadata: Dict[str, Any]) -> None:
        """
        Display dataset information banner.

        Args:
            metadata: Dictionary with source, rows, columns info
        """
        pass

    @abstractmethod
    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """
        Display error message with appropriate styling.

        Args:
            error: Exception to display
            context: Context where error occurred
            severity: Error level (ERROR, WARNING, INFO)
        """
        pass

    @abstractmethod
    def format_progress(self, message: str, current: int, total: int) -> None:
        """
        Display progress indicator.

        Args:
            message: Progress message
            current: Current progress value
            total: Total value for completion
        """
        pass
