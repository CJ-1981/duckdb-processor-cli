"""
Shared utility functions for output formatters.

This module provides common utilities used across formatter implementations.
"""

import pandas as pd


def truncate_dataframe(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    """
    Truncate DataFrame to max_rows by showing head and tail.

    Args:
        df: DataFrame to truncate
        max_rows: Maximum rows to display

    Returns:
        Truncated DataFrame with head and tail rows
    """
    if len(df) <= max_rows:
        return df

    head_rows = max_rows // 2
    tail_rows = max_rows - head_rows
    return pd.concat([df.head(head_rows), df.tail(tail_rows)])


def calculate_progress_percent(current: int, total: int) -> float:
    """
    Calculate progress percentage with zero-division protection.

    Args:
        current: Current progress value
        total: Total value for completion

    Returns:
        Percentage (0-100)
    """
    return (current / total) * 100 if total > 0 else 0
