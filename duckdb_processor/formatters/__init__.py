"""
Output formatters for DuckDB CSV Processor.

This module provides formatter implementations for CLI output including
Rich library integration and legacy simple formatting.
"""

from .base import BaseFormatter
from .config import OutputConfig
from .rich_formatter import RichFormatter
from .simple_formatter import SimpleFormatter
from .terminal import print_banner, print_section_divider

__all__ = [
    "BaseFormatter",
    "OutputConfig",
    "SimpleFormatter",
    "RichFormatter",
    "print_banner",
    "print_section_divider",
]
