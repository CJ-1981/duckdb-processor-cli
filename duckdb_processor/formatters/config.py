"""
Configuration management for output formatters.

This module provides configuration dataclasses and utilities
for managing formatter behavior.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OutputConfig:
    """
    Configuration for output formatting behavior.

    Attributes:
        formatter_type: Type of formatter ('rich' or 'simple')
        color_enabled: Whether color output is enabled
        max_rows: Maximum rows to display in tables
        max_columns: Maximum columns to display
        progress_enabled: Whether progress indicators are shown
        progress_min_duration: Minimum duration (seconds) to show progress
        terminal_width: Terminal width (None for auto-detect)
        truncate_columns: Whether to truncate wide columns
        high_contrast_mode: Whether high-contrast mode is enabled
        screen_reader_mode: Whether screen reader mode is enabled
    """

    # Formatter selection
    formatter_type: str = "rich"  # 'rich' or 'simple'

    # Display options
    color_enabled: bool = True
    max_rows: int = 50
    max_columns: int = 15

    # Progress indicators
    progress_enabled: bool = True
    progress_min_duration: float = 2.0  # seconds

    # Table formatting
    terminal_width: Optional[int] = None  # None = auto-detect
    truncate_columns: bool = True

    # Accessibility
    high_contrast_mode: bool = False
    screen_reader_mode: bool = False

    @classmethod
    def from_args(cls, args) -> 'OutputConfig':
        """
        Create config from CLI arguments.

        Args:
            args: Parsed CLI arguments

        Returns:
            OutputConfig instance
        """
        return cls(
            formatter_type=getattr(args, 'format', 'rich'),
            color_enabled=not getattr(args, 'no_color', False),
            progress_enabled=not getattr(args, 'no_progress', False)
        )

    @classmethod
    def detect_capabilities(cls) -> 'OutputConfig':
        """
        Auto-detect terminal capabilities using existing utilities.

        Returns:
            OutputConfig instance with detected capabilities
        """
        from .terminal import detect_screen_reader, detect_terminal_width, supports_color

        config = cls()
        config.terminal_width = detect_terminal_width()
        config.color_enabled = supports_color()
        config.screen_reader_mode = detect_screen_reader()

        return config
