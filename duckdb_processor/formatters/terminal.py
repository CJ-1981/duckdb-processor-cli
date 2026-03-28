"""
Terminal capability detection utilities.

This module provides utilities for detecting terminal capabilities
including color support, width detection, and screen reader detection.
"""

import os
import shutil
import sys

# Banner formatting constants
BANNER_WIDTH = 58


def detect_terminal_width() -> int:
    """
    Detect terminal width with fallback.

    Returns:
        Terminal width in characters (default 80)
    """
    try:
        return shutil.get_terminal_size().columns
    except Exception:
        return 80  # Default fallback


def supports_color() -> bool:
    """
    Check if terminal supports ANSI color codes.

    Returns:
        True if terminal supports colors, False otherwise
    """
    # Check if output is to a terminal
    if not hasattr(sys.stdout, 'isatty'):
        return False
    if not sys.stdout.isatty():
        return False

    # Check for NO_COLOR environment variable
    if os.environ.get('NO_COLOR'):
        return False

    # Platform-specific checks
    if sys.platform == 'win32':
        # Windows 10+ supports ANSI codes
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return False
    else:
        # Unix-like systems typically support colors
        return True


def detect_screen_reader() -> bool:
    """
    Detect screen reader usage.

    Returns:
        True if screen reader detected, False otherwise
    """
    # Check for common screen reader environment variables
    screen_reader_indicators = [
        'SCREEN_READER',
        'JAWS',
        'NVDA',
        'VOICE_OVER'
    ]
    return any(indicator in os.environ for indicator in screen_reader_indicators)


def print_banner(title: str, width: int = BANNER_WIDTH, char: str = "─") -> None:
    """
    Print a formatted banner with title.

    Args:
        title: Banner title text
        width: Banner width in characters (default BANNER_WIDTH)
        char: Character to use for border (default "─")
    """
    print()
    print(char * width)
    print(f"  {title}")
    print(char * width)


def print_section_divider(width: int = BANNER_WIDTH, char: str = "─") -> None:
    """
    Print a section divider line.

    Args:
        width: Line width in characters (default BANNER_WIDTH)
        char: Character to use (default "─")
    """
    print(char * width)
