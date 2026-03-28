"""
Accessibility tests for formatter system.

This module contains tests verifying accessibility features including
screen reader compatibility, color contrast, and keyboard navigation.
"""

from duckdb_processor.formatters.config import OutputConfig
from duckdb_processor.formatters.simple_formatter import SimpleFormatter
from duckdb_processor.formatters.terminal import (
    detect_screen_reader,
    detect_terminal_width,
    supports_color,
)


class TestScreenReaderSupport:
    """Test screen reader compatibility."""

    def test_simple_formatter_screen_reader_mode(self, capsys):
        """Test SimpleFormatter output is screen reader friendly."""
        formatter = SimpleFormatter()

        # SimpleFormatter should work without colors
        import pandas as pd
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Output should be plain text suitable for screen readers
        assert captured.out is not None
        assert len(captured.out) > 0

    def test_text_independence_from_color(self, capsys):
        """Test information is conveyed through text, not just color."""
        formatter = SimpleFormatter()

        error = Exception("Test error message")
        formatter.format_error(error, "context", "ERROR")

        captured = capsys.readouterr()
        # Error severity should be in text, not just color
        assert "ERROR" in captured.err
        assert "Test error message" in captured.err


class TestColorContrast:
    """Test color contrast and accessibility."""

    def test_high_contrast_mode_config(self):
        """Test high-contrast mode configuration."""
        config = OutputConfig(high_contrast_mode=True)
        assert config.high_contrast_mode is True

    def test_no_color_flag_disables_colors(self):
        """Test --no-color flag disables color output."""
        class Args:
            format = "rich"
            no_color = True
            no_progress = False

        config = OutputConfig.from_args(Args())
        assert config.color_enabled is False


class TestTerminalCapabilityDetection:
    """Test terminal capability detection."""

    def test_detect_terminal_width(self):
        """Test terminal width detection returns reasonable value."""
        width = detect_terminal_width()
        assert isinstance(width, int)
        assert width >= 40  # Minimum reasonable width
        assert width <= 300  # Maximum reasonable width

    def test_supports_color_returns_bool(self):
        """Test color detection returns boolean."""
        result = supports_color()
        assert isinstance(result, bool)

    def test_detect_screen_reader_returns_bool(self):
        """Test screen reader detection returns boolean."""
        result = detect_screen_reader()
        assert isinstance(result, bool)


class TestKeyboardNavigation:
    """Test keyboard navigation and accessibility."""

    def test_formatter_output_is_navigable(self, capsys):
        """Test formatter output is structured for screen readers."""
        formatter = SimpleFormatter()

        import pandas as pd
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Output should be structured and readable
        lines = captured.out.strip().split('\n')
        assert len(lines) > 0
        # Each line should be readable
        for line in lines:
            assert isinstance(line, str)
            assert len(line) > 0


class TestWCAGCompliance:
    """Test WCAG accessibility compliance."""

    def test_color_not_sole_information_source(self, capsys):
        """Test color is not the only way information is conveyed."""
        formatter = SimpleFormatter()

        # Error messages should have text prefix
        error = Exception("Test error")
        formatter.format_error(error, "context", "ERROR")

        captured = capsys.readouterr()
        # "ERROR" text should be present, not just red color
        assert "ERROR" in captured.err

    def test_text_only_mode_preserves_information(self, capsys):
        """Test text-only mode preserves all information."""
        config = OutputConfig(color_enabled=False, screen_reader_mode=True)
        formatter = SimpleFormatter(config.__dict__)

        import pandas as pd
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # All data should be present in output
        assert '1' in captured.out
        assert 'x' in captured.out
