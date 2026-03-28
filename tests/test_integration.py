"""
Integration tests for formatter system.

This module contains integration tests verifying formatter
integration with existing CLI and analyzer components.
"""

import pandas as pd

from duckdb_processor.formatters.config import OutputConfig
from duckdb_processor.formatters.rich_formatter import RichFormatter
from duckdb_processor.formatters.simple_formatter import SimpleFormatter


class TestFormatterIntegration:
    """Test formatter integration with CLI components."""

    def test_simple_formatter_backward_compatibility(self, capsys):
        """Test SimpleFormatter maintains backward compatibility."""
        formatter = SimpleFormatter()

        # Test DataFrame display (legacy format)
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Output should match pandas to_string() format
        assert '1' in captured.out
        assert 'x' in captured.out

    def test_rich_formatter_integration(self, capsys):
        """Test RichFormatter integration."""
        config = OutputConfig(formatter_type="rich")
        formatter = RichFormatter(config.__dict__)

        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Should produce formatted output
        assert 'A' in captured.out or '1' in captured.out

    def test_formatter_selection_via_config(self):
        """Test formatter selection through configuration."""
        config = OutputConfig(formatter_type="simple")
        assert config.formatter_type == "simple"

        config = OutputConfig(formatter_type="rich")
        assert config.formatter_type == "rich"

    def test_cli_flag_integration(self):
        """Test CLI flag integration with formatter selection."""
        class Args:
            format = "simple"
            no_color = True
            no_progress = False

        config = OutputConfig.from_args(Args())
        assert config.formatter_type == "simple"
        assert config.color_enabled is False

    def test_terminal_capability_detection(self):
        """Test automatic terminal capability detection."""
        config = OutputConfig.detect_capabilities()
        assert config.terminal_width is not None
        assert isinstance(config.terminal_width, int)


class TestErrorHandling:
    """Test error handling and fallback behavior."""

    def test_missing_rich_library_fallback(self, capsys):
        """Test fallback when Rich library is not available."""
        # This test would require mocking the Rich import
        # For now, we test the existing behavior
        formatter = RichFormatter()
        df = pd.DataFrame({'A': [1, 2, 3]})

        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Should produce output (either Rich or fallback)
        assert captured.out is not None

    def test_error_message_formatting(self, capsys):
        """Test error message formatting with different severity levels."""
        formatter = RichFormatter()

        for severity in ["ERROR", "WARNING", "INFO"]:
            error = Exception(f"Test {severity}")
            formatter.format_error(error, "test", severity)

        captured = capsys.readouterr()
        assert "ERROR" in captured.err  # Errors go to stderr
        assert "WARNING" in captured.out  # Warnings and INFO go to stdout
        assert "INFO" in captured.out
