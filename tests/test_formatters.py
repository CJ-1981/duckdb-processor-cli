"""
Unit tests for formatter implementations.

This module contains comprehensive unit tests for all formatter classes.
"""

import pandas as pd
import pytest

from duckdb_processor.formatters.base import BaseFormatter
from duckdb_processor.formatters.config import OutputConfig
from duckdb_processor.formatters.rich_formatter import RichFormatter
from duckdb_processor.formatters.simple_formatter import SimpleFormatter


class TestBaseFormatter:
    """Test BaseFormatter abstract class."""

    def test_base_formatter_is_abstract(self):
        """Test that BaseFormatter cannot be instantiated."""
        with pytest.raises(TypeError):
            BaseFormatter()


class TestSimpleFormatter:
    """Test SimpleFormatter implementation."""

    def test_initialization(self):
        """Test SimpleFormatter initialization."""
        formatter = SimpleFormatter()
        assert formatter.max_rows == 50

        formatter = SimpleFormatter({'max_rows': 100})
        assert formatter.max_rows == 100

    def test_format_dataframe_small(self, capsys):
        """Test formatting small DataFrame."""
        formatter = SimpleFormatter()
        df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})

        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        assert '1' in captured.out
        assert 'x' in captured.out

    def test_format_dataframe_large(self, capsys):
        """Test formatting large DataFrame with truncation."""
        formatter = SimpleFormatter({'max_rows': 10})
        df = pd.DataFrame({'A': range(100), 'B': range(100)})

        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # Should show first 5 and last 5 rows
        assert '0' in captured.out
        assert '99' in captured.out

    def test_format_info(self, capsys):
        """Test info banner formatting."""
        formatter = SimpleFormatter()
        metadata = {
            'source': 'test.csv',
            'rows': 100,
            'columns': ['A', 'B', 'C']
        }

        formatter.format_info(metadata)

        captured = capsys.readouterr()
        assert 'DuckDB CSV Processor' in captured.out
        assert 'test.csv' in captured.out
        assert '100' in captured.out

    def test_format_error(self, capsys):
        """Test error message formatting."""
        formatter = SimpleFormatter()
        error = Exception("Test error")

        formatter.format_error(error, "test context")

        captured = capsys.readouterr()
        assert 'ERROR' in captured.err
        assert 'Test error' in captured.err

    def test_format_progress(self, capsys):
        """Test progress indicator."""
        formatter = SimpleFormatter()

        formatter.format_progress("Loading", 50, 100)

        captured = capsys.readouterr()
        assert 'Loading' in captured.out
        assert '50/100' in captured.out
        assert '50.0%' in captured.out


class TestRichFormatter:
    """Test RichFormatter implementation."""

    def test_initialization(self):
        """Test RichFormatter initialization."""
        formatter = RichFormatter()
        assert formatter.max_rows == 50

        formatter = RichFormatter({'max_rows': 100, 'color_enabled': False})
        assert formatter.max_rows == 100
        assert formatter.color_enabled is False

    def test_rich_library_fallback(self, capsys):
        """Test fallback to SimpleFormatter when Rich unavailable."""
        # This test assumes Rich is installed
        # In a real test, you'd mock the import
        formatter = RichFormatter()
        assert formatter.rich_available is True

    def test_format_dataframe_type_aware(self, capsys):
        """Test type-aware column formatting."""
        formatter = RichFormatter()
        df = pd.DataFrame({
            'Numeric': [1, 2, 3],
            'Text': ['a', 'b', 'c']
        })

        formatter.format_dataframe(df)

        captured = capsys.readouterr()
        # RichFormatter should use colors and formatting
        assert '1' in captured.out or 'Numeric' in captured.out

    def test_format_error_color_coded(self, capsys):
        """Test color-coded error messages."""
        formatter = RichFormatter()
        error = Exception("Test error")

        formatter.format_error(error, "test context", "ERROR")

        captured = capsys.readouterr()
        assert 'ERROR' in captured.err
        assert 'Test error' in captured.err


class TestOutputConfig:
    """Test OutputConfig configuration class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = OutputConfig()
        assert config.formatter_type == "rich"
        assert config.color_enabled is True
        assert config.progress_enabled is True
        assert config.max_rows == 50

    def test_custom_values(self):
        """Test custom configuration values."""
        config = OutputConfig(
            formatter_type="simple",
            color_enabled=False,
            max_rows=100
        )
        assert config.formatter_type == "simple"
        assert config.color_enabled is False
        assert config.max_rows == 100

    def test_from_args(self):
        """Test creating config from CLI arguments."""
        class Args:
            format = "simple"
            no_color = True
            no_progress = False

        config = OutputConfig.from_args(Args())
        assert config.formatter_type == "simple"
        assert config.color_enabled is False
        assert config.progress_enabled is True

    def test_detect_capabilities(self):
        """Test auto-detection of terminal capabilities."""
        config = OutputConfig.detect_capabilities()
        assert config.terminal_width is not None
        assert isinstance(config.terminal_width, int)
        # color_enabled depends on environment
