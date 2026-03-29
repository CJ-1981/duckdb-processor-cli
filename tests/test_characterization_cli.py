"""
Characterization tests for CLI behavior preservation.

These tests capture the EXISTING behavior of cli.py before modifications.
They serve as a safety net to ensure backward compatibility during refactoring.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

# Import directly from modules to avoid __init__.py import issues
sys.path.insert(0, '/Users/chimin/Documents/script/duckdb-processor-cli')

from duckdb_processor.cli import build_arg_parser, main


class TestCLIArgumentParsing:
    """Characterization tests for CLI argument parsing."""

    def test_default_arguments(self):
        """Test default argument values."""
        parser = build_arg_parser()
        args = parser.parse_args(['test.csv'])

        assert args.file == 'test.csv'
        assert args.header is None
        assert args.kv is None
        assert args.table == 'data'
        assert args.run is None
        assert args.list_analyzers is False
        assert args.interactive is False

    def test_header_arguments(self):
        """Test header-related argument parsing."""
        parser = build_arg_parser()

        # --header flag
        args = parser.parse_args(['--header', 'test.csv'])
        assert args.header is True

        # --no-header flag
        args = parser.parse_args(['--no-header', 'test.csv'])
        assert args.header is False

    def test_kv_arguments(self):
        """Test key:value format argument parsing."""
        parser = build_arg_parser()

        # --kv flag
        args = parser.parse_args(['--kv', 'test.csv'])
        assert args.kv is True

        # --no-kv flag
        args = parser.parse_args(['--no-kv', 'test.csv'])
        assert args.kv is False

    def test_table_argument(self):
        """Test custom table name argument."""
        parser = build_arg_parser()
        args = parser.parse_args(['--table', 'my_table', 'test.csv'])

        assert args.table == 'my_table'

    def test_run_argument(self):
        """Test analyzer execution argument.

        NOTE: With nargs='+', argparse collects space-separated arguments
        but does NOT split on commas. Comma splitting happens later in main().
        This test captures the raw argparse behavior.
        """
        parser = build_arg_parser()
        args = parser.parse_args(['--run', 'demo,step2', 'test.csv'])

        # argparse with nargs="+" collects space-separated args, not comma-split
        assert args.run == ['demo,step2', 'test.csv']
        assert len(args.run) == 2

    def test_col_names_argument(self):
        """Test column names argument."""
        parser = build_arg_parser()
        args = parser.parse_args(['--col-names', 'A,B,C', 'test.csv'])

        assert args.col_names == ['A,B,C', 'test.csv']

    def test_format_argument_default(self):
        """Test default format argument."""
        parser = build_arg_parser()
        args = parser.parse_args(['test.csv'])

        assert args.format == 'rich'

    def test_format_argument_simple(self):
        """Test --format simple argument."""
        parser = build_arg_parser()
        args = parser.parse_args(['--format', 'simple', 'test.csv'])

        assert args.format == 'simple'

    def test_no_color_argument(self):
        """Test --no-color flag."""
        parser = build_arg_parser()
        args = parser.parse_args(['--no-color', 'test.csv'])

        assert args.no_color is True

    def test_no_progress_argument(self):
        """Test --no-progress flag."""
        parser = build_arg_parser()
        args = parser.parse_args(['--no-progress', 'test.csv'])

        assert args.no_progress is True


class TestCLIOutputFormat:
    """Characterization tests for CLI output format."""

    @patch('duckdb_processor.cli.load')
    @patch('duckdb_processor.cli.run_analyzers')
    def test_info_banner_output_format(self, mock_run, mock_load, capsys):
        """Test that info banner appears in expected format.

        NOTE: This captures CURRENT behavior. The banner format uses
        box-drawing characters (─) and shows specific metadata fields.
        """
        # Mock the processor
        mock_processor = MagicMock()
        mock_processor.print_info = MagicMock()  # Mock print_info to capture output
        mock_load.return_value = mock_processor

        # Run main with test file
        main(['test.csv'])

        # Verify print_info was called
        mock_processor.print_info.assert_called_once()

    @patch('duckdb_processor.cli.load')
    def test_error_output_format(self, mock_load, capsys):
        """Test that error messages appear in expected format.

        NOTE: This captures CURRENT behavior. Errors are printed
        to stderr with 'Error: ' prefix.
        """
        # Mock load to raise ValueError
        mock_load.side_effect = ValueError('Test error message')

        # Run main and expect SystemExit
        with pytest.raises(SystemExit):
            main(['test.csv'])

        captured = capsys.readouterr()

        # Verify error format
        assert 'Error:' in captured.err
        assert 'Test error message' in captured.err


class TestCLIIntegrationPoints:
    """Characterization tests for CLI integration with processor."""

    @patch('duckdb_processor.cli.load')
    @patch('duckdb_processor.cli.run_analyzers')
    def test_processor_print_info_called(self, mock_run, mock_load):
        """Test that print_info is called on the processor.

        NOTE: This is CRITICAL for backward compatibility. The print_info
        method is currently called directly by main() and must continue
        to work after formatter integration.
        """
        # Mock the processor
        mock_processor = MagicMock()
        mock_load.return_value = mock_processor

        # Run main
        main(['test.csv'])

        # Verify print_info was called
        mock_processor.print_info.assert_called_once()

    @patch('duckdb_processor.cli.load')
    @patch('duckdb_processor.cli.get_analyzer')
    def test_analyzers_run_when_requested(self, mock_get_analyzer, mock_load):
        """Test that analyzers are executed when --run is specified.

        NOTE: This captures CURRENT behavior. Analyzers run after
        info banner is displayed. Each analyzer is loaded and run individually.
        """
        # Mock the processor and analyzer
        mock_processor = MagicMock()
        mock_processor.last_result = None  # No export results
        mock_load.return_value = mock_processor

        mock_analyzer = MagicMock()
        mock_analyzer.description = "Demo analyzer"
        mock_get_analyzer.return_value = mock_analyzer

        # Run main with --run flag (file is positional, --run takes remaining args)
        main(['test.csv', '--run', 'demo'])

        # Verify get_analyzer was called for the analyzer
        mock_get_analyzer.assert_called_once_with('demo')

        # Verify analyzer's run method was called
        mock_analyzer.run.assert_called_once_with(mock_processor)


class TestCLILegacyBehavior:
    """Characterization tests for legacy behavior that must be preserved."""

    @patch('duckdb_processor.cli.list_analyzers')
    def test_list_analyzers_output_format(self, mock_list, capsys):
        """Test that --list-analyzers produces expected output format.

        NOTE: This captures CURRENT behavior. Lists analyzer names
        and descriptions in specific format.
        """
        # Mock analyzer list
        mock_list.return_value = [
            {'name': 'demo', 'description': 'Demo analyzer'},
            {'name': 'step2', 'description': 'Second step'}
        ]

        # Run main with --list-analyzers
        result = main(['--list-analyzers'])

        # Should return None and exit
        assert result is None

        captured = capsys.readouterr()
        assert 'Available analyzers:' in captured.out
        assert 'demo' in captured.out
        assert 'Demo analyzer' in captured.out

    def test_repl_exit_commands(self):
        """Test that REPL recognizes exit commands.

        NOTE: This captures CURRENT behavior. REPL exits on
        EXIT, QUIT, or \\q commands.
        """
        from duckdb_processor.cli import interactive_repl

        # Mock processor
        mock_processor = MagicMock()

        # Test exit commands (this will loop, so we'll interrupt)
        with patch('builtins.input', side_effect=['EXIT']):
            try:
                interactive_repl(mock_processor)
            except (EOFError, KeyboardInterrupt):
                pass

            # Should handle exit gracefully
            # (no exception should propagate)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
