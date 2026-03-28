"""
Characterization tests for Processor behavior preservation.

These tests capture the EXISTING behavior of processor.py before modifications.
They serve as a safety net to ensure backward compatibility during refactoring.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from duckdb_processor.processor import Processor


class TestProcessorPrintInfo:
    """Characterization tests for print_info() method."""

    def test_print_info_output_format(self, capsys):
        """Test that print_info produces expected output format.

        NOTE: This captures CURRENT behavior. The info banner uses
        box-drawing characters (─) and displays specific metadata fields.
        """
        # Create a processor with test data
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['A', 'B', 'C'],
            table='test_table',
            source='test.csv',
            has_header=True,
            is_kv=False,
            n_records=100
        )

        # Call print_info
        processor.print_info()

        captured = capsys.readouterr()

        # Verify banner format (current implementation)
        assert 'DuckDB CSV Processor' in captured.out
        assert 'test.csv' in captured.out
        assert 'yes' in captured.out  # has_header
        assert 'flat CSV' in captured.out  # format
        assert '100' in captured.out  # n_records
        assert 'A, B, C' in captured.out  # columns
        assert 'test_table' in captured.out  # table name

    def test_print_info_without_header(self, capsys):
        """Test print_info when has_header is False."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['A', 'B'],
            table='data',
            source='data.csv',
            has_header=False,
            is_kv=False,
            n_records=50
        )

        processor.print_info()

        captured = capsys.readouterr()
        assert 'no' in captured.out  # has_header = False

    def test_print_info_kv_format(self, capsys):
        """Test print_info when is_kv is True."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['key', 'value'],
            table='data',
            source='data.csv',
            has_header=False,
            is_kv=True,
            n_records=25
        )

        processor.print_info()

        captured = capsys.readouterr()
        assert 'key:value' in captured.out  # is_kv format


class TestProcessorInfoMethod:
    """Characterization tests for info() method."""

    def test_info_returns_dict(self):
        """Test that info() returns a dictionary with expected keys."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['A', 'B'],
            table='data',
            source='test.csv',
            has_header=True,
            is_kv=False,
            n_records=100
        )

        info_dict = processor.info()

        # Verify all expected keys are present
        assert 'source' in info_dict
        assert 'header' in info_dict
        assert 'format' in info_dict
        assert 'rows' in info_dict
        assert 'columns' in info_dict
        assert 'table' in info_dict

    def test_info_values(self):
        """Test that info() returns correct values."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['X', 'Y', 'Z'],
            table='my_table',
            source='data.csv',
            has_header=True,
            is_kv=False,
            n_records=42
        )

        info_dict = processor.info()

        assert info_dict['source'] == 'data.csv'
        assert info_dict['header'] is True
        assert info_dict['format'] == 'flat CSV'
        assert info_dict['rows'] == 42
        assert info_dict['columns'] == ['X', 'Y', 'Z']
        assert info_dict['table'] == 'my_table'


class TestProcessorOutputFormat:
    """Characterization tests for DataFrame output format."""

    def test_sql_method_returns_dataframe(self):
        """Test that sql() method returns a DataFrame.

        NOTE: This captures CURRENT behavior. All query results
        are returned as pandas DataFrames.
        """
        # Create a mock DuckDB connection
        mock_con = MagicMock()
        mock_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
        mock_con.execute.return_value.df.return_value = mock_df

        processor = Processor(
            con=mock_con,
            columns=['A', 'B'],
            table='data'
        )

        result = processor.sql('SELECT * FROM data')

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ['A', 'B']
        assert len(result) == 3

    def test_preview_returns_dataframe(self):
        """Test that preview() returns DataFrame.

        NOTE: This captures CURRENT behavior. The preview() method
        executes SQL with LIMIT, but our mock returns the full DataFrame.
        In real execution, DuckDB would apply the LIMIT.
        """
        mock_con = MagicMock()
        mock_df = pd.DataFrame({'A': range(20), 'B': range(20)})
        mock_con.execute.return_value.df.return_value = mock_df

        processor = Processor(
            con=mock_con,
            columns=['A', 'B'],
            table='data'
        )

        result = processor.preview(n=10)

        # Verify result is DataFrame (actual SQL execution would apply LIMIT)
        assert isinstance(result, pd.DataFrame)
        # Verify the SQL query includes LIMIT clause
        mock_con.execute.assert_called_once()
        call_args = mock_con.execute.call_args[0][0]
        assert 'LIMIT' in call_args
        assert '10' in call_args


class TestProcessorMethodSignatures:
    """Characterization tests for method signatures (API stability)."""

    def test_print_info_signature(self):
        """Test that print_info() has no required parameters."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['A'],
            table='data'
        )

        # Should be callable without arguments
        processor.print_info()

    def test_info_signature(self):
        """Test that info() has no required parameters."""
        mock_con = MagicMock()
        processor = Processor(
            con=mock_con,
            columns=['A'],
            table='data'
        )

        # Should be callable without arguments
        result = processor.info()
        assert isinstance(result, dict)


class TestProcessorSideEffects:
    """Characterization tests for side effects that must be preserved."""

    def test_add_column_prints_confirmation(self, capsys):
        """Test that add_column prints confirmation message.

        NOTE: This captures CURRENT behavior. Operations like
        add_column, create_view, and exports print confirmation.
        """
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []
        mock_con.execute.return_value.fetchone.return_value = [0]

        processor = Processor(
            con=mock_con,
            columns=['A'],
            table='data'
        )

        processor.add_column('B', '1')

        captured = capsys.readouterr()
        assert 'Column' in captured.out
        assert 'B' in captured.out
        assert 'added' in captured.out

    def test_create_view_prints_confirmation(self, capsys):
        """Test that create_view prints confirmation message."""
        mock_con = MagicMock()

        processor = Processor(
            con=mock_con,
            columns=['A'],
            table='data'
        )

        processor.create_view('filtered', 'A > 5')

        captured = capsys.readouterr()
        assert 'View' in captured.out
        assert 'filtered' in captured.out
        assert 'created' in captured.out


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
