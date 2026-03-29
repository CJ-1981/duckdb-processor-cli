"""Analyst-facing Processor API.

Data analysts interact with loaded data **exclusively** through this
class.  It wraps a DuckDB connection and provides high-level methods
for common analytical operations — filtering, deriving, aggregating,
pivoting, and exporting — while keeping the SQL details behind a
clean Python interface.

Example
-------
>>> from duckdb_processor import load
>>> p = load("sales.csv")
>>> p.preview()
>>> p.add_column("tier", "CASE WHEN … END")
>>> p.aggregate("region", "amount", "SUM")
>>> p.export_csv("output.csv")
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import duckdb
import pandas as pd

if TYPE_CHECKING:
    from .formatters.base import BaseFormatter


class Processor:
    """High-level data analysis API wrapping a DuckDB connection.

    Analysts receive a :class:`Processor` instance from
    :func:`~duckdb_processor.loader.load` and use its methods to
    explore, transform, and export data.

    Parameters
    ----------
    con:
        An open ``duckdb.DuckDBPyConnection``.
    columns:
        Ordered list of business columns (excludes internal keys).
    table:
        Name of the DuckDB table backing this processor.
    source:
        Human-readable label for where the data came from.
    has_header, is_kv, n_records:
        Metadata recorded at load time for :meth:`info`.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        columns: list[str],
        table: str = "data",
        *,
        source: str = "",
        has_header: bool = False,
        is_kv: bool = False,
        n_records: int = 0,
        formatter: Optional["BaseFormatter"] = None,  # @MX:ANCHOR: Formatter injection point for output customization (REQ-010)
    ):
        self.con = con
        self.columns = columns
        self.table = table
        self._meta = {
            "source": source,
            "has_header": has_header,
            "is_kv": is_kv,
            "n_records": n_records,
        }
        self.formatter = formatter  # Optional formatter for output (REQ-010, REQ-011)
        self.last_result: pd.DataFrame | None = None  # Track last query result for export

    # ── Metadata ─────────────────────────────────────────────

    def info(self) -> dict:
        """Return a summary dictionary describing the loaded dataset."""
        return {
            "source": self._meta["source"],
            "header": self._meta["has_header"],
            "format": "key:value" if self._meta["is_kv"] else "flat CSV",
            "rows": self._meta["n_records"],
            "columns": list(self.columns),
            "table": self.table,
        }

    def print_info(self) -> None:
        """Print a formatted banner with dataset metadata.

        Uses formatter if available (REQ-008), otherwise falls back to
        legacy format for backward compatibility (REQ-010).
        """
        m = self.info()

        # Use formatter if available (REQ-004, REQ-008)
        if self.formatter:
            self.formatter.format_info(m)
        else:
            # Legacy format for backward compatibility (REQ-010)
            width = 58
            print()
            print("\u2501" * width)
            print("  DuckDB CSV Processor")
            print("\u2501" * width)
            print(f"  Source      : {m['source']}")
            print(f"  Header      : {'yes' if m['header'] else 'no'}")
            print(f"  Format      : {m['format']}")
            print(f"  Rows loaded : {m['rows']}")
            print(f"  Columns     : {', '.join(m['columns'])}")
            print(f"  Table       : {m['table']}")
            print("\u2501" * width)
            print()

    # ── Core SQL ─────────────────────────────────────────────

    def sql(self, query: str) -> pd.DataFrame:
        """Execute arbitrary SQL and return the result as a DataFrame.

        The table name configured at load time (default ``data``)
        is available in the query.

        Parameters
        ----------
        query:
            Any valid DuckDB SQL statement.

        Returns
        -------
        pandas.DataFrame
        """
        result = self.con.execute(query).df()
        self.last_result = result  # Store for export
        return result

    def preview(self, n: int = 10) -> pd.DataFrame:
        """Return the first *n* rows from the table."""
        return self.sql(f"SELECT * FROM {self.table} LIMIT {n}")

    def schema(self) -> pd.DataFrame:
        """Return column names and their DuckDB types."""
        return self.sql(f"DESCRIBE {self.table}")

    def coverage(self) -> pd.DataFrame:
        """Report the non-null fill rate for every business column.

        Returns
        -------
        pandas.DataFrame
            Columns: ``column``, ``present``, ``coverage_%``.
        """
        total = self.con.execute(
            f"SELECT COUNT(*) FROM {self.table}"
        ).fetchone()[0]

        rows: list[dict] = []
        for c in self.columns:
            count = self.con.execute(
                f'SELECT COUNT(*) FROM {self.table} '
                f'WHERE "{c}" IS NOT NULL AND "{c}" != \'\''
            ).fetchone()[0]
            rows.append(
                {
                    "column": c,
                    "present": count,
                    "coverage_%": round(count / total * 100, 1) if total else 0,
                }
            )
        return pd.DataFrame(rows)

    # ── Filter ───────────────────────────────────────────────

    def filter(self, where: str) -> pd.DataFrame:
        """Return rows matching a SQL ``WHERE`` clause.

        Example
        -------
        >>> p.filter("status = 'active' AND CAST(amount AS DOUBLE) >= 500")
        """
        return self.sql(f"SELECT * FROM {self.table} WHERE {where}")

    def create_view(self, name: str, where: str) -> None:
        """Persist a filtered view for use in later queries.

        Example
        -------
        >>> p.create_view("active", "status = 'active'")
        >>> p.sql("SELECT COUNT(*) FROM active")
        """
        self.con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM {self.table} WHERE {where}"
        )
        print(f"View '{name}' created")

    # ── Derive ───────────────────────────────────────────────

    def add_column(
        self, new_col: str, expr: str, source: str | None = None
    ) -> None:
        """Add (or replace) a derived column via a SQL expression.

        The new column is typed as ``VARCHAR``; cast explicitly inside
        *expr* if you need a different type for downstream logic.

        Example
        -------
        >>> p.add_column("tier", '''
        ...     CASE
        ...         WHEN CAST(amount AS DOUBLE) >= 10000 THEN 'PLATINUM'
        ...         WHEN CAST(amount AS DOUBLE) >= 5000  THEN 'GOLD'
        ...         ELSE 'BRONZE'
        ...     END
        ... ''')
        """
        tbl = source or self.table
        existing = [r[0] for r in self.con.execute(f"DESCRIBE {tbl}").fetchall()]
        if new_col in existing:
            self.con.execute(f'ALTER TABLE {tbl} DROP COLUMN "{new_col}"')
        self.con.execute(
            f'ALTER TABLE {tbl} ADD COLUMN "{new_col}" VARCHAR'
        )
        self.con.execute(
            f'UPDATE {tbl} SET "{new_col}" = CAST(({expr}) AS VARCHAR)'
        )
        if new_col not in self.columns:
            self.columns.append(new_col)
        print(f"Column '{new_col}' added")

    # ── Aggregate ────────────────────────────────────────────

    def aggregate(
        self,
        group_by: str | list[str],
        agg_field: str,
        func: str = "SUM",
        source: str | None = None,
    ) -> pd.DataFrame:
        """Group-by aggregation on a numeric field.

        Parameters
        ----------
        group_by:
            One column name or a list of column names.
        agg_field:
            The column containing numeric values to aggregate.
        func:
            SQL aggregate function (``SUM``, ``AVG``, ``MIN``, ``MAX``,
            ``COUNT``).
        source:
            Override table/view name (defaults to the main table).

        Example
        -------
        >>> p.aggregate("region", "amount", "SUM")
        >>> p.aggregate(["region", "tier"], "amount", "AVG")
        """
        tbl = source or self.table
        if isinstance(group_by, list):
            gb = ", ".join(f'"{c}"' for c in group_by)
        else:
            gb = f'"{group_by}"'

        return self.sql(f"""
            SELECT {gb},
                   COUNT(*)                                        AS count,
                   ROUND({func}(TRY_CAST("{agg_field}" AS DOUBLE)), 2)
                       AS {func.lower()}_{agg_field}
            FROM {tbl}
            WHERE "{agg_field}" IS NOT NULL AND "{agg_field}" != ''
            GROUP BY {gb}
            ORDER BY {func.lower()}_{agg_field} DESC
        """)

    def pivot(
        self,
        row_key: str,
        col_key: str,
        val: str,
        func: str = "SUM",
        source: str | None = None,
    ) -> pd.DataFrame:
        """Cross-tabulate two categorical keys against a numeric value.

        Dynamically discovers the distinct values of *col_key* and
        creates one output column per value.

        Example
        -------
        >>> p.pivot("region", "tier", "amount")
        """
        tbl = source or self.table
        col_vals = [
            r[0]
            for r in self.con.execute(
                f'SELECT DISTINCT "{col_key}" FROM {tbl} '
                f'WHERE "{col_key}" IS NOT NULL ORDER BY "{col_key}"'
            ).fetchall()
        ]

        cases = ", ".join(
            f"ROUND({func}(CASE WHEN \"{col_key}\" = '{v}' "
            f"THEN TRY_CAST(\"{val}\" AS DOUBLE) END), 2) AS \"{v}\""
            for v in col_vals
        )
        return self.sql(f"""
            SELECT "{row_key}", {cases}
            FROM {tbl}
            GROUP BY "{row_key}"
            ORDER BY "{row_key}"
        """)

    # ── Export ───────────────────────────────────────────────

    def export_csv(self, path: str, query: str | None = None) -> None:
        """Export a query result (or the full table) to CSV.

        Parameters
        ----------
        path:
            Destination file path.
        query:
            SQL query whose results to export.  ``None`` exports the
            entire table.
        """
        q = query or f"SELECT * FROM {self.table}"
        self.con.execute(f"COPY ({q}) TO '{path}' (HEADER, DELIMITER ',')")
        print(f"Exported \u2192 {path}")

    def export_json(self, path: str, query: str | None = None) -> None:
        """Export a query result (or the full table) to JSON.

        The output is a pretty-printed array of objects.
        """
        q = query or f"SELECT * FROM {self.table}"
        rows = self.con.execute(q).df().to_dict(orient="records")
        Path(path).write_text(json.dumps(rows, indent=2, default=str))
        print(f"Exported \u2192 {path}")

    def export_parquet(self, path: str, query: str | None = None) -> None:
        """Export to Parquet for efficient storage or later reload."""
        q = query or f"SELECT * FROM {self.table}"
        self.con.execute(f"COPY ({q}) TO '{path}' (FORMAT PARQUET)")
        print(f"Exported \u2192 {path}")

    def export_xlsx(self, path: str, query: str | None = None) -> None:
        """Export a query result (or the full table) to Excel (XLSX).

        Requires openpyxl to be installed: pip install duckdb-processor[export]

        Parameters
        ----------
        path:
            Destination file path (.xlsx extension recommended).
        query:
            SQL query whose results to export. ``None`` exports the entire table.
        """
        try:
            from openpyxl import Workbook  # noqa: F401
            from openpyxl.utils.dataframe import dataframe_to_rows  # noqa: F401
        except ImportError:
            print("Error: openpyxl not installed. Install with: pip install openpyxl")
            print("Or install with: pip install duckdb_processor[export]")
            return

        q = query or f"SELECT * FROM {self.table}"
        df = self.sql(q)

        wb = Workbook()
        ws = wb.active
        if ws is not None:
            ws.title = "Query Results"

            # Write header
            ws.append(list(df.columns))

            # Write data rows
            for row in dataframe_to_rows(df, index=False, header=False):
                ws.append(row)

            # Auto-adjust column widths (simplified)
            for col_idx, col in enumerate(df.columns, start=1):
                max_len = max(
                    len(str(col)),
                    df[col].astype(str).str.len().max() if not df.empty else 0
                )
                ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(50, (max_len + 2) * 1.2)  # type: ignore

        wb.save(path)
        print(f"Exported \u2192 {path}")

    def export(self, path: str, format: str = "csv", query: str | None = None) -> None:
        """Export query results to file in specified format.

        Parameters
        ----------
        path:
            Destination file path.
        format:
            Export format: csv, json, xlsx, parquet.
        query:
            SQL query whose results to export. ``None`` exports the entire table.
        """
        format = format.lower()

        if format == "csv":
            self.export_csv(path, query)
        elif format == "json":
            self.export_json(path, query)
        elif format == "parquet":
            self.export_parquet(path, query)
        elif format == "xlsx":
            self.export_xlsx(path, query)
        else:
            print(f"Error: Unsupported format '{format}'. Supported: csv, json, xlsx, parquet")
