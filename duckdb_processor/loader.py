"""DuckDB schema inference, table creation, and data loading.

This module orchestrates the full pipeline from raw CSV to a ready-to-use
:class:`~duckdb_processor.processor.Processor` instance via the single
:func:`load` entry point.

Analysts rarely need to import anything from this module directly — they
use :func:`~duckdb_processor.load` (re-exported from the top-level
package) instead.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb

from .config import ProcessorConfig

if TYPE_CHECKING:
    from .formatters.base import BaseFormatter
from .detection import detect_header, detect_kv
from .parsing import normalize, read_input
from .processor import Processor

# Internal metadata keys that must not become table columns.
_STRUCTURAL_KEYS = frozenset({"_row", "_raw", "_error", "_unparsed"})


def _infer_columns(records: list[dict]) -> list[str]:
    """Collect all user-facing column names in first-seen order.

    Keys that start with ``_`` (e.g. ``_row``, ``_error``) are treated
    as internal bookkeeping and excluded from the result.
    """
    seen: dict[str, None] = {}
    for rec in records:
        for k in rec:
            if k not in _STRUCTURAL_KEYS and k not in seen:
                seen[k] = None
    return list(seen.keys())


def _create_table(
    con: duckdb.DuckDBPyConnection,
    columns: list[str],
    table: str,
) -> None:
    """Create (or replace) a DuckDB table with all-``VARCHAR`` columns.

    A trailing ``_row INTEGER`` column is always added for traceability.
    """
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
    col_defs += ", _row INTEGER"
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table} ({col_defs})")


def _insert_records(
    con: duckdb.DuckDBPyConnection,
    columns: list[str],
    records: list[dict],
    table: str,
) -> None:
    """Insert all normalised records into the DuckDB table."""
    col_list = ", ".join(f'"{c}"' for c in columns) + ", _row"
    placeholders = ", ".join(["?"] * (len(columns) + 1))

    for rec in records:
        vals = [
            str(rec.get(c, "")) if rec.get(c, "") != "" else None
            for c in columns
        ]
        vals.append(rec.get("_row"))
        con.execute(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
            vals,
        )


def load(
    config: ProcessorConfig | None = None,
    *,
    formatter: "BaseFormatter | None" = None,  # @MX:ANCHOR: Optional formatter for output customization (REQ-011)
    **kwargs
) -> Processor:
    """**Single entry point** — read, detect, normalise, load into DuckDB.

    This is the primary API for analysts.  Accept either a
    :class:`~duckdb_processor.config.ProcessorConfig` instance *or*
    arbitrary keyword arguments that are forwarded to the dataclass
    constructor.

    Parameters
    ----------
    config:
        A fully-populated configuration object.  When ``None``, *kwargs*
        are used to build one on the fly.
    formatter:
        Optional output formatter for customizing display (REQ-011).
        When None, uses default print() output (backward compatible).
    **kwargs:
        Any field accepted by :class:`~duckdb_processor.config.ProcessorConfig`
        (e.g. ``file``, ``header``, ``kv``, ``table``).

    Returns
    -------
    Processor
        A ready-to-use processor wrapping the loaded data.

    Raises
    ------
    ValueError
        If no data is found in the input source.

    Examples
    --------
    With a config object::

        from duckdb_processor import load, ProcessorConfig
        p = load(ProcessorConfig(file="data.csv"))

    With keyword arguments (quick start)::

        from duckdb_processor import load
        p = load(file="data.csv", table="sales")

    From stdin / notebook::

        p = load()

    With custom formatter (REQ-008)::

        from duckdb_processor.formatters import RichFormatter, OutputConfig
        formatter = RichFormatter(OutputConfig().__dict__)
        p = load(file="data.csv", formatter=formatter)
    """
    if config is None:
        config = ProcessorConfig(**kwargs)

    # 1. Read raw CSV rows
    source = config.file or "stdin"
    raw_rows = read_input(config.file)
    if not raw_rows:
        raise ValueError(f"No data found in {source}")

    # 2. Auto-detect or honour explicit flags
    has_header = (
        config.header
        if config.header is not None
        else detect_header(raw_rows)
    )
    is_kv = (
        config.kv if config.kv is not None else detect_kv(raw_rows, skip_first=has_header)
    )

    # 3. Choose loading strategy (Native DuckDB vs Python)
    con = duckdb.connect()

    if not is_kv and config.file is not None:
        try:
            # Fast native path
            header_opt = "TRUE" if has_header else "FALSE"
            query = f"CREATE TABLE {config.table} AS SELECT *, row_number() over() as _row FROM read_csv_auto('{config.file}', header={header_opt})"
            con.execute(query)
            
            existing_cols = [c[0] for c in con.execute(f"DESCRIBE {config.table}").fetchall() if c[0] != "_row"]
            if config.col_names:
                for idx, new_name in enumerate(config.col_names):
                    if idx < len(existing_cols) and existing_cols[idx] != new_name:
                        con.execute(f'ALTER TABLE {config.table} RENAME COLUMN "{existing_cols[idx]}" TO "{new_name}"')
            
            columns = [c[0] for c in con.execute(f"DESCRIBE {config.table}").fetchall() if c[0] != "_row"]
            n_records = con.execute(f"SELECT COUNT(*) FROM {config.table}").fetchone()[0]
            
            return Processor(
                con,
                columns,
                config.table,
                source=source,
                has_header=has_header,
                is_kv=is_kv,
                n_records=n_records,
                formatter=formatter,
            )
        except Exception:
            # If native loading fails for any reason, drop the table and fallback to python
            con.execute(f"DROP TABLE IF EXISTS {config.table}")

    # Fallback / KV-parsing / stdin path
    records = normalize(raw_rows, has_header, is_kv, config.col_names)

    columns = _infer_columns(records)
    _create_table(con, columns, config.table)
    _insert_records(con, columns, records, config.table)

    return Processor(
        con,
        columns,
        config.table,
        source=source,
        has_header=has_header,
        is_kv=is_kv,
        n_records=len(records),
        formatter=formatter,
    )
