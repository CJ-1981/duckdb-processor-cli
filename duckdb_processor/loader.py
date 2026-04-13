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
    con.execute(f'DROP TABLE IF EXISTS "{table}"')
    con.execute(f'CREATE TABLE "{table}" ({col_defs})')


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
            f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})',
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

    con = duckdb.connect()
    files = config.files if config.files else [None]
    
    first_table = None
    first_columns = None
    first_meta = {}
    all_tables_info = {}

    import os
    import re

    for idx, file_spec in enumerate(files):
        file_path = file_spec
        table_name = config.table if idx == 0 else f"data_{idx}"
        
        if file_spec and isinstance(file_spec, str) and ":" in file_spec and not os.path.exists(file_spec):
            # rudimentary check to avoid breaking Windows absolute paths like C:\foo...
            # we assume if it exists, it's not a mapping. Or we could just split from the right
            parts = file_spec.rsplit(":", 1)
            # if parts[0] is not empty and the file exists (or parts[0] is just a file)
            if len(parts) == 2 and not os.path.isabs(file_spec) or (os.path.isabs(parts[0]) and os.path.exists(parts[0])):
                file_path = parts[0]
                table_name = parts[1]
            elif len(parts) == 2 and parts[0] == "" and len(parts[1]) == 1:
                # C:\ case on windows fallback
                pass
            else:
                file_path = parts[0]
                table_name = parts[1]

        if file_path == file_spec and file_spec and isinstance(file_spec, str):
            stem = os.path.splitext(os.path.basename(file_path))[0]
            stem = re.sub(r'\W+', '_', stem)
            if stem:
                table_name = stem
                # Use config.table if explicitly set for single file or fallback
                if len(files) == 1 and config.table != "data":
                    table_name = config.table

        # 1. Read raw CSV rows
        source = file_path or "stdin"
        raw_rows = read_input(file_path)
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
        
        meta = {
            "source": source,
            "has_header": has_header,
            "is_kv": is_kv,
        }

        # 3. Choose loading strategy
        loaded_native = False
        columns = []
        n_records = 0

        if not is_kv and file_path is not None:
            try:
                # Fast native path
                header_opt = "TRUE" if has_header else "FALSE"
                query = f'CREATE TABLE "{table_name}" AS SELECT *, row_number() over() as _row FROM read_csv_auto(\'{file_path}\', header={header_opt})'
                con.execute(query)
                
                existing_cols = [c[0] for c in con.execute(f'DESCRIBE "{table_name}"').fetchall() if c[0] != "_row"]
                if config.col_names:
                    for c_idx, new_name in enumerate(config.col_names):
                        if c_idx < len(existing_cols) and existing_cols[c_idx] != new_name:
                            con.execute(f'ALTER TABLE "{table_name}" RENAME COLUMN "{existing_cols[c_idx]}" TO "{new_name}"')
                
                columns = [c[0] for c in con.execute(f'DESCRIBE "{table_name}"').fetchall() if c[0] != "_row"]
                n_records = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                loaded_native = True
            except Exception:
                con.execute(f'DROP TABLE IF EXISTS "{table_name}"')

        if not loaded_native:
            # Fallback / KV-parsing / stdin path
            records = normalize(raw_rows, has_header, is_kv, config.col_names)
            columns = _infer_columns(records)
            _create_table(con, columns, table_name)
            _insert_records(con, columns, records, table_name)
            n_records = len(records)
            
        meta["n_records"] = n_records
        all_tables_info[table_name] = {"columns": columns, "meta": meta}
        
        if first_table is None:
            first_table = table_name
            first_columns = columns
            first_meta = meta

    p = Processor(
        con,
        first_columns,
        first_table,
        source=first_meta.get("source", ""),
        has_header=first_meta.get("has_header", False),
        is_kv=first_meta.get("is_kv", False),
        n_records=first_meta.get("n_records", 0),
        formatter=formatter,
    )
    p._tables_info = all_tables_info
    return p
