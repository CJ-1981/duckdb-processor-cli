"""CSV reading and row normalisation.

Responsibilities:
* Read raw text from a file path or stdin.
* Parse individual rows in flat or key:value format.
* Normalise all rows into a uniform ``list[dict]`` representation.
"""
from __future__ import annotations

import csv
import io
import sys
from pathlib import Path


# ── Input reading ────────────────────────────────────────────


def read_input(source: str | None) -> list[list[str]]:
    """Read CSV rows from a file path or stdin.

    Blank rows (where every cell is empty/whitespace) are silently
    dropped so that downstream code never has to handle them.

    Parameters
    ----------
    source:
        File path, or ``None`` to read from ``stdin``.

    Returns
    -------
    list[list[str]]
        Non-empty rows of string tokens.
    """
    if source and Path(source).exists():
        text = Path(source).read_text()
    else:
        text = sys.stdin.read()

    reader = csv.reader(io.StringIO(text))
    return [row for row in reader if any(v.strip() for v in row)]


# ── Row-level parsers ────────────────────────────────────────


def parse_kv_row(row: list[str]) -> dict:
    """Parse a row whose middle columns are ``key:value`` pairs.

    Expected layout::

        id, key1:val1, key2:val2, …, timestamp

    The *first* token becomes ``id`` and the *last* becomes
    ``timestamp``.  Everything in between is split on ``:`` to form
    additional keys.  Items without a colon are collected under the
    ``_unparsed`` key.

    Parameters
    ----------
    row:
        A list of raw string tokens from one CSV row.

    Returns
    -------
    dict
        Parsed key-value mapping.
    """
    row = [v.strip() for v in row]
    if len(row) < 2:
        return {"_raw": ",".join(row), "_error": "too short"}

    record: dict = {"id": row[0], "timestamp": row[-1]}

    for item in row[1:-1]:
        if ":" in item:
            key, _, val = item.partition(":")
            record[key.strip()] = val.strip()
        elif item:
            record.setdefault("_unparsed", []).append(item)

    return record


def parse_flat_row(row: list[str], header: list[str]) -> dict:
    """Parse a standard flat CSV row by zipping values against *header*.

    Short rows are padded with empty strings; long rows are truncated
    to match the header length.

    Parameters
    ----------
    row:
        Raw string tokens from one CSV row.
    header:
        Column names to map against.

    Returns
    -------
    dict
        ``{column_name: cell_value, …}``
    """
    row = [v.strip() for v in row]
    row += [""] * max(0, len(header) - len(row))
    row = row[: len(header)]
    return dict(zip(header, row))


# ── Header resolution ────────────────────────────────────────


def build_header(
    raw_rows: list[list[str]],
    has_header: bool,
    col_names: list[str] | None,
    is_kv: bool,
) -> list[str]:
    """Determine the column names for the output table.

    Resolution order:
    1. Explicit ``col_names`` (highest priority).
    2. First row of the CSV (when ``has_header`` is ``True``).
    3. Empty list for key:value format (columns are dynamic).
    4. Auto-generated ``col_0, col_1, …`` for headerless flat CSV.

    Parameters
    ----------
    raw_rows:
        All raw CSV rows (including the header row if present).
    has_header:
        Whether the first row is a header.
    col_names:
        User-supplied column names.
    is_kv:
        Whether the format is key:value pairs.

    Returns
    -------
    list[str]
        Resolved column names.
    """
    if col_names:
        return col_names

    if has_header:
        return [v.strip() for v in raw_rows[0]]

    if is_kv:
        return []

    max_cols = max(len(r) for r in raw_rows)
    return [f"col_{i}" for i in range(max_cols)]


# ── Normalisation ────────────────────────────────────────────


def normalize(
    raw_rows: list[list[str]],
    has_header: bool,
    is_kv: bool,
    col_names: list[str] | None,
) -> list[dict]:
    """Convert raw CSV rows into a list of record dictionaries.

    Each record includes a ``_row`` key tracking the original 1-based
    row number (useful for debugging or traceability).

    Parameters
    ----------
    raw_rows:
        All raw CSV rows from :func:`read_input`.
    has_header:
        Whether the first row is a header (it will be skipped).
    is_kv:
        Use :func:`parse_kv_row` when ``True``, else
        :func:`parse_flat_row`.
    col_names:
        Explicit column names (forwarded to :func:`build_header`).

    Returns
    -------
    list[dict]
        Normalised records.
    """
    header = build_header(raw_rows, has_header, col_names, is_kv)
    data_rows = raw_rows[1:] if has_header else raw_rows

    records: list[dict] = []
    start = 2 if has_header else 1
    for i, row in enumerate(data_rows, start=start):
        if is_kv:
            rec = parse_kv_row(row)
        else:
            rec = parse_flat_row(row, header)

        rec["_row"] = i
        records.append(rec)

    return records
