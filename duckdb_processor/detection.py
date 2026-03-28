"""Heuristic detection of CSV format characteristics.

These functions inspect raw rows and guess whether the file has a
header row and/or uses key:value-pair columns.  They are called
automatically by :func:`~duckdb_processor.loader.load` when the
corresponding flags are left as ``None`` in
:class:`~duckdb_processor.config.ProcessorConfig`.
"""
from __future__ import annotations


def detect_header(rows: list[list[str]]) -> bool:
    """Guess whether the first row is a column header.

    Heuristic
    ---------
    The first row is deemed a header when its values are *less numeric*
    than the values in subsequent rows.  Specifically, if the first row
    has fewer than half the average numeric-token count of rows 2-6,
    it is classified as a header.

    Parameters
    ----------
    rows:
        Raw CSV rows (each row is a list of string tokens).

    Returns
    -------
    bool
        ``True`` if the first row looks like a header.
    """
    if len(rows) < 2:
        return False

    first = rows[0]
    rest = rows[1 : min(6, len(rows))]

    numeric_in_first = sum(1 for v in first if _is_numeric(v))
    numeric_in_rest = [sum(1 for v in row if _is_numeric(v)) for row in rest]

    avg_rest_numeric = (
        sum(numeric_in_rest) / len(numeric_in_rest) if numeric_in_rest else 0
    )

    return numeric_in_first < avg_rest_numeric * 0.5


def detect_kv(rows: list[list[str]], skip_first: bool = False) -> bool:
    """Guess whether the middle columns contain ``key:value`` pairs.

    Heuristic
    ---------
    If more than 50 % of the *middle* tokens (everything except the
    first and last column) across a sample of up to 20 rows contain a
    colon, the file is classified as key:value format.

    Parameters
    ----------
    rows:
        Raw CSV rows.
    skip_first:
        Set ``True`` when the first row has already been identified as
        a header (so it should not influence the detection).

    Returns
    -------
    bool
        ``True`` if the rows appear to use key:value pairs.
    """
    sample = rows[1:] if skip_first else rows
    sample = sample[:20]
    total = 0
    kv_count = 0

    for row in sample:
        middle = row[1:-1]
        for item in middle:
            if item.strip():
                total += 1
                if ":" in item:
                    kv_count += 1

    return (kv_count / total) > 0.5 if total > 0 else False


def _is_numeric(v: str) -> bool:
    """Return ``True`` if *v* can be parsed as a float."""
    try:
        float(v.strip())
        return True
    except ValueError:
        return False
