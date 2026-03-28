"""Configuration for the DuckDB CSV Processor.

Analysts can create a config directly in notebooks or scripts:

    config = ProcessorConfig(file="sales.csv", table="sales")
    p = load(config)
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProcessorConfig:
    """All knobs for controlling how CSV data is read and loaded.

    Attributes:
        file:          Path to a CSV file. ``None`` means read from stdin.
        header:        ``True`` / ``False`` forces the setting;
                       ``None`` (default) triggers auto-detection.
        kv:            ``True`` / ``False`` forces key:value mode;
                       ``None`` triggers auto-detection.
        col_names:     Explicit column names used when there is no header
                       and the format is flat (not k:v).
        table:         DuckDB table name to create (default ``"data"``).
        run_analyzers: List of analyzer names to execute after loading.
        interactive:   Drop into the SQL REPL after loading.
    """

    file: str | None = None
    header: bool | None = None
    kv: bool | None = None
    col_names: list[str] | None = None
    table: str = "data"
    run_analyzers: list[str] = field(default_factory=list)
    interactive: bool = False
