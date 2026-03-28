"""DuckDB CSV Processor — structured data analysis toolkit.

The top-level ``__init__`` re-exports the small public API that data
analysts need.  Everything else (detection heuristics, row parsing,
DuckDB DDL generation, etc.) is internal infrastructure that analysts
never have to touch.

Quick start (Python)
--------------------
::

    from duckdb_processor import load, Processor

    p = load("data.csv")
    p.preview()
    p.add_column("tier", "CASE WHEN ... END")
    p.aggregate("region", "amount", "SUM")
    p.export_csv("results.csv")

Quick start (CLI)
-----------------
::

    python -m duckdb_processor data.csv --run demo
    python -m duckdb_processor data.csv --run demo --interactive
    python -m duckdb_processor --list-analyzers

Writing a new analyzer
----------------------
::

    from duckdb_processor.analyzer import BaseAnalyzer, register

    @register
    class MyAnalysis(BaseAnalyzer):
        name = "my_analysis"
        description = "My custom analysis"

        def run(self, p: "Processor"):
            result = p.sql("SELECT region, SUM(amount) FROM data GROUP BY 1")
            p.export_csv("my_output.csv")
"""
import duckdb_processor.analysts  # noqa: F401, E402

from .analyzer import (
    BaseAnalyzer,
    get_analyzer,
    list_analyzers,
    register,
    run_analyzers,
)
from .config import ProcessorConfig
from .loader import load
from .processor import Processor

# Trigger auto-discovery of all analyzers in the analysts/ subpackage.

__all__ = [
    "BaseAnalyzer",
    "Processor",
    "ProcessorConfig",
    "get_analyzer",
    "list_analyzers",
    "load",
    "register",
    "run_analyzers",
]
