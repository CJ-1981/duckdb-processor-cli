#!/usr/bin/env python3
"""DuckDB CSV Processor — run directly without installing.

Usage:
    python3 main.py sample_data.csv
    python3 main.py sample_data.csv --run demo
    python3 main.py sample_data.csv --interactive
    python3 main.py --list-analyzers
    cat sample_data.csv | python3 main.py
"""

import sys
import os

# Add the current directory to sys.path so we can import the duckdb_processor package
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from duckdb_processor.cli import main

if __name__ == "__main__":
    main()
