# Project Structure

## Directory Overview

```text
duckdb-processor-cli/
├── duckdb_processor/              # Main package directory
│   ├── __init__.py               # Package initialization and API exports
│   ├── __main__.py               # Package execution entry point
│   ├── analysts/                 # Plugin system directory (dynamically discovered)
│   │   ├── basic_patterns.py     # Example business queries
│   │   ├── data_quality.py       # Data validation checks
│   │   └── ...                   # Other plugin modules
│   ├── formatters/               # Output formatting configuration
│   │   ├── base.py               # Formatter interface
│   │   ├── rich_formatter.py     # Advanced console outputs (styles/tables)
│   │   └── ...                   
│   ├── user_config/              # User-specific configuration overrides
│   ├── analyzer.py               # Plugin registry and BaseAnalyzer definition
│   ├── cli.py                    # Command-line interface and argument parsing
│   ├── config.py                 # Core Processor configuration classes
│   ├── detection.py              # CSV schema and Key-Value inference
│   ├── loader.py                 # Pipeline utilizing native read_csv_auto ingestion
│   ├── parsing.py                # Python-fallback normalization logic
│   ├── processor.py              # Main analytical class (SQL execution, exports)
│   ├── repl.py                   # Unified Interactive REPL with command history
│   └── utils.py                  # Helper functions (e.g. native GUI file dialogs)
├── tests/                        # Comprehensive test suite (pytest)
├── examples/                     # Usage examples
├── docs/                         # Documentation
├── main.py                       # CLI execution entry point
├── pyproject.toml                # Project configurations and dependencies
└── README.md                     # Project overview
```

## Architecture & Data Flow

### 1. Data Loading & Engine
- **`loader.py`**: Orchestrates initial data load. It relies on extremely fast, native DuckDB ingestion (`read_csv_auto`) if a valid file path is given, defaulting to python-based processing for stdin or Key-Value records.
- **`processor.py`**: The core component wrapping the active `duckdb.DuckDBPyConnection`. It handles security via parameterized execution and exposes data manipulation APIs (`sql()`, `aggregate()`, `pivot()`, `filter()`).
- **`parsing.py` & `detection.py`**: Python-level heuristics to deal with unstructured CSV variants safely.

### 2. Interface Layer
- **`cli.py`**: Main controller handling `argparse`. Directs data to either direct plugin evaluations or the interactive session.
- **`repl.py`**: The `EnhancedREPL` handles interactive standard SQL prompts, allowing analysts to write multi-line logic and access special commands (like `\schema`) directly in the shell.
- **`formatters/`**: Abstractions that dictate how outputs are parsed visually, heavily leveraging `rich` for dynamic tables.
- **`utils.py`**: GUI-level utilities separated from core logic, triggering tasks like OS folder selection.

### 3. Dynamic Plugin System (`analysts/`)
The tool employs a fully dynamic extension system to process data efficiently.

- **`analyzer.py`**: Exposes the `BaseAnalyzer` interface and `@register` decorator. It utilizes `pkgutil` to dynamically discover and auto-load any module placed in `duckdb_processor/analysts/`.
- **`analysts/`**: A sandbox where data consultants can implement domain-specific `.py` scripts. Since the registry discovers plugins automatically, developers don't have to statically import classes at the package level.