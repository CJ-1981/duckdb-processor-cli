# Project Structure

## Directory Overview

```
duckdb-processor/
├── duckdb_processor/              # Main package directory
│   ├── __init__.py               # Package initialization and API exports
│   ├── main.py                   # Command-line entry point
│   ├── analysts/                 # Plugin system directory
│   │   ├── __init__.py          # Plugin system initialization
│   │   ├── base_analyst.py      # Base analyst class and interface
│   │   ├── csv_analyst.py       # CSV-specific analysis functions
│   │   ├── stats_analyst.py     # Statistical analysis functions
│   │   └── transform_analyst.py # Data transformation functions
│   ├── core/                    # Core functionality modules
│   │   ├── __init__.py         # Core module initialization
│   │   ├── processor.py        # Main Processor class implementation
│   │   ├── config.py            # Configuration management
│   │   └── utils.py             # Utility functions and helpers
│   ├── exporters/               # Data export functionality
│   │   ├── __init__.py         # Exporters module initialization
│   │   ├── csv_exporter.py     # CSV export implementation
│   │   ├── json_exporter.py    # JSON export implementation
│   │   └── parquet_exporter.py # Parquet export implementation
│   └── cli/                     # Command-line interface
│       ├── __init__.py         # CLI module initialization
│       ├── main.py             # CLI command handlers
│       └── commands.py         # Individual command implementations
├── tests/                       # Test suite
│   ├── __init__.py            # Test package initialization
│   ├── test_processor.py      # Core functionality tests
│   ├── test_analysts.py       # Plugin system tests
│   ├── test_exporters.py      # Export functionality tests
│   └── test_cli.py            # Command-line interface tests
├── examples/                    # Usage examples and tutorials
│   ├── basic_usage.py          # Basic API usage example
│   ├── advanced_analysis.py    # Advanced analysis techniques
│   └── batch_processing.py    # Batch processing examples
├── docs/                        # Documentation
│   ├── user_guide.md          # User guide and getting started
│   ├── api_reference.md        # Complete API documentation
│   └── examples.md            # Additional examples and use cases
├── main.py                      # Direct execution entry point
├── pyproject.toml              # Modern Python packaging configuration
├── requirements.txt            # Python dependencies
├── README.md                   # Project overview and quick start
└── .gitignore                  # Git ignore patterns
```

## Directory Purpose

### Main Package (`duckdb_processor/`)

**Purpose**: Core package containing all functionality for the DuckDB CSV Processor.

**Key Components**:
- `__init__.py` - Package initialization, exports the main API (load, Processor)
- `main.py` - Direct script execution entry point
- `analysts/` - Plugin system for analysis functions
- `core/` - Core processing engine and utilities
- `exporters/` - Data export functionality
- `cli/` - Command-line interface implementation

### Plugin System (`duckdb_processor/analysts/`)

**Purpose**: Extensible framework for adding custom analysis functions through the `@register` decorator.

**Architecture**:
- `base_analyst.py` - Base class defining the analyst interface
- `csv_analyst.py` - CSV-specific analysis functions
- `stats_analyst.py` - Statistical analysis functions (mean, median, std, correlations)
- `transform_analyst.py` - Data transformation functions (filtering, pivoting, merging)

**Key Features**:
- Decorator-based plugin registration
- Standardized interface for all analyst functions
- Automatic plugin discovery and loading
- Support for custom analysis logic

### Core Engine (`duckdb_processor/core/`)

**Purpose**: Main processing engine and core functionality.

**Components**:
- `processor.py` - Main `Processor` class with analyst-facing API
- `config.py` - Configuration management and settings
- `utils.py` - Utility functions for data processing

**API Methods**:
- `sql()` - Execute raw SQL queries
- `filter()` - Apply data filtering conditions
- `add_column()` - Add calculated columns
- `aggregate()` - Perform aggregations
- `pivot()` - Create pivot tables

### Export System (`duckdb_processor/exporters/`)

**Purpose**: Data export functionality supporting multiple formats.

**Export Formats**:
- `csv_exporter.py` - CSV file export
- `json_exporter.py` - JSON file export
- `parquet_exporter.py` - Parquet file export

**Features**:
- Format-specific optimization
- Streaming support for large datasets
- Compression options
- Custom formatting capabilities

### Command-Line Interface (`duckdb_processor/cli/`)

**Purpose**: User-friendly command-line interface.

**Components**:
- `main.py` - CLI entry point and command routing
- `commands.py` - Individual command implementations

**CLI Commands**:
- `duckdb-processor` - Main command with interactive mode
- Interactive SQL shell
- Batch processing commands
- Configuration management

### Test Suite (`tests/`)

**Purpose**: Comprehensive test coverage for quality assurance.

**Test Categories**:
- `test_processor.py` - Core functionality and API testing
- `test_analysts.py` - Plugin system testing
- `test_exporters.py` - Export functionality testing
- `test_cli.py` - Command-line interface testing

### Examples (`examples/`)

**Purpose**: Learning resources and usage patterns.

**Example Types**:
- `basic_usage.py` - Getting started with the API
- `advanced_analysis.py` - Complex analysis workflows
- `batch_processing.py` - Bulk data processing examples

## Key Files and Their Roles

### Entry Points
- `main.py` - Direct script execution: `python main.py`
- `pyproject.toml` - Package configuration for modern Python distribution
- `duckdb_processor/__init__.py` - Module imports: `from duckdb_processor import load, Processor`

### Architecture Files
- `duckdb_processor/core/processor.py` - Main processing engine
- `duckdb_processor/analysts/base_analyst.py` - Plugin system foundation
- `duckdb_processor/cli/main.py` - Command-line interface

### Configuration
- `pyproject.toml` - Build system and dependencies
- `requirements.txt` - Runtime dependencies
- `duckdb_processor/core/config.py` - Application configuration

## Plugin System Architecture

### Plugin Registration
Analyst functions are registered using the `@register` decorator:

```python
@register
def my_analysis_function(data, *args, **kwargs):
    # Analysis logic here
    return result
```

### Plugin Loading
The plugin system automatically discovers and loads all analyst functions:
- Scans the `analysts/` directory
- Imports modules and finds decorated functions
- Registers functions with the main processor

### Plugin Categories
- **Analyst Functions** - Data analysis and transformation
- **Export Functions** - Data format export capabilities
- **Extension Points** - Customizable processing hooks

## Module Relationships

### Data Flow
1. **Input** - CSV files loaded into DuckDB
2. **Processing** - Analyst functions apply transformations
3. **Analysis** - SQL queries and aggregations
4. **Output** - Results exported in desired format

### Dependencies
- **Core** depends on `analysts/` for analysis capabilities
- **CLI** depends on `core/` and `exporters/` for functionality
- **Tests** cover all modules for quality assurance

### Extensibility
- New analyst functions added via plugin system
- New export formats implemented in `exporters/`
- Custom processing logic in `core/` extensions

This structure provides a solid foundation for the DuckDB CSV Processor, balancing flexibility, performance, and maintainability.