# DuckDB CSV Processor - Module Catalog

## Module Structure Overview

The DuckDB CSV Processor follows a **layered architecture** with four distinct layers, each with specific responsibilities and clear interfaces between layers.

---

## Infrastructure Layer

### `loader.py` - Data Loading Pipeline

**Purpose**: Primary entry point orchestrating the complete data loading pipeline from raw CSV to ready-to-use Processor instance.

**Key Functions**:
- `load()` - Main entry point (primary API)
- `_infer_columns()` - Collect user-facing column names in first-seen order
- `_create_table()` - Create DuckDB table with VARCHAR columns
- `_insert_records()` - Insert normalized records into DuckDB table

**Responsibilities**:
- Coordinate all loading phases (read → detect → normalize → load)
- Handle configuration building and validation
- Manage DuckDB connection and table creation
- Return configured Processor instance

**Public Interface**:
- `load(config=None, **kwargs)` - Primary API entry point

**Internal Dependencies**:
- `config.ProcessorConfig` - Configuration object
- `detection.detect_header/detect_kv` - Format detection
- `parsing.read_input/normalize` - Data reading and normalization
- `processor.Processor` - Return type

---

### `detection.py` - Format Detection Heuristics

**Purpose**: Implement heuristic detection of CSV format characteristics (header presence and key:value format).

**Key Functions**:
- `detect_header(rows)` - Guess if first row is a header
- `detect_kv(rows, skip_first=False)` - Guess if middle columns contain key:value pairs
- `_is_numeric(v)` - Check if value can be parsed as float

**Responsibilities**:
- Analyze raw CSV rows for format characteristics
- Apply heuristics to determine header presence
- Detect key:value format patterns
- Provide numeric content analysis

**Detection Heuristics**:
- **Header Detection**: First row has <50% numeric tokens compared to subsequent rows
- **Key:Value Detection**: >50% of middle tokens contain colons in sample rows
- **Numeric Detection**: Float parsing with graceful failure

**Internal Dependencies**:
- None (pure utility functions)

---

### `parsing.py` - CSV Reading and Normalization

**Purpose**: Read raw CSV data and normalize it into a uniform list[dict] representation.

**Key Functions**:
- `read_input(source)` - Read CSV from file or stdin
- `parse_kv_row(row)` - Parse key:value format row
- `parse_flat_row(row, header)` - Parse standard flat CSV row
- `build_header(raw_rows, has_header, col_names, is_kv)` - Determine column names
- `normalize(raw_rows, has_header, is_kv, col_names)` - Convert to list[dict]

**Responsibilities**:
- Handle input reading from various sources
- Parse different CSV formats (flat, key:value)
- Resolve column names with multiple strategies
- Normalize data to uniform representation
- Add traceability metadata (_row numbering)

**Format Support**:
- **Flat CSV**: Standard columnar format
- **Key:Value**: Dynamic column format with key:value pairs
- **Header/No-Header**: Both supported with auto-detection
- **Mixed Length**: Handles varying row lengths gracefully

**Internal Dependencies**:
- `csv` module - CSV parsing
- `pathlib.Path` - File path handling
- Standard library utilities

---

## Domain API Layer

### `processor.py` - Analyst-Facing API

**Purpose**: High-level data analysis API wrapping DuckDB operations for data analysts.

**Key Components**:
- **Processor Class** - Main business logic interface
- **Metadata Methods** - Dataset information and schema
- **Core SQL Methods** - Direct SQL execution and data access
- **Filter Methods** - Data filtering and view creation
- **Derive Methods** - Column derivation and transformation
- **Aggregate Methods** - Group-by aggregations
- **Pivot Methods** - Cross-tabulation operations
- **Export Methods** - Data export in multiple formats

**Public Methods**:
- **Metadata**: `info()`, `print_info()`, `schema()`, `coverage()`
- **Core SQL**: `sql(query)`, `preview(n)`, `filter(where)`, `create_view(name, where)`
- **Derive**: `add_column(new_col, expr, source=None)`
- **Aggregate**: `aggregate(group_by, agg_field, func="SUM", source=None)`
- **Pivot**: `pivot(row_key, col_key, val, func="SUM", source=None)`
- **Export**: `export_csv(path, query=None)`, `export_json(path, query=None)`, `export_parquet(path, query=None)`

**Responsibilities**:
- Provide high-level interface to DuckDB operations
- Handle SQL generation for common analytical tasks
- Manage data type conversions and error handling
- Enable method chaining for complex workflows
- Export data in multiple formats

**Internal Dependencies**:
- `duckdb` - Database connection and SQL execution
- `pandas` - DataFrame operations for results
- `json` - JSON export formatting
- `pathlib.Path` - File path operations

---

### `analyzer.py` - Plugin Framework

**Purpose**: Abstract base class and registry system for extensible analysis modules.

**Key Components**:
- **BaseAnalyzer Abstract Class** - Plugin interface definition
- **Registry System** - Global analyzer registry
- **Registration Decorator** - Simple plugin registration
- **Plugin Management** - Discovery, instantiation, and execution

**Abstract Class**:
```python
class BaseAnalyzer(ABC):
    name: str = ""
    description: str = ""

    @abstractmethod
    def run(self, p: Processor) -> None:
        pass
```

**Registry Functions**:
- `register(cls)` - Decorator for plugin registration
- `get_analyzer(name)` - Instantiate analyzer by name
- `list_analyzers()` - List all registered analyzers
- `run_analyzers(p, names)` - Execute multiple analyzers

**Responsibilities**:
- Define plugin interface and contract
- Manage global analyzer registry
- Enable automatic plugin discovery
- Provide plugin execution framework
- Support analyzer chaining and sequencing

**Internal Dependencies**:
- `abc` - Abstract base class support
- `typing.TYPE_CHECKING` - Forward reference support

---

## Application Layer

### `cli.py` - Command Line Interface

**Purpose**: Command-line interface for the DuckDB CSV Processor with argument parsing and execution orchestration.

**Key Functions**:
- `build_arg_parser()` - Construct argument parser
- `interactive_repl(p)` - Interactive SQL REPL
- `main(argv=None)` - CLI entry point

**CLI Features**:
- **File Input**: CSV file or stdin support
- **Format Options**: Force header/no-header, force key:value/no-key:value
- **Column Naming**: Explicit column names for headerless data
- **Analysis Execution**: Run named analyzers individually or in sequence
- **Interactive Mode**: Drop into SQL REPL after loading
- **Analyzer Listing**: List all available analyzers

**Argument Parsing**:
- Mutually exclusive groups for format options
- Comprehensive help text with usage examples
- Validation and error handling
- Flexible argument combinations

**Interactive REPL**:
- Special commands: EXIT/QUIT, \schema, \coverage
- SQL execution with error handling
- Schema and coverage inspection
- Graceful exit handling

**Responsibilities**:
- Parse command-line arguments
- Load data based on configuration
- Execute requested analyzers
- Provide interactive interface
- Handle errors and user feedback

**Internal Dependencies**:
- `argparse` - Command-line parsing
- `sys` - System utilities
- `analyzer` - Analyzer functions
- `config` - Configuration classes
- `loader` - Data loading functions

---

### `__init__.py` - Package Initialization

**Purpose**: Package initialization and public API re-export.

**Exported Components**:
- **Core API**: `load`, `Processor`, `ProcessorConfig`
- **Plugin Framework**: `BaseAnalyzer`, `register`, `list_analyzers`, `run_analyzers`
- **Analyzer Functions**: `get_analyzer`

**Auto-Discovery**:
- Imports `duckdb_processor.analysts` for plugin registration
- Triggers `@register` decorators via module import
- Makes all analyzers available to CLI and programs

**Public Interface**:
```python
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
```

**Responsibilities**:
- Define public API surface
- Enable plugin auto-discovery
- Provide clear import structure
- Centralize package-level exports

---

### `config.py` - Configuration Management

**Purpose**: Configuration dataclass for specifying how CSV data should be read and loaded.

**Configuration Class**:
```python
@dataclass
class ProcessorConfig:
    file: str | None = None
    header: bool | None = None
    kv: bool | None = None
    col_names: list[str] | None = None
    table: str = "data"
    run_analyzers: list[str] = field(default_factory=list)
    interactive: bool = False
```

**Configuration Options**:
- **Input Source**: File path or stdin
- **Format Detection**: Override or auto-detect header/key:value
- **Column Naming**: Explicit names for headerless data
- **Table Name**: Custom DuckDB table name
- **Analysis Pipeline**: Analyzers to execute after loading
- **Interactive Mode**: Enable SQL REPL

**Responsibilities**:
- Encapsulate all configuration options
- Provide sensible defaults
- Support multiple configuration methods
- Enable programmatic and CLI usage

**Internal Dependencies**:
- `dataclasses.dataclass` - Dataclass definition
- `dataclasses.field` - Field with default factory

---

## Plugin Layer

### `analysts/__init__.py` - Plugin Auto-Discovery

**Purpose**: Auto-discover and register all analyzers in the analysts package.

**Key Functions**:
- Import-time module discovery using `pkgutil.iter_modules()`
- Automatic import of all analyst modules
- Trigger registration via module import

**Discovery Process**:
1. Iterate over all modules in the analysts directory
2. Import each module automatically
3. Trigger `@register` decorators during import
4. Make all analyzers available to the registry

**Responsibilities**:
- Enable plugin auto-discovery
- Simplify plugin development (no registration calls needed)
- Ensure all plugins are available to CLI and API
- Maintain clean plugin structure

---

### `analysts/_template.py` - Plugin Development Template

**Purpose**: Template for creating new analysis modules.

**Template Features**:
- Complete example analyzer implementation
- `@register` decorator usage
- Available Processor methods documentation
- Best practices and examples

**Template Structure**:
```python
@register
class MyAnalysis(BaseAnalyzer):
    name = "my_analysis"
    description = "One-line summary"

    def run(self, p):
        # Implementation using Processor methods
        pass
```

**Responsibilities**:
- Provide starting point for new analyzers
- Demonstrate plugin development patterns
- Document available Processor methods
- Ensure consistent plugin structure

---

### `analysts/demo.py` - Built-in Demo Analysis

**Purpose**: Example analysis demonstrating all major Processor capabilities.

**Demo Features**:
- Complete analysis workflow
- All Processor method demonstrations
- Real-world usage patterns
- Educational value for users

**Demo Workflow**:
1. Show coverage and preview data
2. Add derived column (tier categorization)
3. Filter data based on conditions
4. Perform aggregations by different dimensions
5. Create pivot tables for cross-tabulation
6. Execute complex ad-hoc SQL queries

**Responsibilities**:
- Demonstrate system capabilities
- Provide learning example
- Test all Processor methods
- Showcase best practices

---

## Module Relationships and Interfaces

### Layer Communication
- **Infrastructure → Domain**: Loaded data and configuration
- **Domain → Application**: Processor API and plugin execution
- **Application → Plugin**: Analyzer execution context
- **Plugin ↔ Domain**: Business logic execution and data access

### Key Interfaces
- **Processor Interface**: High-level data operations
- **Plugin Interface**: `BaseAnalyzer.run()` method
- **Configuration Interface**: `ProcessorConfig` dataclass
- **CLI Interface**: Argument parsing and user interaction

### Data Flow
1. CLI → Configuration → Loader → DuckDB
2. Processor → Plugin → Data Operations → Results
3. Export → File Output → User Consumption

This modular architecture enables clean separation of concerns, independent development of components, and easy extension through the plugin system while maintaining a cohesive, usable API for data analysts.