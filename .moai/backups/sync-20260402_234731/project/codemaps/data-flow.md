# DuckDB CSV Processor - Data Flow Documentation

## Data Flow Overview

The DuckDB CSV Processor implements a **multi-stage data pipeline** that transforms raw CSV input into a powerful analytical database with extensible analysis capabilities. The flow is designed to be automatic, efficient, and transparent to users while providing extensive flexibility for custom analysis workflows.

---

## 1. CSV Loading Pipeline

### Stage 1: Input Reading
**Purpose**: Read raw CSV data from various input sources
**Components**: `parsing.read_input()`
**Input Sources**:
- File path (e.g., "data.csv")
- Standard input (stdin) via pipe redirection
- Direct file content

**Process**:
```python
def read_input(source: str | None) -> list[list[str]]:
    if source and Path(source).exists():
        text = Path(source).read_text()  # Read from file
    else:
        text = sys.stdin.read()           # Read from stdin

    reader = csv.reader(io.StringIO(text))
    return [row for row in reader if any(v.strip() for v in row)]  # Filter empty rows
```

**Key Features**:
- Handles both file and stdin input
- Removes empty rows (all whitespace cells)
- Preserves raw CSV structure
- Efficient streaming processing

---

### Stage 2: Format Detection
**Purpose**: Auto-detect CSV characteristics to determine parsing strategy
**Components**: `detection.detect_header()`, `detection.detect_kv()`
**Detection Logic**:

**Header Detection**:
- Analyzes numeric content in first row vs subsequent rows
- First row is header if it has <50% numeric tokens compared to rows 2-6
- Override capability via `--header`/`--no-header` flags

**Key:Value Detection**:
- Checks >50% of middle tokens for colon separators in sample rows
- Identifies key:value pair format in middle columns
- Override capability via `--kv`/`--no-kv` flags

**Process**:
```python
has_header = (
    config.header
    if config.header is not None
    else detect_header(raw_rows)
)

is_kv = (
    config.kv if config.kv is not None else detect_kv(raw_rows, skip_first=has_header)
)
```

**Key Features**:
- Heuristic-based automatic detection
- Fallback to explicit configuration
- Handles edge cases gracefully
- Optimized for common CSV formats

---

### Stage 3: Data Normalization
**Purpose**: Convert raw CSV rows to uniform list[dict] representation
**Components**: `parsing.normalize()`, `parsing.build_header()`, `parsing.parse_*_row()`
**Normalization Process**:

**Header Resolution**:
1. Explicit `col_names` (highest priority)
2. First row of CSV (when `has_header=True`)
3. Empty list for key:value format (dynamic columns)
4. Auto-generated `col_0, col_1, ...` for headerless flat CSV

**Row Parsing**:
- **Flat format**: `parse_flat_row()` - zips values against header
- **Key:value format**: `parse_kv_row()` - extracts key:value pairs from middle columns
- **Traceability**: Adds `_row` key with original row number

**Process**:
```python
def normalize(raw_rows, has_header, is_kv, col_names):
    header = build_header(raw_rows, has_header, col_names, is_kv)
    data_rows = raw_rows[1:] if has_header else raw_rows

    records = []
    for i, row in enumerate(data_rows, start=2 if has_header else 1):
        if is_kv:
            rec = parse_kv_row(row)
        else:
            rec = parse_flat_row(row, header)
        rec["_row"] = i  # Add traceability
        records.append(rec)

    return records
```

**Key Features**:
- Uniform output format regardless of input format
- Handles variable row lengths gracefully
- Preserves data integrity
- Adds debugging metadata

---

### Stage 4: Database Loading
**Purpose**: Load normalized data into DuckDB for efficient analytical processing
**Components**: `loader._create_table()`, `loader._insert_records()`
**Process**:

**Table Creation**:
```python
def _create_table(con, columns, table):
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
    col_defs += ", _row INTEGER"  # Traceability column
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(f"CREATE TABLE {table} ({col_defs})")
```

**Data Insertion**:
```python
def _insert_records(con, columns, records, table):
    col_list = ", ".join(f'"{c}"' for c in columns) + ", _row"
    placeholders = ", ".join(["?"] * (len(columns) + 1))

    for rec in records:
        vals = [str(rec.get(c, "")) if rec.get(c, "") != "" else None for c in columns]
        vals.append(rec.get("_row"))
        con.execute(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})", vals)
```

**Key Features**:
- Efficient batch insertion
- Type-safe VARCHAR storage with proper NULL handling
- Preserves traceability with `_row` column
- Clean table creation with proper quoting

---

### Stage 5: Processor Creation
**Purpose**: Create analytical interface for data operations
**Components**: `processor.Processor` constructor
**Process**:
```python
return Processor(
    con,                    # DuckDB connection
    columns,               # Business columns (excluding internal keys)
    config.table,         # Table name
    source=source,        # Human-readable source label
    has_header=has_header,
    is_kv=is_kv,
    n_records=len(records),
)
```

**Key Features**:
- Clean API for data operations
- Preserves loading metadata
- Ready for analytical processing
- Supports method chaining

---

## 2. Analysis Execution Flow

### Input Processing
**Purpose**: Accept and validate CLI arguments for analysis execution
**Components**: `cli.build_arg_parser()`, `cli.main()`
**Argument Processing**:

**Basic Arguments**:
- `file`: CSV file path (optional, uses stdin if omitted)
- `--header`/`--no-header`: Force header detection
- `--kv`/`--no-kv`: Force key:value format
- `--col-names`: Explicit column names for headerless data
- `--table`: Custom table name

**Analysis Arguments**:
- `--run`: Comma-separated analyzer names
- `--list-analyzers`: Show available analyzers
- `--interactive`: Drop into SQL REPL

**Process**:
```python
config = ProcessorConfig(
    file=args.file,
    header=args.header,
    kv=args.kv,
    col_names=col_names,
    table=args.table,
    interactive=args.interactive,
)
```

**Key Features**:
- Comprehensive argument validation
- Flexible configuration options
- Clear help and usage examples
- Error handling for invalid inputs

---

### Analysis Discovery
**Purpose**: Find and prepare requested analyzers for execution
**Components**: `analyzer.list_analyzers()`, `analyzer.get_analyzer()`
**Discovery Process**:

**Registry Population**:
- Auto-import of `analysts/` modules
- Trigger `@register` decorators during import
- Build metadata list for CLI display

**Analyzer Resolution**:
```python
def run_analyzers(p, names):
    for name in names:
        analyzer = get_analyzer(name)  # Get from registry
        desc = analyzer.description
        print(f"\n{'─' * 58}")
        print(f"  [{name}] {desc}")
        print(f"{'─' * 58}")
        analyzer.run(p)  # Execute with processor
```

**Key Features**:
- Automatic plugin discovery
- Clear execution progress feedback
- Error handling for missing analyzers
- Support for analyzer chaining

---

### Analysis Execution
**Purpose**: Execute analyzer business logic with full access to data
**Components**: `BaseAnalyzer.run()` implementations
**Execution Process**:

**Plugin Interface**:
```python
class DemoAnalysis(BaseAnalyzer):
    name = "demo"
    description = "Built-in demo of Processor methods"

    def run(self, p):
        # Business logic using Processor methods
        p.add_column("tier", "CASE WHEN ... END")
        result = p.aggregate("region", "amount", "SUM")
        p.export_csv("results.csv")
```

**Common Operations**:
- **Data Exploration**: `p.preview()`, `p.coverage()`, `p.schema()`
- **Data Transformation**: `p.add_column()`, `p.create_view()`
- **Data Analysis**: `p.aggregate()`, `p.pivot()`, `p.sql()`
- **Data Export**: `p.export_csv()`, `p.export_json()`, `p.export_parquet()`

**Key Features**:
- Full access to Processor API
- Stateful execution (analyzers modify data)
- Method chaining support
- Error handling and feedback

---

### Interactive Mode
**Purpose**: Provide interactive SQL REPL for data exploration
**Components**: `cli.interactive_repl()`
**REPL Features**:

**Special Commands**:
- `EXIT`/`QUIT`/`\q`: Exit REPL
- `\schema`: Show table schema
- `\coverage`: Show column coverage
- All other commands executed as SQL

**Process**:
```python
while True:
    try:
        query = input("\nsql> ").strip()
        if query.upper() in ("EXIT", "QUIT", "\\Q"):
            break
        elif query == "\\schema":
            print(p.schema().to_string(index=False))
        elif query == "\\coverage":
            print(p.coverage().to_string(index=False))
        else:
            print(p.sql(query).to_string(index=False))
    except Exception as e:
        print(f"  {e}")
```

**Key Features**:
- Full SQL access to loaded data
- Context-aware special commands
- Error handling for invalid queries
- Useful debugging commands

---

## 3. Plugin Discovery Mechanism

### Module Auto-Discovery
**Purpose**: Automatically find and register all analyzer plugins
**Components**: `analysts/__init__.py`
**Discovery Process**:

**Iteration and Import**:
```python
for _importer, _modname, _ispkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_modname}")
```

**Registration Trigger**:
- Module import triggers `@register` decorators
- Each `@register` call adds analyzer to global registry
- Registry populated with metadata (name, description, class)

**Key Features**:
- Zero-configuration plugin discovery
- Automatic registration on import
- Support for multiple analyzers per module
- Clean separation between discovery and execution

---

### Plugin Registration
**Purpose**: Register analyzer classes with the global registry
**Components**: `analyzer.register()` decorator
**Registration Process**:

**Decorator Implementation**:
```python
def register(cls: type[BaseAnalyzer]) -> type[BaseAnalyzer]:
    instance = cls()
    if not instance.name:
        raise ValueError(f"{cls.__name__} must define a non-empty 'name'")
    _registry[instance.name] = cls
    return cls
```

**Registration Requirements**:
- Class must define `name` attribute
- Class must implement `run()` method
- Class should define `description` attribute
- Automatically added to global registry

**Key Features**:
- Simple one-line registration
- Automatic validation
- Global registry access
- Support for class-level attributes

---

### Plugin Management
**Purpose**: Provide registry management functions for discovery and execution
**Components**: `analyzer.list_analyzers()`, `analyzer.get_analyzer()`
**Management Functions**:

**List Available Analyzers**:
```python
def list_analyzers() -> list[dict]:
    return [
        {"name": name, "description": cls().description}
        for name, cls in sorted(_registry.items())
    ]
```

**Get Analyzer Instance**:
```python
def get_analyzer(name: str) -> BaseAnalyzer:
    if name not in _registry:
        available = ", ".join(sorted(_registry)) or "(none registered)"
        raise KeyError(f"Analyzer '{name}' not found. Available: {available}")
    return _registry[name]()
```

**Key Features**:
- Registry introspection
- Error handling for missing analyzers
- Metadata extraction for display
- Instance creation on demand

---

## 4. Request Lifecycle Examples

### Basic Data Loading
```bash
# Command: duckdb-processor data.csv
# Flow: CLI → load() → read_input() → detect_header() → normalize() → create_table() → insert_records() → Processor

# Steps:
1. cli.main() parses arguments
2. loader.load() with default config
3. parsing.read_input() reads data.csv
4. detection.detect_header() auto-detects header
5. parsing.normalize() converts to list[dict]
6. loader._create_table() creates DuckDB table
7. loader._insert_records() loads data
8. Returns Processor instance
9. Prints info banner
```

### Analysis Pipeline
```bash
# Command: duckdb-processor data.csv --run demo,analytics
# Flow: CLI → load() → DemoAnalysis.run() → AnalyticsAnalysis.run()

# Steps:
1. Same as basic loading for data.csv
2. cli.main() detects --run argument
3. analyzer.run_analyzers() starts execution
4. DemoAnalysis.run() executes:
   - p.coverage() → SQL: SELECT coverage for each column
   - p.preview() → SQL: SELECT * FROM data LIMIT 10
   - p.add_column() → ALTER TABLE + UPDATE for tier column
   - p.aggregate() → GROUP BY query for region, SUM(amount)
5. AnalyticsAnalysis.run() executes:
   - Custom business logic
   - Additional data transformations
   - Export results to file
```

### Interactive Exploration
```bash
# Command: duckdb-processor data.csv --interactive
# Flow: CLI → load() → interactive_repl() → User queries

# Steps:
1. Same as basic loading for data.csv
2. cli.main() detects --interactive argument
3. cli.interactive_repl() starts
4. User enters "SELECT COUNT(*) FROM data"
5. p.sql("SELECT COUNT(*) FROM data") executes
6. Returns result as DataFrame
7. Formats and displays to user
8. User continues until EXIT command
```

### Plugin Development
```python
# File: analysts/sales_report.py
# Flow: Import → @register → Registry population → CLI discovery

# Steps:
1. User creates analysts/sales_report.py
2. File contains @register decorator
3. Package import triggers importlib.import_module()
4. @register decorator adds to _registry
5. CLI --list-analyzers shows "sales_report"
6. CLI --run sales_report executes SalesReport.run()
```

This comprehensive data flow enables automatic, efficient processing of CSV data while providing powerful extensible analysis capabilities through the plugin system. Each stage is optimized for performance and usability while maintaining clean interfaces between components.