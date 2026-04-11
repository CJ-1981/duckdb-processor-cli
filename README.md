# DuckDB CSV Processor

**High-performance structured data analysis toolkit with DuckDB integration**

[![Tests](https://img.shields.io/badge/tests-59%20passing-brightgreen)](tests/)
[![Python](https://img.shields.io/badge/python-3.10+-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

## Features

- 🚀 **Fast CSV Loading** - Auto-detects headers, key-value pairs, and flat CSV formats
- 📊 **Rich Terminal Output** - Professional tables with colors, borders, and progress bars
- 🎨 **Formatter Options** - Rich (styled) or Simple (plain text) output modes
- 💾 **Output to File** - Save dataset info to file with optional timestamp
- 🔌 **Extensible Analysts** - Plugin system for custom analysis modules
- 🖥️ **Interactive SQL REPL** - Query your data with SQL in real-time
- ♿ **Accessible** - Screen reader support, high-contrast mode, WCAG compliant

## Installation

### From GitHub

```bash
git clone https://github.com/CJ-1981/duckdb-processor-cli.git
cd duckdb-processor-cli
pip install -e .
```

### From PyPI (Coming Soon)

```bash
pip install duckdb-processor-cli
```

### Dependencies

- Python 3.10+
- DuckDB >= 0.9.0
- Pandas >= 2.0.0
- Rich >= 13.7.0

## Quick Start

### Basic Usage

```bash
# Load and analyze a CSV file
python -m duckdb_processor data.csv

# Save output to file (default: duckdb_output_YYYYMMDD_HHMMSS.txt)
python -m duckdb_processor data.csv --output

# Save to custom file
python -m duckdb_processor data.csv --output results.txt

# Run built-in demo analyst
python -m duckdb_processor data.csv --run sample_data_demo

# Run SQL examples analyst
python -m duckdb_processor data.csv --run sample_data_sql_examples

# Interactive SQL mode
python -m duckdb_processor data.csv --interactive
```

### Multi-File Support

You can load multiple files at once for joining and cross-analysis.

```bash
# Load multiple files (auto-named by filename)
python -m duckdb_processor sales.csv users.csv --interactive

# Load with custom table mapping
python -m duckdb_processor sales.csv:sales users.csv:customers --interactive
```

Inside the interactive REPL, you can join them:
```sql
sql> SELECT * FROM sales JOIN customers ON sales.user_id = customers.id;
```

#### Gradio UI Multi-File Support
The Gradio UI also supports uploading multiple files and providing table name mappings. You can navigate between loaded tables using the "Active Table (Navigation)" dropdown.

---

### Command-Line Options

#### File Loading

```bash
# From file
python -m duckdb_processor data.csv

# From stdin
cat data.csv | python -m duckdb_processor

# Force header detection
python -m duckdb_processor data.csv --header

# Force key-value pair format
python -m duckdb_processor data.csv --kv
```

#### Output Formatting

```bash
# Rich formatter (default - colors and tables)
python -m duckdb_processor data.csv

# Simple formatter (plain text, legacy compatibility)
python -m duckdb_processor data.csv --format simple

# Disable colors
python -m duckdb_processor data.csv --no-color

# Disable progress indicators
python -m duckdb_processor data.csv --no-progress

# Save output to file
python -m duckdb_processor data.csv --output
python -m duckdb_processor data.csv -o custom_name.txt
```

#### Analyst Options

```bash
# List available analysts
python -m duckdb_processor --list-analyzers

# Run single analyst
python -m duckdb_processor data.csv --run sample_data_demo

# Run multiple analysts
python -m duckdb_processor data.csv --run sample_data_demo,sample_data_sql_examples
```

## Output Examples

### Rich Formatter (Default)

```
╭──────────────────────────────── Dataset Info ────────────────────────────────╮
│ Source: sales_data.csv                                                      │
│ Rows: 1,000                                                                 │
│ Columns: id, name, category, price, quantity                                 │
╰──────────────────────────────────────────────────────────────────────────────╯
```

### Simple Formatter

```
──────────────────────────────────────────────────────────
  DuckDB CSV Processor
──────────────────────────────────────────────────────────
  Source      : sales_data.csv
  Rows loaded : 1000
  Columns     : id, name, category, price, quantity
──────────────────────────────────────────────────────────
```

## Interactive SQL REPL

```bash
python -m duckdb_processor data.csv --interactive
```

### REPL Commands

```sql
sql> SELECT * FROM data LIMIT 5;
sql> SELECT category, COUNT(*) FROM data GROUP BY category;
sql> \schema     -- Show column names and types
sql> \coverage    -- Show column fill rates
sql> EXIT        -- Exit REPL
```

## Creating Custom Analysts

**Quick Start:** See [ANALYST_EXAMPLES.md](ANALYST_EXAMPLES.md) for a comprehensive guide with 6 ready-to-use example analysts covering:
- Basic filtering and aggregation patterns
- Time series analysis
- Data quality checks
- Business metrics and KPIs
- Python-native analysis (no SQL required)

Create a new file in `duckdb_processor/analysts/`:

```python
"""My custom analysis."""
from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class MyAnalysis(BaseAnalyzer):
    """Custom analysis description."""

    name = "my_analysis"
    description = "Description of what it does"

    def run(self, p):
        """Execute analysis using configured formatter."""

        def format_result(df):
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Run your queries
        result = p.sql("SELECT * FROM data LIMIT 10")
        format_result(result)
```

Run your analyst:

```bash
# List all available analysts
python -m duckdb_processor --list-analyzers

# Run your analyst
python -m duckdb_processor data.csv --run my_analysis

# Run multiple analysts
python -m duckdb_processor data.csv --run sample_data_demo,basic_patterns
```

### Built-in Example Analysts

- **`sample_data_demo`** - Comprehensive demo for sample_data.csv
- **`sample_data_sql_examples`** - SQL query patterns for sample_data.csv
- **`basic_patterns`** - Fundamental analysis for beginners (works with any data)
- **`time_analysis`** - Time series and trend analysis (works with any data)
- **`data_quality`** - Data validation and quality checks (works with any data)
- **`business_metrics`** - KPIs, Pareto analysis, percentiles (works with any data)
- **`python_patterns`** - Pure Python analysis (works with any data)

See [ANALYST_EXAMPLES.md](ANALYST_EXAMPLES.md) for detailed documentation of each example.

## Gradio Web Interface

A user-friendly web interface is available for interactive data analysis without using the command line.

### One-Click Launcher Scripts

The easiest way to launch the Gradio interface is using the provided launcher scripts:

**macOS / Linux:**
```bash
./run.sh
```

**Windows:**
```batch
run.bat
```

These scripts will automatically:
- ✅ Create a virtual environment (if it doesn't exist)
- ✅ Install all required dependencies
- ✅ Launch the Gradio web interface

### Manual Launch

If you prefer to launch the interface manually:

```bash
# Install UI dependencies
pip install -e ".[ui]"

# Launch Gradio app
python gradio_app.py
```

### Gradio Interface Features

- 📊 **Interactive SQL Query Builder** - Build queries without writing SQL
- 📈 **Real-time Data Preview** - View query results instantly
- 💾 **Export Results** - Download as CSV, JSON, or Markdown
- 🎨 **Dark/Light Mode** - Toggle between themes
- 📝 **Report Builder** - Generate PDF and Markdown reports
- 🔌 **Plugin System** - Test and manage custom analysts

The interface will open at `http://localhost:7860` by default.

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=duckdb_processor --cov-report=html

# Run specific test file
pytest tests/test_formatters.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Linting
ruff check duckdb_processor/ tests/

# Format code
ruff format duckdb_processor/ tests/

# Type checking
mypy duckdb_processor/
```

### Project Structure

```
duckdb_processor/
├── __init__.py          # Package initialization
├── cli.py               # Command-line interface
├── processor.py          # Core Processor API
├── loader.py             # Data loading pipeline
├── config.py             # Configuration classes
├── formatters/           # Output formatters
│   ├── base.py          # Base formatter interface
│   ├── rich_formatter.py # Rich library formatter
│   ├── simple_formatter.py # Plain text formatter
│   ├── terminal.py      # Terminal detection utilities
│   └── utils.py         # Shared utilities
├── analysts/             # Analyst plugins
│   ├── demo.py          # Built-in demo
│   └── sql_examples.py  # SQL example queries
└── user_config/          # User configuration management
```

## Configuration

### User Configuration File

Optional configuration file at `~/.duckdb_processor/config.yaml`:

```yaml
# Output formatting preferences
output:
  format: rich  # rich or simple
  color: true
  max_rows: 50

# Progress indicators
progress:
  enabled: true
  min_duration: 2.0

# Accessibility
accessibility:
  high_contrast: false
  screen_reader: false
```

## Formatter Options

### Rich Formatter

- Type-aware table formatting (numeric columns right-aligned)
- Color-coded messages (ERROR, WARNING, INFO)
- Progress bars for long operations
- Terminal width detection
- Graceful fallback to SimpleFormatter if Rich unavailable

### Simple Formatter

- Plain text output using pandas string representation
- Backward compatible with existing scripts
- No special terminal capabilities required

## Accessibility

- **Screen Reader Detection** - Automatically detects screen reader usage
- **High Contrast Mode** - Configurable high-contrast output
- **WCAG Compliance** - Color not sole information source
- **Text-Only Mode** - Preserves information without colors

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to GitHub.

### Development Setup

```bash
# Clone repository
git clone https://github.com/CJ-1981/duckdb-processor-cli.git
cd duckdb-processor-cli

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

### Submitting Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [DuckDB](https://duckdb.org/) - In-memory SQL analytics
- [Rich](https://rich.readthedocs.io/) - Terminal formatting library
- [Pandas](https://pandas.pydata.org/) - Data manipulation library

## Links

- [GitHub Repository](https://github.com/CJ-1981/duckdb-processor-cli)
- [Issue Tracker](https://github.com/CJ-1981/duckdb-processor-cli/issues)
- [Documentation](https://github.com/CJ-1981/duckdb-processor-cli/wiki)

## Changelog

### Version 1.0.0 (2026-03-29)

**Features:**
- ✅ Rich library integration for professional terminal output
- ✅ Simple formatter for backward compatibility
- ✅ Terminal capability detection (color, screen reader, width)
- ✅ CLI flags: --format, --no-color, --no-progress, --output
- ✅ Enhanced REPL with command history and auto-completion
- ✅ Test data and SQL examples

**Testing:**
- 59 tests covering formatters, CLI, integration, and accessibility
- 100% coverage on new formatter core code
- TRUST 5 quality gates verified

**Documentation:**
- Comprehensive README
- Quick start guide
- Formatter examples and comparison
- SPEC documents for development workflow
