# DuckDB CSV Processor - Entry Points Documentation

## Entry Points Overview

The DuckDB CSV Processor provides multiple entry points to accommodate different usage scenarios and user preferences. Each entry point serves a specific purpose and provides different levels of functionality and integration capabilities.

---

## Primary Entry Points

### 1. Main Entry Point - `main.py`

**Purpose**: Direct execution without installation, ideal for quick testing and one-off usage.

**Invocation Path**:
```bash
python3 main.py [options] [file.csv]
```

**Usage Examples**:
```bash
# Basic usage with CSV file
python3 main.py data.csv

# Run specific analyzer
python3 main.py data.csv --run demo

# Interactive mode
python3 main.py data.csv --interactive

# List available analyzers
python3 main.py --list-analyzers

# Read from stdin
cat data.csv | python3 main.py
```

**Features**:
- No installation required
- Full CLI functionality
- Suitable for quick testing
- Good for development and debugging
- Works with any Python 3.10+ installation

**Target Users**: Developers, data scientists, and casual users who want to try the tool without installation.

---

### 2. Module Entry Point - `__main__.py`

**Purpose**: Execution as a Python module, provides same functionality as main.py but with module semantics.

**Invocation Path**:
```bash
python -m duckdb_processor [options] [file.csv]
```

**Usage Examples**:
```bash
# Basic module execution
python -m duckdb_processor data.csv

# With analyzers
python -m duckdb_processor data.csv --run demo,step2

# Interactive mode
python -m duckdb_processor data.csv --interactive

# List analyzers
python -m duckdb_processor --list-analyzers
```

**Features**:
- Standard Python module execution
- Better integration with Python environment
- Works with virtual environments
- Suitable for production use
- Consistent with Python packaging standards

**Target Users**: Production environments, virtualized setups, and Python-centric workflows.

---

### 3. Installed Package Entry Point - `duckdb-processor`

**Purpose**: CLI command after package installation, provides the most convenient interface for regular users.

**Installation**:
```bash
# Install in development mode
pip install -e .

# Install from PyPI (when published)
pip install duckdb_processor
```

**Invocation Path**:
```bash
duckdb-processor [options] [file.csv]
```

**Usage Examples**:
```bash
# Standard execution
duckdb-processor data.csv

# With analysis pipeline
duckdb-processor data.csv --run demo,analytics

# Interactive exploration
duckdb-processor data.csv --interactive

# Available tools
duckdb-processor --list-analyzers
```

**Features**:
- No need to specify python command
- Works in any shell environment
- Easy to add to PATH
- Suitable for scripting and automation
- Best for regular usage

**Target Users**: Regular users, automated workflows, and production deployment scenarios.

---

## Secondary Entry Points

### 4. Programmatic API - `duckdb_processor.load()`

**Purpose**: Library integration for use in Python scripts, notebooks, and other applications.

**Import Path**:
```python
from duckdb_processor import load, Processor
```

**Usage Examples**:
```python
# Basic usage
from duckdb_processor import load
p = load("data.csv")
p.preview()

# With configuration
from duckdb_processor import load, ProcessorConfig
config = ProcessorConfig(file="data.csv", table="sales")
p = load(config)

# In notebooks
p = load()  # Reads from stdin/notebook input
p.aggregate("region", "amount", "SUM")
```

**Features**:
- Full programmatic access to all functionality
- Seamless integration with Python code
- Suitable for automation pipelines
- Access to all Processor methods
- Can be combined with custom analyzers

**Target Users**: Python developers, data scientists, and automation engineers.

---

### 5. Plugin Development - `analysts/` Directory

**Purpose**: Extensibility entry point for creating custom analysis modules.

**Directory Structure**:
```
duckdb_processor/
└── analysts/
    ├── __init__.py          # Auto-discovery
    ├── _template.py         # Development template
    ├── demo.py              # Built-in demo
    └── custom_analysis.py   # User-created plugins
```

**Creation Process**:
```python
# Create new analyzer
# analysts/my_analysis.py
from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class MyAnalysis(BaseAnalyzer):
    name = "my_analysis"
    description = "Custom analysis description"

    def run(self, p):
        # Implementation using Processor methods
        result = p.aggregate("region", "amount", "SUM")
        p.export_csv("my_results.csv")
```

**Usage Examples**:
```bash
# Run custom analyzer
duckdb-processor data.csv --run my_analysis

# Chain multiple analyzers
duckdb-processor data.csv --run demo,my_analysis,step2

# List all analyzers
duckdb-processor --list-analyzers
```

**Features**:
- Easy plugin development
- Automatic discovery and registration
- Full access to Processor API
- Support for analyzer chaining
- Integration with CLI and programmatic API

**Target Users**: Advanced users, data engineers, and teams with custom analysis needs.

---

## Entry Point Comparison Matrix

| Entry Point | Installation Required | CLI Access | Programmatic Use | Plugin Development | Best For |
|-------------|---------------------|------------|-----------------|-------------------|----------|
| `main.py` | No | ✓ | ✓ | ✓ | Quick testing, development |
| `__main__.py` | No | ✓ | ✓ | ✓ | Standard module execution |
| `duckdb-processor` | Yes | ✓ | ✓ | ✓ | Regular usage, production |
| `load()` function | No | ✗ | ✓ | ✓ | Library integration, automation |
| `analysts/` | No | ✓ | ✓ | ✓ | Custom analysis development |

---

## Entry Point Selection Guide

### Quick Testing and Development
**Use**: `main.py` or `__main__.py`
**Reason**: No installation required, full functionality available
**Example**: `python -m duckdb_processor test_data.csv --run demo`

### Production Usage and Deployment
**Use**: `duckdb-processor` (after installation)
**Reason**: Convenient command-line interface, easy to use in scripts
**Example**: `duckdb-processor production_data.csv --run analytics`

### Python Integration and Automation
**Use**: `load()` function
**Reason**: Seamless integration with Python code and workflows
**Example**:
```python
from duckdb_processor import load
p = load("data.csv")
results = p.aggregate("category", "amount", "SUM")
```

### Custom Analysis Development
**Use**: `analysts/` directory + CLI
**Reason**: Easy plugin development and testing
**Example**:
```python
# Create analysts/custom.py
@registry.register
class CustomAnalysis(BaseAnalyzer):
    # implementation

# Run via: duckdb-processor data.csv --run custom
```

### Scripting and Automation
**Use**: `duckdb-processor` command or `load()` function
**Reason**: Both can be automated, choice depends on environment
**Example** (shell script):
```bash
#!/bin/bash
duckdb-processor "$1" --run analysis --output results.csv
```

### Interactive Exploration
**Use: `__main__.py` or `duckdb-processor` + `--interactive`**
**Reason**: Full interactive SQL REPL with all data loaded
**Example**: `python -m duckdb_processor data.csv --interactive`

---

## Entry Point Integration Patterns

### 1. Development Workflow
```bash
# 1. Test with main.py
python3 main.py data.csv --run demo

# 2. Develop custom analyzer
# Edit analysts/my_analysis.py

# 3. Test module execution
python -m duckdb_processor data.csv --run my_analysis

# 4. Install for production
pip install -e .
duckdb-processor data.csv --run my_analysis
```

### 2. Jupyter/Notebook Integration
```python
# Library entry point for notebooks
from duckdb_processor import load

# Load data from file or notebook input
p = load("data.csv")

# Interactive analysis
p.preview()
p.aggregate("region", "amount", "SUM")

# Custom analysis
p.add_column("category", "CASE WHEN ... END")
p.export_csv("results.csv")
```

### 3. Automated Pipeline
```bash
#!/bin/bin/bash
# Automated analysis pipeline

# Input file
INPUT_FILE="$1"

# Load and run analysis pipeline
duckdb-processor "$INPUT_FILE" \
    --run demo,analytics,export \
    --output results.csv

# Next step in pipeline
process_results results.csv
```

### 4. Plugin Development Workflow
```python
# Create new analyzer
# analysts/sales_analysis.py
from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class SalesAnalysis(BaseAnalyzer):
    name = "sales_analysis"
    description = "Monthly sales trends and forecasting"

    def run(self, p):
        # Implementation
        p.add_column("month", "DATE_TRUNC('month', order_date)")
        monthly = p.aggregate("month", "amount", "SUM")
        p.export_csv("monthly_sales.csv")
```

---

## Entry Point Configuration and Environment

### Environment Variables
```bash
# PYTHONPATH - For development
export PYTHONPATH=.:$PYTHONPATH
python -m duckdb_processor data.csv

# Virtual environment
source venv/bin/activate
pip install -e .
duckdb-processor data.csv
```

### Configuration Files
```python
# Programmatic configuration
from duckdb_processor import ProcessorConfig

config = ProcessorConfig(
    file="data.csv",
    table="custom_table",
    header=True,
    interactive=True
)
p = load(config)
```

### Plugin Discovery
```python
# Plugin auto-discovery happens on import
import duckdb_processor

# All analyzers are now registered
from duckdb_processor import list_analyzers
analyzers = list_analyzers()
```

This comprehensive set of entry points provides flexibility for different usage scenarios while maintaining a consistent and intuitive user experience across all interfaces.