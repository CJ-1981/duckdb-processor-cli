# DuckDB CSV Processor - Architecture Documentation

## Overview

This directory contains comprehensive architecture documentation (codemaps) for the DuckDB CSV Processor, a plugin-based CSV data analysis toolkit. The documentation provides deep insights into the system architecture, module relationships, data flows, and design patterns.

## Documentation Structure

### 📁 Core Documentation

- **[overview.md](overview.md)** - High-level architecture summary
  - Architecture pattern (layered plugin architecture)
  - Design patterns used (Plugin, Factory, Registry, Facade)
  - System boundaries and responsibilities
  - Key architectural decisions and trade-offs

- **[modules.md](modules.md)** - Module catalog with detailed descriptions
  - Purpose of each module and key functions
  - Responsibilities and public interfaces
  - Module relationships and communication patterns
  - Layer architecture explanation

- **[dependencies.md](dependencies.md)** - Complete dependency graph
  - Internal dependencies with adjacency list
  - External dependencies with purposes
  - Coupling assessment and analysis
  - Dependency direction and quality metrics

- **[entry-points.md](entry-points.md)** - Entry point documentation
  - All entry points with invocation paths
  - Usage examples for each entry point
  - Target user scenarios and selection guide
  - Integration patterns and workflows

- **[data-flow.md](data-flow.md)** - Data flow documentation
  - CSV loading pipeline (5 stages)
  - Analysis execution flow
  - Plugin discovery mechanism
  - Complete request lifecycle examples

## Architecture Overview

### 🏗️ Layered Plugin Architecture

The system implements a **4-layer architecture** with clear separation of concerns:

``┌─────────────────────────────────────┐
│        Application Layer             │ ← CLI, Package Init, Config
│  (cli.py, __init__.py, config.py)   │
├─────────────────────────────────────┤
│        Domain API Layer             │ ← Processor API, Plugin Framework
│  (processor.py, analyzer.py)         │
├─────────────────────────────────────┤
│       Infrastructure Layer          │ ← Data Loading, Format Detection
│  (loader.py, parsing.py, detection.py) │
├─────────────────────────────────────┤
│        Plugin Layer                 │ ← Analysis Modules
│      (analysts/ directory)          │
└─────────────────────────────────────┘```

### 🔧 Key Design Patterns

- **Plugin Pattern**: Extensible analysis modules with auto-discovery
- **Factory Pattern**: `load()` function creates Processor instances
- **Registry Pattern**: Global analyzer registry for CLI discovery
- **Facade Pattern**: Processor class abstracts DuckDB complexity
- **Strategy Pattern**: Format detection and parsing strategies
- **DTO Pattern**: ProcessorConfig encapsulates configuration options

### 🚀 Core Capabilities

1. **Automatic Format Detection**: Header and key:value format detection
2. **Plugin System**: Custom analysis modules with `@register` decorator
3. **Clean API**: High-level methods for common analytical operations
4. **Multiple Interfaces**: CLI, programmatic API, and interactive REPL
5. **Export Options**: CSV, JSON, and Parquet export formats
6. **Interactive Exploration**: SQL REPL with special commands

## 📊 System Metrics

### Quality Attributes
- **Maintainability**: Clear layer separation with single responsibility
- **Extensibility**: Plugin system enables independent analyzer development
- **Usability**: Simple API with sensible defaults and auto-detection
- **Performance**: Fast in-memory processing with DuckDB
- **Reliability**: Graceful handling of malformed data and clear error messages

### Code Structure
- **Total Modules**: 9 core modules across 4 layers
- **Entry Points**: 5 different interfaces for various usage scenarios
- **Plugin System**: Auto-discovery with decorator-based registration
- **Dependencies**: Minimal external dependencies (duckdb, pandas, stdlib)

## 🔍 Usage Scenarios

### For Data Analysts
```bash
# Quick analysis
duckdb-processor sales.csv --run demo

# Custom analysis pipeline
duckdb-processor data.csv --run analysis,report --interactive
```

### For Developers
```python
# Library integration
from duckdb_processor import load
p = load("data.csv")
p.aggregate("region", "amount", "SUM")
```

### For Plugin Developers
```python
# Create custom analyzer
@register
class MyAnalysis(BaseAnalyzer):
    name = "my_analysis"
    description = "Custom data analysis"
    def run(self, p):
        # Implementation using Processor methods
        pass
```

## 🔄 Workflows

### Development Workflow
1. Test with `main.py` (no installation needed)
2. Develop custom analyzers in `analysts/` directory
3. Test with `python -m duckdb_processor`
4. Install for production: `pip install -e .`

### Analysis Pipeline
1. **Load**: CSV → Detection → Normalization → DuckDB
2. **Analyze**: Execute registered analyzers in sequence
3. **Export**: Results to CSV, JSON, or Parquet

### Plugin Development
1. Create analyzer file in `analysts/` directory
2. Use `@register` decorator for automatic discovery
3. Implement `run()` method with Processor API
4. Test via `--run analyzer_name`

## 🎯 Integration Points

### External Systems
- **DuckDB**: In-memory analytical SQL database
- **Pandas**: DataFrame operations for results
- **CLI**: Shell command-line interface
- **Python**: Library integration for automation

### Extension Points
- **Plugin System**: Add custom analysis modules
- **Export Methods**: Extend for new output formats
- **Detection Strategies**: Enhance format detection
- **Processor Methods**: Add new analytical operations

## 📚 Related Resources

### Official Documentation
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Python CSV Module](https://docs.python.org/3/library/csv.html)

### Development Tools
- **Testing**: pytest with coverage >=85%
- **Linting**: ruff for code quality
- **Type Checking**: mypy for type safety
- **Packaging**: setuptools for distribution

## 🔧 Configuration and Environment

### Installation
```bash
# Development
pip install -e .

# Production
pip install duckdb_processor
```

### Environment Setup
```bash
# Virtual environment
python -m venv venv
source venv/bin/activate
pip install -e .

# Plugin development
cp analysts/_template.py analysts/my_analysis.py
# Edit my_analysis.py and test with --run my_analysis
```

This architecture documentation provides comprehensive context for understanding, maintaining, and extending the DuckDB CSV Processor system. Each document focuses on specific aspects while maintaining consistency across the overall architecture.