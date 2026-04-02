# DuckDB CSV Processor - Dependency Graph

## Dependency Overview

The DuckDB CSV Processor implements a **layered dependency architecture** with clear separation between infrastructure, domain, and application layers. Dependencies flow primarily upward, with minimal cross-layer dependencies.

---

## Internal Dependencies (Adjacency List)

### Infrastructure Layer Dependencies

```
loader.py → config.py, detection.py, parsing.py, processor.py
detection.py → (no internal dependencies)
parsing.py → csv, io, sys, pathlib.Path (stdlib modules)
```

### Domain API Layer Dependencies

```
processor.py → duckdb, pandas, json, pathlib.Path
analyzer.py → (internal only, no external dependencies)
```

### Application Layer Dependencies

```
cli.py → analyzer.py, config.py, loader.py, processor.py
__init__.py → analyzer.py, config.py, loader.py, processor.py, analysts
config.py → dataclasses (stdlib)
```

### Plugin Layer Dependencies

```
analysts/__init__.py → importlib, pkgutil (stdlib)
analysts/_template.py → duckdb_processor.analyzer
analysts/demo.py → duckdb_processor.analyzer
```

## Full Dependency Graph

```
loader.py
├── config.ProcessorConfig
├── detection.detect_header
├── detection.detect_kv
├── parsing.read_input
├── parsing.normalize
└── processor.Processor

detection.py
└── (no dependencies)

parsing.py
├── csv (stdlib)
├── io (stdlib)
├── sys (stdlib)
└── pathlib.Path (stdlib)

processor.py
├── duckdb (external)
├── pandas (external)
├── json (stdlib)
└── pathlib.Path (stdlib)

analyzer.py
├── abc (stdlib)
└── typing.TYPE_CHECKING (stdlib)

cli.py
├── analyzer.py (list_analyzers, run_analyzers)
├── config.py (ProcessorConfig)
├── loader.py (load)
└── processor.py (Processor)

__init__.py
├── analyzer.py (BaseAnalyzer, get_analyzer, list_analyzers, register, run_analyzers)
├── config.py (ProcessorConfig)
├── loader.py (load)
├── processor.py (Processor)
└── duckdb_processor.analysts (auto-import)

config.py
├── dataclasses.dataclass (stdlib)
└── dataclasses.field (stdlib)

analysts/__init__.py
├── importlib (stdlib)
└── pkgutil (stdlib)

analysts/_template.py
└── duckdb_processor.analyzer (BaseAnalyzer, register)

analysts/demo.py
└── duckdb_processor.analyzer (BaseAnalyzer, register)
```

## External Dependencies

### Runtime Dependencies

```
duckdb>=0.9
└── Core database engine for in-memory SQL processing
└── Provides DuckDBPyConnection and SQL execution capabilities
└── Required for all data storage and operations

pandas>=2.0
└── DataFrame operations for query results
└── Data structure for method return values
└── Required for data manipulation and display
```

### Development Dependencies

```
pytest>=7.0
└── Testing framework for unit and integration tests
└── Required for development and CI/CD
```

### Standard Library Dependencies

```
csv
└── CSV parsing functionality
└── Required for reading CSV input data

io
└── Input/output operations
└── Required for file and stdin handling

sys
└── System-specific parameters and functions
└── Required for command-line argument parsing

pathlib
└── Object-oriented filesystem paths
└── Required for file path handling

dataclasses
└── Data structure classes
└── Required for configuration definitions

argparse
└── Command-line argument parsing
└── Required for CLI interface

abc
└── Abstract base classes
└── Required for plugin interface definitions

typing
└── Type hints and annotations
└── Required for type safety and documentation

json
└── JSON serialization and deserialization
└── Required for JSON export functionality

importlib
└── Import system utilities
└── Required for plugin discovery

pkgutil
└── Package utilities
└── Required for module iteration and discovery
```

## Coupling Assessment

### Low Coupling
- **detection.py**: Pure utility functions with no dependencies
- **parsing.py**: Only depends on standard library modules
- **analyzer.py**: Abstract plugin framework with minimal dependencies
- **config.py**: Simple dataclass with only standard library dependencies

### Medium Coupling
- **loader.py**: Depends on multiple internal modules but maintains clear interface
- **processor.py**: Depends on external libraries (duckdb, pandas) but provides clean abstraction
- **cli.py**: Depends on multiple modules but well-orchestrated

### High Coupling
- **__init__.py**: Central export point with many dependencies (expected for public API)
- **analysts modules**: Depend on core analyzer framework

## Dependency Direction Analysis

### Unidirectional Dependencies
All dependencies flow in the correct upward direction:
- Infrastructure → Domain → Application
- No circular dependencies detected
- Clean separation between layers

### Dependency Inversion (Where Applied)
- **Plugin System**: Application depends on abstraction (BaseAnalyzer) rather than concrete implementations
- **Processor Interface**: Plugins depend on Processor abstraction rather than database details
- **Configuration Pattern**: Components depend on configuration interface rather than hardcoded values

## Dependency Violations and Mitigations

### Potential Issues
1. **Direct Database Access**: Could abstract DuckDB dependency for testing
2. **Hardcoded Export Formats**: Could make export strategies pluggable
3. **Detection Logic Coupling**: Detection algorithms are tightly coupled to parsing

### Mitigations Applied
1. **Plugin Architecture**: Enables extension without modification of core components
2. **Configuration System**: Centralizes options and reduces hardcoded values
3. **Clean Interfaces**: Well-defined APIs between layers reduce coupling
4. **Separation of Concerns**: Each module has single, clear responsibility

## External Dependency Integration

### DuckDB Integration
- **Abstraction**: Processor class provides clean interface
- **Type Safety**: Uses DuckDBPyConnection type hints
- **Error Handling**: Graceful handling of database operations
- **Performance**: In-memory processing for fast analytics

### Pandas Integration
- **Type Conversion**: Automatic DataFrame conversion for query results
- **Format Control**: Consistent DataFrame format for all method returns
- **Display Formatting**: String representation for user display
- **Export Support**: Pandas-based export functionality

## Test Dependencies

### Unit Testing Structure
- **Mock-able Interfaces**: Clear abstractions for testing
- **Configuration Injection**: Testable with various configurations
- **Plugin Isolation**: Analyzers can be tested independently
- **CLI Testing**: Argument parsing and execution flow testable

### Testing Challenges
- **Database Testing**: DuckDB dependency requires test database setup
- **File I/O**: Input/output operations need mocking or temporary files
- **Plugin Discovery**: Auto-discovery requires careful test isolation

## Development Dependencies

### Build System
- **setuptools**: Package building and distribution
- **wheel**: Binary distribution support

### Quality Tools
- **pytest**: Unit and integration testing
- **setuptools.find**: Package discovery
- **setuptools.package-data**: Plugin package inclusion

## Runtime Dependencies Impact

### Performance Considerations
- **DuckDB**: Fast in-memory processing, minimal overhead
- **Pandas**: Efficient DataFrame operations, good for medium datasets
- **Standard Library**: Minimal performance impact

### Memory Usage
- **DuckDB**: In-memory database, efficient for analytical workloads
- **Pandas**: DataFrame caching, efficient for repeated operations
- **Data Normalization**: List[dict] representation has some overhead

## Dependency Management Strategy

### Version Compatibility
- **Python 3.10+**: Modern Python features and type hints
- **DuckDB 0.9+**: Stable analytical database with good performance
- **Pandas 2.0+**: Modern pandas with improved performance

### Future Extensibility
- **Plugin System**: Easy addition of new analysis modules
- **Configuration System**: Easy addition of new options
- **Export System**: Pluggable export strategies possible
- **Detection System**: Extensible detection algorithms

This dependency structure enables maintainable, testable, and extensible code while providing clean abstractions for users and plugin developers.