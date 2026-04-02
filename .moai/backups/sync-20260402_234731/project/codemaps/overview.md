# DuckDB CSV Processor - Architecture Overview

## Architecture Pattern: Layered Plugin Architecture

The DuckDB CSV Processor implements a **layered plugin architecture** with clean separation of concerns between infrastructure, domain logic, and presentation layers. This design enables extensible data analysis capabilities while maintaining a simple, intuitive API for data analysts.

## System Boundaries and Responsibilities

### Core System Components
- **Infrastructure Layer**: Responsible for data loading, format detection, and database setup
- **Domain API Layer**: Provides the business logic API and plugin framework
- **Application Layer**: CLI interface and public API orchestration
- **Plugin Layer**: Extensible analysis modules that can be registered dynamically

### External Dependencies
- **DuckDB**: In-memory analytical SQL database for data processing
- **Pandas**: DataFrame operations for query results and data manipulation
- **Standard Library**: CSV parsing, CLI argument parsing, dataclass configuration

## Key Architectural Decisions

### 1. Single Entry Point Design
- `load()` function serves as the primary API entry point
- Accepts both configuration objects and keyword arguments
- Orchestrates the entire pipeline from CSV to Processor instance
- Hides complex detection and loading logic from users

### 2. Plugin Registry Pattern
- Global analyzer registry enables automatic discovery
- `@register` decorator simplifies plugin registration
- Import-time auto-discovery via `pkgutil.iter_modules()`
- CLI can list and execute registered analyzers by name

### 3. Processor API Facade
- Clean Python interface wrapping DuckDB operations
- High-level methods for common analytical operations
- Abstracts away SQL complexity for business users
- Supports method chaining for complex workflows

### 4. Format Detection Heuristics
- Automatic header detection based on numeric content analysis
- Key:value format detection using colon frequency analysis
- Override capability for explicit format specification
- Graceful handling of edge cases and malformed data

### 5. Data Pipeline Architecture
1. **Input**: Read raw CSV from file or stdin
2. **Detection**: Auto-detect format characteristics
3. **Normalization**: Convert to uniform list[dict] representation
4. **Loading**: Create DuckDB table with inferred schema
5. **Processing**: Provide Processor API for analytical operations

## Design Patterns Implemented

### Plugin Pattern
- BaseAnalyzer abstract class defines the plugin interface
- Registry maintains collection of available plugins
- Factory pattern for plugin instantiation by name
- Enables runtime plugin discovery and execution

### Factory Pattern
- `load()` function acts as factory for Processor instances
- Configuration-based creation with sensible defaults
- Handles complex construction logic internally
- Returns standardized API regardless of input source

### Registry Pattern
- Global `_registry` dictionary tracks available analyzers
- Decorator pattern for plugin registration
- Enables CLI discovery and execution
- Supports runtime introspection and listing

### Facade Pattern
- Processor class provides simplified interface to complex DuckDB operations
- High-level methods abstract SQL generation details
- Consistent API for common analytical tasks
- Reduces cognitive load for data analysts

### Strategy Pattern
- Format detection strategies (header, key:value)
- Parser strategies for different data formats
- Export strategies for different output formats
- Enables runtime selection of processing approaches

### Data Transfer Object Pattern
- ProcessorConfig dataclass encapsulates configuration options
- Immutable configuration with sensible defaults
- Clear separation between configuration and business logic
- Enables easy serialization and testing

## Architectural Benefits

### Maintainability
- Clear layer separation reduces coupling
- Each module has single, well-defined responsibility
- Plugin system enables independent development of analyzers
- Configuration system centralizes option handling

### Extensibility
- Plugin architecture allows easy addition of new analyzers
- Clean interfaces enable custom processor implementations
- Export methods can be extended for new formats
- Detection strategies can be enhanced for new formats

### Usability
- Simple entry point reduces API surface for users
- High-level Processor methods hide SQL complexity
- Auto-detection minimizes configuration requirements
- CLI provides convenient access to all functionality

### Performance
- DuckDB provides fast in-memory analytical processing
- Single-pass data loading minimizes overhead
- Efficient CSV parsing and normalization
- Pandas integration for optimized DataFrame operations

## Architectural Trade-offs

### Complexity vs. Flexibility
- **Trade-off**: Plugin system adds configuration complexity
- **Resolution**: Simple decorator-based registration keeps user experience clean
- **Impact**: Enables powerful extensibility while maintaining simple usage

### Auto-detection vs. Explicit Control
- **Trade-off**: Heuristic detection may not always be accurate
- **Resolution**: Allow override of detection via configuration
- **Impact**: Balances automation with manual control for edge cases

### Abstraction vs. Transparency
- **Trade-off**: Processor API abstracts SQL details
- **Resolution**: Provide raw SQL access via `sql()` method
- **Impact**: Enables both simplicity and advanced usage when needed

## Architectural Quality Attributes

### Performance
- Fast data loading with single-pass CSV parsing
- Efficient in-memory processing with DuckDB
- Optimized SQL generation for common operations
- Minimal overhead for data transformation operations

### Reliability
- Graceful handling of malformed CSV data
- Type-safe processor methods with proper error handling
- Clear error messages for common issues
- Transaction-safe database operations

### Security
- SQL injection prevention through parameterized queries
- Safe file path handling with validation
- No execution of arbitrary user code
- Clean separation of user data and business logic

### Usability
- Intuitive high-level API for data analysts
- Comprehensive documentation and examples
- Built-in demo analysis for learning
- Interactive SQL REPL for exploration

This architecture successfully balances technical sophistication with user simplicity, enabling powerful data analysis capabilities while maintaining an accessible interface for data analysts of all skill levels.