# Changelog

All notable changes to the DuckDB CSV Processor project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Output file formatting** - Output files now contain plain text instead of Rich formatting codes (ANSI escape sequences). Uses SimpleFormatter when writing to files while preserving Rich formatting on console.

### Enhanced
- **REPL keyboard support** - Added readline support for better keyboard interaction:
  - Arrow keys for cursor movement (left/right) and command history (up/down)
  - Inline editing support - edit anywhere in SQL commands, not just delete from back
  - New `\tables` command to list all database tables
  - New `\help` command with keyboard shortcut documentation
  - Better error handling for empty query results

## [1.0.0] - 2026-03-29

### Added
- **Output file option** - New `--output`/-o flag to save dataset info to file:
  - Custom filename: `--output results.txt`
  - Default filename with timestamp: `--output` (creates `duckdb_output_YYYYMMDD_HHMMSS.txt`)
  - Short form: `-o my_output.txt`

### Changed
- **Analyst formatter integration** - All built-in analysts (demo, sql_examples) now use configured formatter for output:
  - Rich formatter: Professional tables with colors and borders
  - Simple formatter: Plain text output (backward compatible)
  - Graceful fallback when Rich library unavailable

### Documentation
- **README.md** - Comprehensive project documentation including:
  - Installation instructions
  - Usage examples with all CLI options
  - Output formatter examples
  - Interactive SQL REPL guide
  - Custom analyst creation tutorial
  - Development setup and testing
  - Configuration reference
  - Accessibility features
  - Contributing guidelines

## [1.0.0-rc1] - 2026-03-28

### Added
- **Rich library integration** - Professional terminal output with:
  - Type-aware table formatting (numeric columns right-aligned, cyan color)
  - Color-coded messages (ERROR in red, WARNING in yellow, INFO in blue)
  - Progress bars for long operations
  - Terminal width detection
  - Graceful fallback to SimpleFormatter when Rich unavailable

- **Formatter system** - Modular output formatting:
  - `BaseFormatter` - Abstract base class defining formatter interface
  - `RichFormatter` - Rich library-based formatter with colors and progress bars
  - `SimpleFormatter` - Legacy pandas-based formatter for backward compatibility
  - `OutputConfig` - Configuration dataclass for formatter behavior

- **Terminal detection utilities**:
  - `supports_color()` - Check if terminal supports ANSI color codes
  - `detect_terminal_width()` - Auto-detect terminal width
  - `detect_screen_reader()` - Detect screen reader usage for accessibility

- **CLI flags**:
  - `--format [rich|simple]` - Choose output format
  - `--no-color` - Disable colored output
  - `--no-progress` - Disable progress indicators

- **Accessibility features**:
  - Screen reader detection and mode
  - High contrast mode support
  - WCAG compliance (color not sole information source)
  - Text-only mode preserves information without colors

- **Test data and examples**:
  - `test_data.csv` - Sample dataset (10 products)
  - `QUICKSTART.md` - Testing guide and usage examples
  - `FORMATTER_EXAMPLES.md` - Before/after comparison

- **Enhanced REPL** - Interactive SQL mode with command history and auto-completion

- **User configuration system** - Persistent user preferences

- **Comprehensive test suite**:
  - 59 tests covering formatters, CLI, integration, and accessibility
  - 100% coverage on new formatter core code
  - TRUST 5 quality gates verified

### Security
- No breaking changes - All existing functionality preserved
- Backward compatible via SimpleFormatter
- Input validation on all file paths
- Safe error handling with clear error messages

### Performance
- Lazy Rich import - Library loaded only when needed
- Efficient DataFrame truncation utilities
- Shared progress calculation utilities

### Documentation
- Complete SPEC documentation (SPEC-CLI-001)
- API documentation for formatter system
- Contributor guidelines
- Architecture documentation in `.moai/project/`

## [0.1.0] - 2026-03-27

### Added
- Initial commit with basic DuckDB CSV Processor functionality
- CSV file loading with auto-detection (headers, key-value pairs, flat CSV)
- Processor API for data analysis (coverage, preview, filter, aggregate, pivot)
- Analyst plugin system with demo analyst
- Interactive SQL REPL
- Basic pandas-based string output

[Unreleased]: https://github.com/CJ-1981/duckdb-processor-cli/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/CJ-1981/duckdb-processor-cli/compare/v0.1.0...v1.0.0-rc1
[1.0.0-rc1]: https://github.com/CJ-1981/duckdb-processor-cli/compare/v0.1.0...v1.0.0
