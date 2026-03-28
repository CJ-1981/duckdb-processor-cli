# CLI Output Formatting Enhancement - Implementation Plan

**SPEC ID:** SPEC-CLI-001
**Created:** 2026-03-28
**Status:** Planning
**Priority:** High

---

## Executive Summary

This comprehensive implementation plan addresses CLI output formatting enhancements for the DuckDB CSV Processor based on research findings. The plan proposes a single comprehensive SPEC with a three-phase implementation approach that balances innovation with backward compatibility.

**Current State Analysis:**
- Basic `print()` statements with `pandas.DataFrame.to_string(index=False)`
- 50+ print statements throughout the codebase
- No progress indicators, colors, or advanced formatting
- Minimal interactive REPL with basic text output

**Target State:**
- Professional-grade terminal formatting with Rich library
- Progress indicators for long-running operations
- Type-aware table formatting with color coding
- Enhanced interactive REPL with command history
- Accessibility features (screen reader support, high-contrast mode)

**Implementation Strategy:** Phased rollout with backward compatibility guarantees

---

## 1. SPEC Candidate Proposal

### SPEC-CLI-001: Comprehensive CLI Output Formatting Enhancement

**Rationale for Single Comprehensive SPEC:**

The research document identifies three distinct areas of enhancement:
1. Rich library integration and foundation
2. Progress indicators and core features
3. Advanced REPL and accessibility features

However, splitting these into separate SPECs would create:
- Dependency complexity (Phase 2 depends on Phase 1 foundation)
- Integration challenges (coordinating multiple SPEC implementations)
- Delayed value delivery (users wait longer for complete feature set)

**Single SPEC Benefits:**
- Cohesive vision with phased implementation
- Reduced coordination overhead
- Continuous value delivery across phases
- Simplified testing and validation
- Clear rollback strategy if issues arise

**Implementation Approach:**
- **Phase 1 (Foundation):** Rich library integration with backward compatibility
- **Phase 2 (Core Features):** Progress bars, enhanced tables, error formatting
- **Phase 3 (Advanced Features):** REPL enhancements, accessibility, configuration

Each phase is independently valuable and can be released incrementally.

---

## 2. EARS Structure

### Module 1: Rich Library Integration Foundation

**Ubiquitous Requirements:**
- The system shall maintain backward compatibility with existing output format
- The system shall support both Rich and Simple formatter implementations
- The system shall provide configuration option for formatter selection

**Event-Driven Requirements:**
- WHEN user enables rich formatting, the system shall use Rich library for output
- WHEN user specifies --format simple, the system shall use legacy pandas.to_string() format
- WHEN terminal does not support color, the system shall automatically fallback to SimpleFormatter

**State-Driven Requirements:**
- IF terminal supports ANSI color codes, the system shall enable color-coded output
- IF formatter is not explicitly configured, the system shall use RichFormatter by default
- IF Rich library is not installed, the system shall gracefully fallback to SimpleFormatter

**Unwanted Requirements:**
- The system shall not break existing analyzer scripts that depend on print() output
- The system shall not require code changes in existing analyst implementations
- The system shall not force users to adopt new formatting immediately

**Optional Requirements:**
- WHERE possible, the system shall provide automatic terminal capability detection
- WHERE possible, the system shall support custom color themes via configuration
- WHERE possible, the system shall allow user-defined formatter plugins

### Module 2: Progress Indicators

**Ubiquitous Requirements:**
- The system shall display progress during long-running operations
- The system shall estimate time remaining for progress operations
- The system shall allow users to disable progress indicators via --no-progress flag

**Event-Driven Requirements:**
- WHEN loading CSV files larger than 10MB, the system shall show progress indicator
- WHEN running analysis operations, the system shall show progress for each analyzer
- WHEN exporting data, the system shall show export progress

**State-Driven Requirements:**
- IF operation completes in less than 2 seconds, the system may skip progress display
- IF operation is cancelled by user, the system shall display cancellation message
- IF progress cannot be determined, the system shall display indeterminate progress spinner

**Unwanted Requirements:**
- The system shall not display progress for instant operations (< 500ms)
- The system shall not slow down operations with progress overhead
- The system shall not clutter output with excessive progress updates

**Optional Requirements:**
- WHERE possible, the system shall display ETA (Estimated Time of Arrival)
- WHERE possible, the system shall show throughput metrics (rows/sec, MB/sec)
- WHERE possible, the system shall allow progress bar customization via configuration

### Module 3: Enhanced Table Formatting

**Ubiquitous Requirements:**
- The system shall format DataFrames with type-aware column styling
- The system shall respect terminal width constraints
- The system shall provide fallback formatting for wide tables

**Event-Driven Requirements:**
- WHEN displaying query results, the system shall apply Rich table formatting
- WHEN table exceeds terminal width, the system shall enable smart truncation
- WHEN displaying large DataFrames, the system shall show head and tail with row count

**State-Driven Requirements:**
- IF DataFrame has more than 50 rows, the system shall display truncated view with count
- IF column width exceeds available terminal width, the system shall wrap or truncate content
- IF terminal width cannot be detected, the system shall default to 80-character width

**Unwanted Requirements:**
- The system shall not overflow terminal width with wide tables
- The system shall not display empty tables without message
- The system shall not lose data precision in numeric display

**Optional Requirements:**
- WHERE possible, the system shall detect terminal width and adjust column widths dynamically
- WHERE possible, the system shall provide alternate display formats (JSON, CSV, Markdown)
- WHERE possible, the system shall support pager integration for large results

### Module 4: Error Message Enhancement

**Ubiquitous Requirements:**
- The system shall categorize errors by severity level (ERROR, WARNING, INFO)
- The system shall use consistent error message format across all operations
- The system shall preserve full error details for debugging

**Event-Driven Requirements:**
- WHEN error occurs, the system shall display color-coded error message
- WHEN SQL execution fails, the system shall highlight syntax errors
- WHEN file operation fails, the system shall provide file path and error reason

**State-Driven Requirements:**
- IF error is recoverable, the system shall provide helpful suggestion
- IF error is critical, the system shall display clear error message with exit instructions
- IF debug mode is enabled, the system shall display full exception traceback

**Unwanted Requirements:**
- The system shall not display raw exception tracebacks to end users in normal mode
- The system shall not use technical jargon without explanation
- The system shall not hide error details needed for troubleshooting

**Optional Requirements:**
- WHERE possible, the system shall suggest corrective actions for common errors
- WHERE possible, the system shall provide error code for documentation lookup
- WHERE possible, the system shall link to online help resources for complex errors

### Module 5: Configuration and Accessibility

**Ubiquitous Requirements:**
- The system shall respect user preferences for output formatting
- The system shall support command-line flags for format control
- The system shall not rely solely on color to convey information

**Event-Driven Requirements:**
- WHEN user specifies --no-color, the system shall disable color output
- WHEN user specifies --format, the system shall use specified formatter
- WHEN screen reader is detected, the system shall use text-only mode

**State-Driven Requirements:**
- IF user has color preference in config file, the system shall respect it
- IF terminal does not support color, the system shall automatically disable colors
- IF accessibility mode is enabled, the system shall use high-contrast colors

**Unwanted Requirements:**
- The system shall not rely solely on color to convey critical information
- The system shall not ignore user-specified formatting preferences
- The system shall not force accessibility mode without user consent

**Optional Requirements:**
- WHERE possible, the system shall provide configuration file for persistent preferences
- WHERE possible, the system shall support theme selection (default, high-contrast, monochrome)
- WHERE possible, the system shall detect screen reader and enable accessibility mode automatically

---

## 3. Implementation Approach

### Phase 1: Foundation (Low Risk) - Weeks 1-2

**Objectives:**
- Integrate Rich library without breaking existing functionality
- Create formatter architecture with fallback support
- Add configuration options for format selection

**Deliverables:**
1. Create `duckdb_processor/formatters/` directory
2. Implement `BaseFormatter` abstract class
3. Implement `RichFormatter` with Console integration
4. Implement `SimpleFormatter` (legacy compatibility wrapper)
5. Add `OutputConfig` dataclass for settings
6. Add `--format` CLI option (table/simple)
7. Add unit tests for formatter classes

**Files to Create:**
- `duckdb_processor/formatters/__init__.py`
- `duckdb_processor/formatters/base.py` - BaseFormatter abstract class
- `duckdb_processor/formatters/rich_formatter.py` - Rich library integration
- `duckdb_processor/formatters/simple_formatter.py` - Legacy format wrapper
- `duckdb_processor/formatters/config.py` - OutputConfig dataclass
- `tests/test_formatters.py` - Formatter unit tests

**Files to Modify:**
- `duckdb_processor/cli.py` - Add --format option, integrate formatters
- `duckdb_processor/processor.py` - Update print_info() to use formatters
- `pyproject.toml` - Add rich>=13.7.0 dependency

**Acceptance Criteria:**
- All existing tests pass without modification
- New formatter tests achieve 85%+ coverage
- Users can switch between formats via --format flag
- Legacy output format remains functional
- No breaking changes to existing analyzer scripts

**Rollback Plan:**
- Configuration flag to disable Rich formatting
- SimpleFormatter maintains exact legacy behavior
- No changes to existing analyzer API

### Phase 2: Core Features (Medium Risk) - Weeks 3-5

**Objectives:**
- Add progress indicators for long-running operations
- Implement enhanced table formatting
- Add color-coded error messages

**Deliverables:**
1. Progress bars for CSV loading in `loader.py`
2. Progress indicators for analysis execution in `analyzer.py`
3. Enhanced table formatting with type-aware styling
4. Color-coded error messages with severity levels
5. Terminal width detection and adaptive formatting

**Files to Modify:**
- `duckdb_processor/loader.py` - Add progress bar during CSV loading
- `duckdb_processor/analyzer.py` - Add progress for each analyzer
- `duckdb_processor/formatters/rich_formatter.py` - Enhanced table formatting
- `duckdb_processor/cli.py` - Error message formatting
- `duckdb_processor/processor.py` - Update output methods

**Key Implementation Details:**

**Progress Indicators:**
```python
# loader.py - CSV loading with progress
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

def load_csv_with_progress(file_path: str, config: ProcessorConfig) -> Processor:
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Loading CSV...", total=100)
        # Load logic here
        progress.update(task, advance=100)
    return processor
```

**Enhanced Table Formatting:**
```python
# rich_formatter.py - Type-aware table formatting
from rich.table import Table
from rich.console import Console

def format_dataframe(self, df: pd.DataFrame) -> None:
    table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        style = "cyan" if pd.api.types.is_numeric_dtype(df[col]) else "white"
        table.add_column(col, style=style)
    for _, row in df.iterrows():
        table.add_row(*row.astype(str).tolist())
    self.console.print(table)
```

**Error Message Formatting:**
```python
# cli.py - Color-coded error messages
from rich.console import Console
console = Console()

def display_error(error: Exception, context: str) -> None:
    console.print(f"[bold red]Error:[/bold red] {error}", file=sys.stderr)
    if isinstance(error, SQLError):
        console.print(f"[yellow]Hint:[/yellow] Check SQL syntax in: {context}")
```

**Acceptance Criteria:**
- Progress bars display for operations > 2 seconds
- Tables are formatted with type-specific colors
- Error messages are color-coded by severity
- Terminal width detection works correctly
- Performance impact < 5% for small datasets (< 1000 rows)

**Rollback Plan:**
- `--no-progress` flag to disable progress indicators
- `--no-color` flag to disable color output
- Fallback to simple formatting on performance degradation

### Phase 3: Advanced Features (Higher Risk) - Weeks 6-8

**Objectives:**
- Enhance interactive REPL with command history and auto-completion
- Add pager integration for large results
- Implement accessibility features

**Deliverables:**
1. Enhanced REPL with readline support and command history
2. Tab auto-completion for SQL keywords and table names
3. Pager integration for large result sets
4. Screen reader compatibility mode
5. High-contrast color theme
6. User configuration file support

**Files to Create:**
- `duckdb_processor/repl.py` - Enhanced REPL implementation
- `duckdb_processor/config/user_config.py` - User configuration management

**Files to Modify:**
- `duckdb_processor/cli.py` - Integrate enhanced REPL
- `duckdb_processor/formatters/rich_formatter.py` - Add accessibility features
- `tests/test_repl.py` - REPL unit tests

**Key Implementation Details:**

**Enhanced REPL:**
```python
# repl.py - Interactive REPL with history and auto-completion
import readline
import atexit
from pathlib import Path

class EnhancedREPL:
    def __init__(self):
        self.history_file = Path.home() / '.duckdb_processor_history'
        self._setup_history()
        self._setup_completion()

    def _setup_history(self):
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        atexit.register(readline.write_history_file, self.history_file)

    def _setup_completion(self):
        import rlcompleter
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self._completer)

    def _completer(self, text, state):
        # SQL keywords and table name completion
        pass
```

**Pager Integration:**
```python
# rich_formatter.py - Pager for large results
import subprocess

def format_large_dataframe(self, df: pd.DataFrame, max_rows: int = 50) -> None:
    if len(df) > max_rows:
        # Use pager for large results
        with self.console.pager():
            self.console.print(table)
    else:
        self.console.print(table)
```

**Accessibility Features:**
```python
# config.py - Accessibility configuration
@dataclass
class AccessibilityConfig:
    screen_reader_mode: bool = False
    high_contrast_mode: bool = False
    text_only_mode: bool = False

    @classmethod
    def detect_accessibility_needs(cls) -> 'AccessibilityConfig':
        # Detect screen readers or accessibility tools
        pass
```

**Acceptance Criteria:**
- Command history persists across sessions
- Tab completion works for SQL keywords and table names
- Pager activates for results > 100 rows
- Screen reader mode provides text-only output
- High-contrast theme meets WCAG AAA standards
- User configuration file saves preferences

**Rollback Plan:**
- Legacy REPL available via `--legacy-repl` flag
- Accessibility features disabled by default
- Configuration file opt-in (not auto-created)

---

## 4. Technology Stack

### Library Specifications (Production Stable Only)

**Rich Library:**
- **Version:** `rich>=13.7.0` (latest stable as of 2025)
- **Purpose:** Terminal formatting, progress bars, table rendering
- **Justification:** Industry-leading Python terminal formatting library with cross-platform support
- **Documentation:** https://rich.readthedocs.io/
- **License:** MIT (permissive)

**Click Library:**
- **Version:** `click>=8.1.7` (stable release)
- **Purpose:** CLI framework consistency (already in project dependencies)
- **Justification:** Aligns with existing CLI structure in tech.md
- **Documentation:** https://click.palletsprojects.com/
- **License:** BSD-3-Clause (permissive)

**Tabulate Library:**
- **Version:** `tabulate>=0.9.0` (stable release)
- **Purpose:** Fallback table formatting for simple mode
- **Justification:** Lightweight table formatting alternative to Rich
- **Documentation:** https://github.com/astanin/python-tabulate
- **License:** MIT (permissive)

### Updated pyproject.toml Dependencies

```toml
[project]
dependencies = [
    "duckdb>=0.9.0",
    "pandas>=2.0.0",
    "click>=8.0.0",
    "rich>=13.7.0",           # NEW: Terminal formatting
    "tabulate>=0.9.0",        # NEW: Fallback table formatting
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "black>=22.0.0",
    "ruff>=0.1.0",
    "mypy>=1.0.0",
]
```

### Version Selection Rationale

**Why Rich 13.7.0+?**
- Stable production release (no beta/alpha)
- Active maintenance and regular updates
- Comprehensive feature set (tables, progress, syntax highlighting)
- Cross-platform terminal handling
- Strong community adoption (70K+ GitHub stars)

**Why not newer Rich features?**
- Avoid experimental features from latest releases
- Prioritize stability over cutting-edge functionality
- Ensure backward compatibility with older Python 3.10

**Why Tabulate as fallback?**
- Lightweight alternative to Rich
- Multiple table formats (plain, simple, grid, fancy_grid)
- Zero external dependencies for basic formatting
- Proven reliability in production environments

---

## 5. Reference Implementations

### Existing Code Patterns (Reference Locations)

**1. Current Output Patterns in duckdb_processor/:**

**CLI Output (duckdb_processor/cli.py):**
- **Lines 97-136:** Interactive REPL implementation
  - Basic `print()` statements for UI
  - Unicode box drawing characters (`─`, `├`, `└`)
  - Direct DataFrame display via `to_string(index=False)`
  - Reference: Minimal REPL to be enhanced in Phase 3

**DataFrame Display (duckdb_processor/cli.py:126, 129, 133):**
```python
# Current pattern to be replaced with formatter
print(p.schema().to_string(index=False))
print(p.coverage().to_string(index=False))
print(p.sql(query).to_string(index=False))
```
- **Enhancement target:** Replace with formatter.format_dataframe(df)

**Info Banner (duckdb_processor/processor.py:82-97):**
```python
# Current info banner implementation
def print_info(self) -> None:
    m = self.info()
    width = 58
    print()
    print("\u2501" * width)  # Unicode box drawing
    print("  DuckDB CSV Processor")
    # ... more formatted output
```
- **Enhancement target:** Replace with formatter.print_info(metadata)

**Analysis Output (duckdb_processor/analyzer.py:134-138):**
```python
# Current analysis execution output
print(f"\n{'─' * 58}")
print(f"  [{name}] {desc}")
print(f"{'─' * 58}")
```
- **Enhancement target:** Replace with formatter.print_section(name, desc)

### External Best Practices

**1. Rich Library Official Examples:**
- **Source:** https://rich.readthedocs.io/en/stable/introduction.html
- **Patterns:**
  - Console initialization with color system
  - Table creation with column styling
  - Progress bar with ETA estimation
  - Syntax highlighting for SQL

**2. DuckDB CLI (Reference Implementation):**
- **Features:**
  - Rich table output with automatic sizing
  - Progress indicators for long-running queries
  - Color coding for different data types
  - Interactive mode with command history
- **Learning Source:** https://duckdb.org/docs/guides/python/sql_on_pandas

**3. psql (PostgreSQL CLI):**
- **Features:**
  - Pager integration for large results
  - Command history and auto-completion
  - Configurable output formats (unaligned, html, latex)
  - Timing for query execution
- **Learning Source:** PostgreSQL documentation on psql

**4. csvkit (CSV Processing Suite):**
- **Features:**
  - Multiple output formats (CSV, TSV, Markdown)
  - Intelligent column handling
  - Progress indicators for large files
- **Learning Source:** https://csvkit.readthedocs.io/

### Code Patterns to Implement

**Formatter Interface (based on research):**
```python
# duckdb_processor/formatters/base.py
from abc import ABC, abstractmethod
import pandas as pd

class BaseFormatter(ABC):
    """Abstract base class for output formatters."""

    @abstractmethod
    def format_dataframe(self, df: pd.DataFrame) -> None:
        """Display a DataFrame with appropriate formatting."""
        pass

    @abstractmethod
    def format_info(self, metadata: dict) -> None:
        """Display dataset information banner."""
        pass

    @abstractmethod
    def format_error(self, error: Exception, context: str) -> None:
        """Display error message with appropriate styling."""
        pass
```

**Rich Formatter Implementation (based on Rich docs):**
```python
# duckdb_processor/formatters/rich_formatter.py
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn

class RichFormatter(BaseFormatter):
    """Rich library-based formatter with colors and progress bars."""

    def __init__(self, console: Console | None = None):
        self.console = console or Console()

    def format_dataframe(self, df: pd.DataFrame) -> None:
        """Display DataFrame with type-aware styling."""
        table = Table(show_header=True, header_style="bold magenta")
        for col in df.columns:
            style = "cyan" if pd.api.types.is_numeric_dtype(df[col]) else "white"
            table.add_column(col, style=style)
        for _, row in df.iterrows():
            table.add_row(*row.astype(str).tolist())
        self.console.print(table)
```

---

## 6. Risk Analysis and Mitigation

### High Risk Items

**Risk 1: Breaking Existing Analyzer Scripts**
- **Impact:** HIGH - Existing user workflows disrupted
- **Probability:** MEDIUM - API changes could affect analyzers
- **Mitigation Strategy:**
  - Maintain complete backward compatibility in Phase 1
  - Add extensive integration tests for existing analyzers
  - Provide opt-in period with `--use-rich-format` flag (default: false)
  - Run full test suite on every commit
- **Rollback Plan:**
  - Configuration flag to disable Rich formatting globally
  - SimpleFormatter maintains exact legacy behavior
  - No changes to existing analyzer API surface

**Risk 2: Performance Degradation for Large Datasets**
- **Impact:** HIGH - User experience degradation on large files
- **Probability:** MEDIUM - Rich library overhead for large tables
- **Mitigation Strategy:**
  - Add performance benchmarking before/after
  - Implement fast mode for DataFrames > 10K rows
  - Use simple formatting as fallback for large datasets
  - Profile memory usage with memory_profiler
- **Rollback Plan:**
  - Automatic fallback to SimpleFormatter when slow
  - `--fast-mode` flag to force simple formatting
  - Performance threshold configuration

**Risk 3: Terminal Compatibility Issues**
- **Impact:** MEDIUM - Broken output on certain terminals
- **Probability:** LOW - Rich has good cross-platform support
- **Mitigation Strategy:**
  - Test on Windows Terminal, iTerm, GNOME Terminal, Linux console
  - Implement terminal capability detection
  - Provide `--no-color` flag for legacy terminals
  - Fallback to SimpleFormatter on terminal errors
- **Rollback Plan:**
  - Force simple mode via environment variable
  - Graceful degradation with user notification

### Medium Risk Items

**Risk 4: Color Accessibility for Colorblind Users**
- **Impact:** MEDIUM - Excludes colorblind users from enhanced UX
- **Probability:** LOW - Colorblind-safe palettes available
- **Mitigation Strategy:**
  - Use colorblind-safe palettes (e.g., Okabe-Ito)
  - Add text alternatives to color coding
  - Provide high-contrast mode option
  - Test with colorblind simulators
- **Rollback Plan:**
  - Monochrome theme available
  - Text-only mode preserves all information

**Risk 5: Memory Usage Increase**
- **Impact:** MEDIUM - Higher memory footprint for Rich objects
- **Probability:** MEDIUM - Rich tables consume more memory
- **Mitigation Strategy:**
  - Implement streaming output for large tables
  - Limit buffer size for formatted output
  - Add memory usage monitoring
  - Benchmark memory on 1M+ row datasets
- **Rollback Plan:**
  - Automatic fallback to simple format at memory threshold
  - Configurable memory limit

**Risk 6: User Resistance to Change**
- **Impact:** LOW - User preference for existing interface
- **Probability:** MEDIUM - Change aversion common
- **Mitigation Strategy:**
  - Gradual rollout with opt-in period (Phase 1: default off)
  - Clear documentation of benefits
  - Tutorial videos for new features
  - Beta testing with power users
- **Rollback Plan:**
  - Legacy format always available via flag
  - User choice respected in config file

### Low Risk Items

**Risk 7: Library Version Conflicts**
- **Impact:** LOW - Dependency conflicts with other packages
- **Probability:** LOW - Rich has minimal dependencies
- **Mitigation Strategy:**
  - Pin Rich to specific version in pyproject.toml
  - Use virtual environments for isolation
  - Test dependency resolution
- **Rollback Plan:**
  - downgrade Rich to compatible version
  - Remove Rich if conflicts unresolvable

### Risk Monitoring

**Metrics to Track:**
1. **Performance:** Startup time, large dataset processing time
2. **Memory:** Peak memory usage, memory leaks
3. **Compatibility:** Bug reports by terminal type
4. **User Satisfaction:** Feedback on new features, adoption rate

**Warning Signs:**
- 20%+ performance degradation on benchmarks
- Memory usage increase > 50%
- More than 5 bug reports related to formatting
- User adoption < 20% after 1 month

---

## 7. Success Criteria

### Phase 1 Success Criteria (Week 2)

**Functional Requirements:**
- [ ] All existing tests pass without modification
- [ ] New formatter tests achieve 85%+ coverage
- [ ] Users can switch between formats via `--format` flag
- [ ] Legacy output format remains 100% functional
- [ ] No breaking changes to existing analyzer scripts

**Performance Requirements:**
- [ ] Startup time increase < 100ms
- [ ] Memory usage increase < 10MB
- [ ] Formatter initialization < 50ms

**Quality Requirements:**
- [ ] Zero ruff linting errors
- [ ] Zero mypy type errors
- [ ] All formatters have docstrings
- [ ] Code review approved by 2+ developers

### Phase 2 Success Criteria (Week 5)

**Functional Requirements:**
- [ ] Progress bars display for operations > 2 seconds
- [ ] Tables formatted with type-specific colors
- [ ] Error messages color-coded by severity
- [ ] Terminal width detection works correctly
- [ ] Pager integration for results > 100 rows

**Performance Requirements:**
- [ ] Progress overhead < 5% for small datasets
- [ ] Table formatting < 200ms for 1000 rows
- [ ] No slowdown for operations < 2 seconds

**User Experience Requirements:**
- [ ] Color coding improves error comprehension
- [ ] Progress indicators reduce perceived wait time
- [ ] Table formatting enhances data readability

### Phase 3 Success Criteria (Week 8)

**Functional Requirements:**
- [ ] Command history persists across sessions
- [ ] Tab completion works for SQL keywords
- [ ] Screen reader mode provides text-only output
- [ ] High-contrast theme meets WCAG AAA standards
- [ ] User configuration file saves preferences

**Accessibility Requirements:**
- [ ] Screen reader testing passed (NVDA, JAWS)
- [ ] Colorblind-friendly palette confirmed
- [ ] Keyboard-only navigation functional
- [ ] Text-only mode preserves all information

**Adoption Requirements:**
- [ ] 50%+ of users adopt new formatting (opt-in)
- [ ] User satisfaction rating > 4.0/5.0
- [ ] Zero requests to revert to legacy format
- [ ] Feature requests for additional enhancements

### Overall Project Success Criteria

**Technical Excellence:**
- [ ] 85%+ test coverage across all new code
- [ ] Zero known critical bugs
- [ ] Zero security vulnerabilities
- [ ] Documentation completeness > 90%

**User Impact:**
- [ ] Measurable improvement in user experience
- [ ] Reduced time to debug errors
- [ ] Enhanced data comprehension
- [ ] Positive user feedback > 80%

**Maintainability:**
- [ ] Clear separation of formatters from core logic
- [ ] Easy to add new formatters via plugin pattern
- [ ] Comprehensive inline documentation
- [ ] Example code for custom formatters

---

## 8. Next Steps

### Immediate Actions (Week 1)

1. **Review and Approve Plan**
   - Stakeholder review of implementation approach
   - Risk assessment validation
   - Resource allocation confirmation

2. **Setup Development Environment**
   - Create feature branch: `feature/cli-formatting-enhancement`
   - Install Rich library: `pip install rich>=13.7.0`
   - Setup formatter test suite

3. **Begin Phase 1 Implementation**
   - Create `duckdb_processor/formatters/` directory
   - Implement BaseFormatter abstract class
   - Add unit tests for formatters

### Documentation and Communication

1. **User Documentation**
   - Update README.md with new formatting features
   - Create migration guide for existing users
   - Add examples of new formatting capabilities

2. **Developer Documentation**
   - Document formatter API for extensions
   - Add examples of custom formatters
   - Create troubleshooting guide

3. **Release Notes**
   - Phase 1 release notes (Rich library integration)
   - Phase 2 release notes (Progress indicators, enhanced tables)
   - Phase 3 release notes (REPL enhancements, accessibility)

---

## Appendix A: EARS Specification Examples

### Example 1: Ubiquitous Requirement

**Requirement:** The system shall maintain backward compatibility with existing output format.

**EARS Pattern:** Ubiquitous (always active)

**Test Scenario:**
```gherkin
Given existing analyzer script using print(df.to_string(index=False))
When script is executed with new formatting system
Then output matches legacy format exactly
And script executes without modification
```

### Example 2: Event-Driven Requirement

**Requirement:** WHEN user enables rich formatting, the system shall use Rich library for output.

**EARS Pattern:** Event-Driven (trigger-response)

**Test Scenario:**
```gherkin
Given user specifies --format rich flag
When DataFrame is displayed
Then output uses Rich table formatting
And colors are enabled if terminal supports
```

### Example 3: State-Driven Requirement

**Requirement:** IF terminal supports ANSI color codes, the system shall enable color-coded output.

**EARS Pattern:** State-Driven (conditional behavior)

**Test Scenario:**
```gherkin
Given terminal supports ANSI color codes
When error message is displayed
Then error text is colored red
And hint text is colored yellow
```

---

## Appendix B: Reference Implementations

### B.1 Current Output Patterns

**Location:** `duckdb_processor/cli.py:97-136`

**Current Implementation:**
```python
def interactive_repl(p: Processor) -> None:
    """Minimal interactive SQL REPL."""
    print("\n── Interactive SQL REPL ─────────────────────────────────")
    print(f"  Table: '{p.table}'  |  Type EXIT to quit  |  \\schema for columns")
    print("─" * 58)

    while True:
        try:
            query = input("\nsql> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if query.upper() in ("EXIT", "QUIT", "\\Q"):
            print("Bye.")
            break
        if query == "\\schema":
            print(p.schema().to_string(index=False))
            continue

        try:
            print(p.sql(query).to_string(index=False))
        except Exception as e:
            print(f"  {e}")
```

**Enhancement Target:** Replace with enhanced REPL in Phase 3

### B.2 External Best Practice: Rich Library Table

**Source:** https://rich.readthedocs.io/en/stable/tables.html

**Reference Implementation:**
```python
from rich.console import Console
from rich.table import Table

console = Console()

table = Table(show_header=True, header_style="bold magenta")
table.add_column("Released", style="dim", width=12)
table.add_column("Title", style="bold yellow")
table.add_column("Box Office", justify="right", style="bold cyan")

table.add_row("Dec 20, 2019", "Star Wars: The Rise of Skywalker", "$952,110,690")
table.add_row("May 25, 2018", "Solo: A Star Wars Story", "$393,151,347")

console.print(table)
```

**Adaptation for DuckDB CSV Processor:**
```python
def format_dataframe(self, df: pd.DataFrame) -> None:
    table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            table.add_column(col, justify="right", style="cyan")
        else:
            table.add_column(col, style="white")
    for _, row in df.iterrows():
        table.add_row(*row.astype(str).tolist())
    self.console.print(table)
```

---

**Document Status:** Ready for Review
**Next Action:** Present plan to user for annotation cycle
**Timeline:** 8 weeks total (2 weeks per phase)
**Team Size:** 1-2 developers
**Risk Level:** Medium (mitigated by phased approach)
