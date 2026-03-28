# SPEC-CLI-001: CLI Output Formatting Enhancement

---
id: "SPEC-CLI-001"
version: "1.0.0"
status: "draft"
created: "2026-03-28"
updated: "2026-03-28"
author: "CJ-1981"
priority: "High"
issue_number: 0
---

## HISTORY

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0.0 | 2026-03-28 | Initial SPEC creation | CJ-1981 |

---

## Environment

### System Context

**Project:** DuckDB CSV Processor
**Description:** High-performance structured data analysis toolkit with DuckDB integration
**Current State:** Basic CLI with pandas DataFrame string representation output
**Target State:** Professional-grade terminal formatting with Rich library integration

### Technical Environment

**Runtime:**
- Python 3.10+
- DuckDB >= 0.9.0
- Pandas >= 2.0.0

**Current Dependencies:**
- duckdb>=0.9.0
- pandas>=2.0.0
- click>=8.0.0

**New Dependencies:**
- rich>=13.7.0 (terminal formatting)
- tabulate>=0.9.0 (fallback table formatting)

### Operating Environment

**Supported Platforms:**
- Linux (Ubuntu, Debian, RHEL, Alpine)
- macOS (10.15+)
- Windows (Windows 10+, Windows Server 2019+)

**Terminal Support:**
- ANSI color code compatible terminals
- Legacy terminal fallback support
- Screen reader compatibility

---

## Assumptions

### Technical Assumptions

**ASSUMPTION-001:** Rich library version 13.7.0+ is stable and production-ready
- **Confidence:** High
- **Evidence:** Rich is industry-leading with 70K+ GitHub stars, regular releases, MIT license
- **Risk if Wrong:** Dependency instability may cause rollout delays
- **Validation Method:** Check Rich GitHub issues for critical bugs, verify release stability

**ASSUMPTION-002:** Existing analyzer scripts depend on current print() output format
- **Confidence:** High
- **Evidence:** Code analysis shows 50+ print statements using df.to_string(index=False)
- **Risk if Wrong:** Breaking changes disrupt user workflows
- **Validation Method:** Review existing analyzer examples, test backward compatibility

**ASSUMPTION-003:** Terminal width detection is reliable on target platforms
- **Confidence:** Medium
- **Evidence:** shutil.get_terminal_size() is standard library, but may fail in redirected output
- **Risk if Wrong:** Formatting may break in piped/output-redirected scenarios
- **Validation Method:** Test with various terminal types and output redirection scenarios

### Business Assumptions

**ASSUMPTION-004:** Users desire enhanced formatting without workflow disruption
- **Confidence:** High
- **Evidence:** Research indicates demand for professional CLI appearance
- **Risk if Wrong:** Low adoption if changes require significant workflow changes
- **Validation Method:** Beta testing with power users, feedback collection

**ASSUMPTION-005:** Performance overhead of Rich is acceptable for typical use cases
- **Confidence:** Medium
- **Evidence:** Rich is optimized for performance, but overhead exists
- **Risk if Wrong:** User experience degradation on large datasets
- **Validation Method:** Performance benchmarking before/after, set acceptable thresholds

### User Assumptions

**ASSUMPTION-006:** Users have terminals supporting ANSI color codes
- **Confidence:** Medium
- **Evidence:** Most modern terminals support colors, but legacy systems exist
- **Risk if Wrong:** Broken output on legacy terminals
- **Validation Method:** Implement fallback detection, test on legacy terminals

**ASSUMPTION-007:** Users prefer progressive disclosure of advanced features
- **Confidence:** High
- **Evidence:** Industry best practices for feature rollout
- **Risk if Wrong:** Feature overwhelm reduces adoption
- **Validation Method:** Gradual rollout with user feedback monitoring

---

## Requirements

### Module 1: Rich Library Integration Foundation

**Ubiquitous Requirements:**

**REQ-001:** The system shall maintain backward compatibility with existing output format.
- **Rationale:** Existing analyzer scripts depend on current print(df.to_string()) behavior
- **Priority:** High
- **Traceability:** plan.md Phase 1 acceptance criteria

**REQ-002:** The system shall support both Rich and Simple formatter implementations.
- **Rationale:** Enables gradual migration and fallback support
- **Priority:** High
- **Traceability:** plan.md Module 1 architecture

**REQ-003:** The system shall provide configuration option for formatter selection.
- **Rationale:** User control over output format behavior
- **Priority:** Medium
- **Traceability:** plan.md Configuration section

**Event-Driven Requirements:**

**REQ-004:** WHEN user enables rich formatting, the system shall use Rich library for output.
- **Rationale:** Explicit user opt-in for enhanced formatting
- **Priority:** High
- **Traceability:** plan.md Phase 1 deliverables

**REQ-005:** WHEN user specifies --format simple, the system shall use legacy pandas.to_string() format.
- **Rationale:** Legacy format preservation for compatibility
- **Priority:** High
- **Traceability:** plan.md Backward compatibility requirements

**REQ-006:** WHEN terminal does not support color, the system shall automatically fallback to SimpleFormatter.
- **Rationale:** Graceful degradation on legacy terminals
- **Priority:** Medium
- **Traceability:** plan.md Terminal compatibility section

**State-Driven Requirements:**

**REQ-007:** IF terminal supports ANSI color codes, the system shall enable color-coded output.
- **Rationale:** Enhanced user experience on compatible terminals
- **Priority:** Medium
- **Traceability:** plan.md Terminal detection logic

**REQ-008:** IF formatter is not explicitly configured, the system shall use RichFormatter by default.
- **Rationale:** Progressive enhancement with opt-out capability
- **Priority:** Medium
- **Traceability:** plan.md Default behavior specification

**REQ-009:** IF Rich library is not installed, the system shall gracefully fallback to SimpleFormatter.
- **Rationale:** Resilience to dependency issues
- **Priority:** High
- **Traceability:** plan.md Dependency management

**Unwanted Requirements:**

**REQ-010:** The system shall not break existing analyzer scripts that depend on print() output.
- **Rationale:** Zero-breaking-change requirement for user trust
- **Priority:** Critical
- **Traceability:** plan.md Risk mitigation section

**REQ-011:** The system shall not require code changes in existing analyst implementations.
- **Rationale:** Analyst plugin API must remain stable
- **Priority:** High
- **Traceability:** plan.md API stability requirements

**REQ-012:** The system shall not force users to adopt new formatting immediately.
- **Rationale:** User choice and gradual migration path
- **Priority:** High
- **Traceability:** plan.md Migration strategy

**Optional Requirements:**

**REQ-013:** WHERE possible, the system shall provide automatic terminal capability detection.
- **Rationale:** Enhanced UX without configuration burden
- **Priority:** Low
- **Traceability:** plan.md Smart defaults

**REQ-014:** WHERE possible, the system shall support custom color themes via configuration.
- **Rationale:** User customization and accessibility
- **Priority:** Low
- **Traceability:** plan.md Theme system

**REQ-015:** WHERE possible, the system shall allow user-defined formatter plugins.
- **Rationale:** Extensibility for custom use cases
- **Priority:** Low
- **Traceability:** plan.md Plugin architecture

---

### Module 2: Progress Indicators

**Ubiquitous Requirements:**

**REQ-016:** The system shall display progress during long-running operations.
- **Rationale:** User feedback during extended operations
- **Priority:** High
- **Traceability:** plan.md Phase 2 progress bars

**REQ-017:** The system shall estimate time remaining for progress operations.
- **Rationale:** Enhanced UX with completion prediction
- **Priority:** Medium
- **Traceability:** plan.md ETA calculation

**REQ-018:** The system shall allow users to disable progress indicators via --no-progress flag.
- **Rationale:** User control and script compatibility
- **Priority:** Medium
- **Traceability:** plan.md Configuration options

**Event-Driven Requirements:**

**REQ-019:** WHEN loading CSV files larger than 10MB, the system shall show progress indicator.
- **Rationale:** Progress for operations with noticeable duration
- **Priority:** High
- **Traceability:** plan.md loader.py progress integration

**REQ-020:** WHEN running analysis operations, the system shall show progress for each analyzer.
- **Rationale:** Feedback during multi-step analysis workflows
- **Priority:** Medium
- **Traceability:** plan.md analyzer.py progress tracking

**REQ-021:** WHEN exporting data, the system shall show export progress.
- **Rationale:** Feedback during export operations
- **Priority:** Medium
- **Traceability:** plan.md Export progress tracking

**State-Driven Requirements:**

**REQ-022:** IF operation completes in less than 2 seconds, the system may skip progress display.
- **Rationale:** Avoid clutter for instant operations
- **Priority:** Low
- **Traceability:** plan.md Fast operation handling

**REQ-023:** IF operation is cancelled by user, the system shall display cancellation message.
- **Rationale:** Clear communication of user action
- **Priority:** Medium
- **Traceability:** plan.md Cancellation handling

**REQ-024:** IF progress cannot be determined, the system shall display indeterminate progress spinner.
- **Rationale:** Feedback even when progress unknown
- **Priority:** Medium
- **Traceability:** plan.md Indeterminate progress

**Unwanted Requirements:**

**REQ-025:** The system shall not display progress for instant operations (< 500ms).
- **Rationale:** Prevent flickering and visual noise
- **Priority:** Medium
- **Traceability:** plan.md Performance optimization

**REQ-026:** The system shall not slow down operations with progress overhead.
- **Rationale:** Performance impact minimization
- **Priority:** High
- **Traceability:** plan.md Overhead limits

**REQ-027:** The system shall not clutter output with excessive progress updates.
- **Rationale:** Clean output readability
- **Priority:** Medium
- **Traceability:** plan.md Update frequency

**Optional Requirements:**

**REQ-028:** WHERE possible, the system shall display ETA (Estimated Time of Arrival).
- **Rationale:** Enhanced completion prediction
- **Priority:** Low
- **Traceability:** plan.md Time estimation

**REQ-029:** WHERE possible, the system shall show throughput metrics (rows/sec, MB/sec).
- **Rationale:** Performance insight for users
- **Priority:** Low
- **Traceability:** plan.md Throughput calculation

**REQ-030:** WHERE possible, the system shall allow progress bar customization via configuration.
- **Rationale:** User customization options
- **Priority:** Low
- **Traceability:** plan.md Progress configuration

---

### Module 3: Enhanced Table Formatting

**Ubiquitous Requirements:**

**REQ-031:** The system shall format DataFrames with type-aware column styling.
- **Rationale:** Visual distinction for data types improves readability
- **Priority:** High
- **Traceability:** plan.md Type-aware formatting

**REQ-032:** The system shall respect terminal width constraints.
- **Rationale:** Prevent terminal overflow and broken output
- **Priority:** High
- **Traceability:** plan.md Terminal width handling

**REQ-033:** The system shall provide fallback formatting for wide tables.
- **Rationale:** Graceful handling of oversized tables
- **Priority:** Medium
- **Traceability:** plan.md Overflow handling

**Event-Driven Requirements:**

**REQ-034:** WHEN displaying query results, the system shall apply Rich table formatting.
- **Rationale:** Enhanced result presentation
- **Priority:** High
- **Traceability:** plan.md Query result formatting

**REQ-035:** WHEN table exceeds terminal width, the system shall enable smart truncation.
- **Rationale:** Prevent overflow while preserving data
- **Priority:** High
- **Traceability:** plan.md Truncation logic

**REQ-036:** WHEN displaying large DataFrames, the system shall show head and tail with row count.
- **Rationale:** Balance between detail and overview
- **Priority:** Medium
- **Traceability:** plan.md Large dataset handling

**State-Driven Requirements:**

**REQ-037:** IF DataFrame has more than 50 rows, the system shall display truncated view with count.
- **Rationale:** Default threshold for large dataset display
- **Priority:** Medium
- **Traceability:** plan.md Truncation threshold

**REQ-038:** IF column width exceeds available terminal width, the system shall wrap or truncate content.
- **Rationale:** Adaptive column sizing
- **Priority:** Medium
- **Traceability:** plan.md Column width management

**REQ-039:** IF terminal width cannot be detected, the system shall default to 80-character width.
- **Rationale:** Fallback for undetectable terminal width
- **Priority:** Low
- **Traceability:** plan.md Default width

**Unwanted Requirements:**

**REQ-040:** The system shall not overflow terminal width with wide tables.
- **Rationale:** Prevent broken output on narrow terminals
- **Priority:** High
- **Traceability:** plan.md Overflow prevention

**REQ-041:** The system shall not display empty tables without message.
- **Rationale:** Clear communication of empty result sets
- **Priority:** Medium
- **Traceability:** plan.md Empty table handling

**REQ-042:** The system shall not lose data precision in numeric display.
- **Rationale:** Maintain data integrity in presentation
- **Priority:** High
- **Traceability:** plan.md Precision preservation

**Optional Requirements:**

**REQ-043:** WHERE possible, the system shall detect terminal width and adjust column widths dynamically.
- **Rationale:** Optimal space utilization
- **Priority:** Low
- **Traceability:** plan.md Dynamic sizing

**REQ-044:** WHERE possible, the system shall provide alternate display formats (JSON, CSV, Markdown).
- **Rationale:** Alternative output formats for different use cases
- **Priority:** Low
- **Traceability:** plan.md Format alternatives

**REQ-045:** WHERE possible, the system shall support pager integration for large results.
- **Rationale:** Enhanced navigation for large datasets
- **Priority:** Low
- **Traceability:** plan.md Pager support

---

### Module 4: Error Message Enhancement

**Ubiquitous Requirements:**

**REQ-046:** The system shall categorize errors by severity level (ERROR, WARNING, INFO).
- **Rationale:** Clear error communication
- **Priority:** High
- **Traceability:** plan.md Error categorization

**REQ-047:** The system shall use consistent error message format across all operations.
- **Rationale:** Predictable error presentation
- **Priority:** Medium
- **Traceability:** plan.md Error format standardization

**REQ-048:** The system shall preserve full error details for debugging.
- **Rationale:** Debug information preservation
- **Priority:** Medium
- **Traceability:** plan.md Debug mode support

**Event-Driven Requirements:**

**REQ-049:** WHEN error occurs, the system shall display color-coded error message.
- **Rationale:** Visual distinction for errors
- **Priority:** High
- **Traceability:** plan.md Color coding

**REQ-050:** WHEN SQL execution fails, the system shall highlight syntax errors.
- **Rationale:** SQL debugging assistance
- **Priority:** Medium
- **Traceability:** plan.md SQL error highlighting

**REQ-051:** WHEN file operation fails, the system shall provide file path and error reason.
- **Rationale:** Clear file error communication
- **Priority:** High
- **Traceability:** plan.md File error context

**State-Driven Requirements:**

**REQ-052:** IF error is recoverable, the system shall provide helpful suggestion.
- **Rationale:** Actionable error messages
- **Priority:** Medium
- **Traceability:** plan.md Helpful suggestions

**REQ-053:** IF error is critical, the system shall display clear error message with exit instructions.
- **Rationale:** Clear critical error communication
- **Priority:** High
- **Traceability:** plan.md Critical error handling

**REQ-054:** IF debug mode is enabled, the system shall display full exception traceback.
- **Rationale:** Enhanced debugging information
- **Priority:** Medium
- **Traceability:** plan.md Debug mode

**Unwanted Requirements:**

**REQ-055:** The system shall not display raw exception tracebacks to end users in normal mode.
- **Rationale:** User-friendly error presentation
- **Priority:** High
- **Traceability:** plan.md Traceback suppression

**REQ-056:** The system shall not use technical jargon without explanation.
- **Rationale:** Accessible error messages
- **Priority:** Medium
- **Traceability:** plan.md Jargon avoidance

**REQ-057:** The system shall not hide error details needed for troubleshooting.
- **Rationale:** Debug information availability
- **Priority:** High
- **Traceability:** plan.md Debug detail preservation

**Optional Requirements:**

**REQ-058:** WHERE possible, the system shall suggest corrective actions for common errors.
- **Rationale:** Proactive error resolution guidance
- **Priority:** Low
- **Traceability:** plan.md Error suggestions

**REQ-059:** WHERE possible, the system shall provide error code for documentation lookup.
- **Rationale:** Error reference support
- **Priority:** Low
- **Traceability:** plan.md Error codes

**REQ-060:** WHERE possible, the system shall link to online help resources for complex errors.
- **Rationale:** Extended help access
- **Priority:** Low
- **Traceability:** plan.md Online help links

---

### Module 5: Configuration and Accessibility

**Ubiquitous Requirements:**

**REQ-061:** The system shall respect user preferences for output formatting.
- **Rationale:** User control over output behavior
- **Priority:** High
- **Traceability:** plan.md User preferences

**REQ-062:** The system shall support command-line flags for format control.
- **Rationale:** Runtime format configuration
- **Priority:** High
- **Traceability:** plan.md CLI flags

**REQ-063:** The system shall not rely solely on color to convey information.
- **Rationale:** Accessibility requirement
- **Priority:** High
- **Traceability:** plan.md Color independence

**Event-Driven Requirements:**

**REQ-064:** WHEN user specifies --no-color, the system shall disable color output.
- **Rationale:** User control over color output
- **Priority:** High
- **Traceability:** plan.md Color control flag

**REQ-065:** WHEN user specifies --format, the system shall use specified formatter.
- **Rationale:** Explicit formatter selection
- **Priority:** High
- **Traceability:** plan.md Format flag

**REQ-066:** WHEN screen reader is detected, the system shall use text-only mode.
- **Rationale:** Screen reader compatibility
- **Priority:** Medium
- **Traceability:** plan.md Screen reader detection

**State-Driven Requirements:**

**REQ-067:** IF user has color preference in config file, the system shall respect it.
- **Rationale:** Persistent user preferences
- **Priority:** Medium
- **Traceability:** plan.md Config file support

**REQ-068:** IF terminal does not support color, the system shall automatically disable colors.
- **Rationale:** Automatic capability detection
- **Priority:** High
- **Traceability:** plan.md Auto color disable

**REQ-069:** IF accessibility mode is enabled, the system shall use high-contrast colors.
- **Rationale:** Accessibility enhancement
- **Priority:** Medium
- **Traceability:** plan.md High contrast mode

**Unwanted Requirements:**

**REQ-070:** The system shall not rely solely on color to convey critical information.
- **Rationale:** Accessibility compliance
- **Priority:** High
- **Traceability:** plan.md Color independence

**REQ-071:** The system shall not ignore user-specified formatting preferences.
- **Rationale:** User control respect
- **Priority:** High
- **Traceability:** plan.md Preference respect

**REQ-072:** The system shall not force accessibility mode without user consent.
- **Rationale:** User choice in accessibility
- **Priority:** Medium
- **Traceability:** plan.md Opt-in accessibility

**Optional Requirements:**

**REQ-073:** WHERE possible, the system shall provide configuration file for persistent preferences.
- **Rationale:** Persistent settings
- **Priority:** Low
- **Traceability:** plan.md Config file

**REQ-074:** WHERE possible, the system shall support theme selection (default, high-contrast, monochrome).
- **Rationale:** Theme customization
- **Priority:** Low
- **Traceability:** plan.md Theme system

**REQ-075:** WHERE possible, the system shall detect screen reader and enable accessibility mode automatically.
- **Rationale:** Automatic accessibility
- **Priority:** Low
- **Traceability:** plan.md Auto accessibility

---

## Specifications

### Component Specification 1: Formatter Architecture

**BaseFormatter Interface:**

```python
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any

class BaseFormatter(ABC):
    """Abstract base class for output formatters."""

    @abstractmethod
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize formatter with configuration."""
        pass

    @abstractmethod
    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """Display a DataFrame with appropriate formatting."""
        pass

    @abstractmethod
    def format_info(self, metadata: Dict[str, Any]) -> None:
        """Display dataset information banner."""
        pass

    @abstractmethod
    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """Display error message with appropriate styling."""
        pass

    @abstractmethod
    def format_progress(self, message: str, current: int, total: int) -> None:
        """Display progress indicator."""
        pass
```

**RichFormatter Implementation:**

```python
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TaskProgressColumn
import pandas as pd

class RichFormatter(BaseFormatter):
    """Rich library-based formatter with colors and progress bars."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.console = Console()
        self.color_enabled = config.get('color_enabled', True) if config else True
        self.max_rows = config.get('max_rows', 50) if config else 50

    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """Display DataFrame with type-aware styling."""
        table = Table(show_header=True, header_style="bold magenta")

        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                table.add_column(col, justify="right", style="cyan")
            else:
                table.add_column(col, style="white")

        display_df = df
        if len(df) > (max_rows or self.max_rows):
            display_df = pd.concat([df.head(25), df.tail(25)])

        for _, row in display_df.iterrows():
            table.add_row(*row.astype(str).tolist())

        self.console.print(table)

    def format_info(self, metadata: Dict[str, Any]) -> None:
        """Display dataset information banner."""
        from rich.panel import Panel
        info_text = "\n".join([
            f"Source: {metadata.get('source', 'N/A')}",
            f"Rows: {metadata.get('rows', 'N/A')}",
            f"Columns: {', '.join(metadata.get('columns', []))}"
        ])
        panel = Panel(info_text, title="Dataset Info", border_style="blue")
        self.console.print(panel)

    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """Display color-coded error message."""
        color_map = {"ERROR": "red", "WARNING": "yellow", "INFO": "blue"}
        color = color_map.get(severity, "white")
        self.console.print(f"[{color}]{severity}: {error}[/{color}]", file=sys.stderr)

    def format_progress(self, message: str, current: int, total: int) -> None:
        """Display progress bar."""
        with Progress(
            SpinnerColumn(),
            "[progress.description]{task.description}",
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task(message, total=total)
            progress.update(task, completed=current)
```

**SimpleFormatter Implementation:**

```python
class SimpleFormatter(BaseFormatter):
    """Legacy compatibility wrapper using pandas string representation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.max_rows = config.get('max_rows', 50) if config else 50

    def format_dataframe(self, df: pd.DataFrame, max_rows: Optional[int] = None) -> None:
        """Display DataFrame using pandas to_string() format."""
        display_df = df
        if len(df) > (max_rows or self.max_rows):
            display_df = pd.concat([df.head(25), df.tail(25)])
        print(display_df.to_string(index=False))

    def format_info(self, metadata: Dict[str, Any]) -> None:
        """Display dataset information using legacy format."""
        width = 58
        print("\n" + "─" * width)
        print("  DuckDB CSV Processor")
        print("─" * width)
        print(f"  Source      : {metadata.get('source', 'N/A')}")
        print(f"  Rows loaded : {metadata.get('rows', 'N/A')}")
        print(f"  Columns     : {', '.join(metadata.get('columns', []))}")
        print("─" * width + "\n")

    def format_error(self, error: Exception, context: str, severity: str = "ERROR") -> None:
        """Display error message in legacy format."""
        print(f"{severity}: {error}", file=sys.stderr)

    def format_progress(self, message: str, current: int, total: int) -> None:
        """Display simple progress indicator."""
        percent = (current / total) * 100 if total > 0 else 0
        print(f"{message}: {current}/{total} ({percent:.1f}%)")
```

### Component Specification 2: Configuration Management

**OutputConfig Dataclass:**

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class OutputConfig:
    """Configuration for output formatting behavior."""

    # Formatter selection
    formatter_type: str = "rich"  # 'rich' or 'simple'

    # Display options
    color_enabled: bool = True
    max_rows: int = 50
    max_columns: int = 15

    # Progress indicators
    progress_enabled: bool = True
    progress_min_duration: float = 2.0  # seconds

    # Table formatting
    terminal_width: Optional[int] = None  # None = auto-detect
    truncate_columns: bool = True

    # Accessibility
    high_contrast_mode: bool = False
    screen_reader_mode: bool = False

    @classmethod
    def from_args(cls, args) -> 'OutputConfig':
        """Create config from CLI arguments."""
        return cls(
            formatter_type=getattr(args, 'format', 'rich'),
            color_enabled=not getattr(args, 'no_color', False),
            progress_enabled=not getattr(args, 'no_progress', False)
        )

    @classmethod
    def detect_capabilities(cls) -> 'OutputConfig':
        """Auto-detect terminal capabilities."""
        import shutil
        config = cls()

        # Detect terminal width
        config.terminal_width = shutil.get_terminal_size().columns

        # Detect color support
        try:
            import os
            config.color_enabled = os.isatty(sys.stdout.fileno())
        except:
            config.color_enabled = False

        return config
```

### Component Specification 3: Terminal Detection

**Capability Detection Utilities:**

```python
import shutil
import sys
import os

def detect_terminal_width() -> int:
    """Detect terminal width with fallback."""
    try:
        return shutil.get_terminal_size().columns
    except:
        return 80  # Default fallback

def supports_color() -> bool:
    """Check if terminal supports ANSI color codes."""
    # Check if output is to a terminal
    if not hasattr(sys.stdout, 'isatty'):
        return False
    if not sys.stdout.isatty():
        return False

    # Check for NO_COLOR environment variable
    if os.environ.get('NO_COLOR'):
        return False

    # Platform-specific checks
    if sys.platform == 'win32':
        # Windows 10+ supports ANSI codes
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except:
            return False
    else:
        # Unix-like systems typically support colors
        return True

def detect_screen_reader() -> bool:
    """Detect screen reader usage."""
    # Check for common screen reader environment variables
    screen_reader_indicators = [
        'SCREEN_READER',
        'JAWS',
        'NVDA',
        'VOICE_OVER'
    ]
    return any(indicator in os.environ for indicator in screen_reader_indicators)
```

---

## Traceability

### Requirements to Components Mapping

**Formatter Architecture:**
- REQ-001 → BaseFormatter abstract class
- REQ-002 → RichFormatter and SimpleFormatter implementations
- REQ-003 → OutputConfig.formatter_type
- REQ-010 → SimpleFormatter backward compatibility

**Progress Indicators:**
- REQ-016 → BaseFormatter.format_progress()
- REQ-018 → OutputConfig.progress_enabled
- REQ-019 → CSV loading progress integration
- REQ-026 → Progress overhead monitoring

**Table Formatting:**
- REQ-031 → RichFormatter.format_dataframe() type-aware styling
- REQ-032 → Terminal width detection
- REQ-040 → Overflow prevention logic

**Error Messages:**
- REQ-046 → Error severity categorization
- REQ-049 → Color-coded error display
- REQ-055 → Traceback suppression in normal mode

**Configuration:**
- REQ-062 → CLI flags implementation
- REQ-064 → --no-color flag
- REQ-068 → Automatic color disable

### Module Integration Points

**Phase 1 (Foundation):**
- duckdb_processor/formatters/ directory creation
- BaseFormatter, RichFormatter, SimpleFormatter classes
- OutputConfig configuration system
- CLI flag integration (--format, --no-color, --no-progress)

**Phase 2 (Core Features):**
- Progress indicator integration in loader.py
- Enhanced table formatting in processor.py
- Error message formatting in cli.py
- Terminal width detection utilities

**Phase 3 (Advanced Features):**
- Enhanced REPL with command history (repl.py)
- Pager integration for large results
- Accessibility features (high contrast, screen reader mode)
- User configuration file support

---

## Non-Functional Requirements

### Performance Requirements

**NFR-001:** Formatting overhead shall not exceed 5% for datasets under 1000 rows.
- **Measurement:** Benchmark timing comparison
- **Priority:** High
- **Traceability:** plan.md Performance testing

**NFR-002:** Memory usage increase shall be under 10MB for typical operations.
- **Measurement:** Memory profiling
- **Priority:** Medium
- **Traceability:** plan.md Memory overhead

**NFR-003:** Startup time increase shall be under 100ms with Rich integration.
- **Measurement:** Startup timing benchmarks
- **Priority:** Medium
- **Traceability:** plan.md Startup performance

### Reliability Requirements

**NFR-004:** System shall gracefully fallback to SimpleFormatter on Rich errors.
- **Measurement:** Error handling testing
- **Priority:** High
- **Traceability:** plan.md Fallback logic

**NFR-005:** System shall maintain 100% backward compatibility with existing analyzers.
- **Measurement:** Integration test suite
- **Priority:** Critical
- **Traceability:** plan.md Compatibility requirements

### Usability Requirements

**NFR-006:** All color-coded information shall have text alternatives.
- **Measurement:** Accessibility audit
- **Priority:** High
- **Traceability:** plan.md Color independence

**NFR-007:** Error messages shall provide actionable guidance for common issues.
- **Measurement:** User feedback testing
- **Priority:** Medium
- **Traceability:** plan.md Error usability

### Maintainability Requirements

**NFR-008:** Code coverage for new formatter code shall be 85% or higher.
- **Measurement:** pytest coverage reporting
- **Priority:** High
- **Traceability:** plan.md Quality gates

**NFR-009:** All new code shall pass ruff linting and mypy type checking.
- **Measurement:** CI/CD quality gates
- **Priority:** High
- **Traceability:** plan.md Code quality

### Compatibility Requirements

**NFR-010:** System shall support Python 3.10+ without syntax errors.
- **Measurement:** Multi-version testing
- **Priority:** High
- **Traceability:** plan.md Python version support

**NFR-011:** System shall work on Linux, macOS, and Windows 10+.
- **Measurement:** Cross-platform testing
- **Priority:** High
- **Traceability:** plan.md Platform support

---

## Security Considerations

### Input Validation

**SEC-001:** Terminal width detection shall validate input ranges.
- **Rationale:** Prevent integer overflow or invalid values
- **Priority:** Medium
- **Mitigation:** Input sanitization and bounds checking

**SEC-002:** Configuration file parsing shall validate schema.
- **Rationale:** Prevent configuration injection attacks
- **Priority:** Medium
- **Mitigation:** Schema validation with pydantic

### Output Sanitization

**SEC-003:** Error message display shall sanitize user input.
- **Rationale:** Prevent log injection or output manipulation
- **Priority:** High
- **Mitigation:** Escape sequences and validation

**SEC-004:** Table formatting shall handle special characters safely.
- **Rationale:** Prevent terminal escape sequence injection
- **Priority:** High
- **Mitigation:** Rich library built-in sanitization

### Dependency Security

**SEC-005:** Rich library dependency shall be pinned to stable version.
- **Rationale:** Prevent supply chain attacks from latest releases
- **Priority:** High
- **Mitigation:** Version pinning in pyproject.toml

**SEC-006:** All dependencies shall be scanned for known vulnerabilities.
- **Rationale:** Prevent dependency vulnerabilities
- **Priority:** High
- **Mitigation:** Automated dependency scanning in CI/CD

---

## Dependencies

### Required Dependencies

**Rich >= 13.7.0:**
- Purpose: Terminal formatting, progress bars, table rendering
- License: MIT (permissive)
- Justification: Industry-leading Python terminal formatting library
- Documentation: https://rich.readthedocs.io/

**Tabulate >= 0.9.0:**
- Purpose: Fallback table formatting for simple mode
- License: MIT (permissive)
- Justification: Lightweight table formatting alternative
- Documentation: https://github.com/astanin/python-tabulate

### Existing Dependencies

**DuckDB >= 0.9.0:**
- Purpose: In-memory SQL database engine
- Role: Core data processing

**Pandas >= 2.0.0:**
- Purpose: DataFrame manipulation
- Role: Data structure handling

**Click >= 8.0.0:**
- Purpose: CLI framework
- Role: Command-line interface

### Development Dependencies

**pytest >= 7.0.0:**
- Purpose: Testing framework
- Role: Test execution and discovery

**pytest-cov >= 4.0.0:**
- Purpose: Coverage reporting
- Role: Test coverage measurement

**black >= 22.0.0:**
- Purpose: Code formatting
- Role: Code style consistency

**ruff >= 0.1.0:**
- Purpose: Linting
- Role: Code quality checks

**mypy >= 1.0.0:**
- Purpose: Type checking
- Role: Static type validation

---

## Constraints

### Technical Constraints

**CONST-001:** Must maintain backward compatibility with existing analyzer API.
- **Impact:** Design must preserve existing function signatures
- **Mitigation:** Formatter abstraction layer

**CONST-002:** Must not increase memory usage beyond 10MB for typical operations.
- **Impact:** Efficient formatter implementation required
- **Mitigation:** Performance benchmarking and optimization

**CONST-003:** Must support Python 3.10+ without breaking changes.
- **Impact:** Code must be compatible with oldest supported Python version
- **Mitigation:** Multi-version testing

### Operational Constraints

**CONST-004:** Must not require changes to existing user scripts.
- **Impact:** Zero-breaking-change requirement
- **Mitigation:** Opt-in with configuration flags

**CONST-005:** Must provide fallback for terminals without color support.
- **Impact:** Graceful degradation required
- **Mitigation:** Terminal capability detection

### Business Constraints

**CONST-006:** Must be implementable within 8-week timeline.
- **Impact:** Phased rollout with incremental value
- **Mitigation:** Three-phase implementation approach

**CONST-007:** Must achieve 85%+ test coverage for new code.
- **Impact:** Comprehensive test suite required
- **Mitigation:** Test-driven development approach

---

## Success Criteria

### Phase 1 Success Criteria (Week 2)

**Functional:**
- [ ] All existing tests pass without modification
- [ ] New formatter tests achieve 85%+ coverage
- [ ] Users can switch between formats via --format flag
- [ ] Legacy output format remains 100% functional
- [ ] No breaking changes to existing analyzer scripts

**Performance:**
- [ ] Startup time increase < 100ms
- [ ] Memory usage increase < 10MB
- [ ] Formatter initialization < 50ms

**Quality:**
- [ ] Zero ruff linting errors
- [ ] Zero mypy type errors
- [ ] All formatters have docstrings
- [ ] Code review approved

### Phase 2 Success Criteria (Week 5)

**Functional:**
- [ ] Progress bars display for operations > 2 seconds
- [ ] Tables formatted with type-specific colors
- [ ] Error messages color-coded by severity
- [ ] Terminal width detection works correctly
- [ ] Pager integration for results > 100 rows

**Performance:**
- [ ] Progress overhead < 5% for small datasets
- [ ] Table formatting < 200ms for 1000 rows
- [ ] No slowdown for operations < 2 seconds

**User Experience:**
- [ ] Color coding improves error comprehension
- [ ] Progress indicators reduce perceived wait time
- [ ] Table formatting enhances data readability

### Phase 3 Success Criteria (Week 8)

**Functional:**
- [ ] Command history persists across sessions
- [ ] Tab completion works for SQL keywords
- [ ] Screen reader mode provides text-only output
- [ ] High-contrast theme meets WCAG AAA standards
- [ ] User configuration file saves preferences

**Accessibility:**
- [ ] Screen reader testing passed (NVDA, JAWS)
- [ ] Colorblind-friendly palette confirmed
- [ ] Keyboard-only navigation functional
- [ ] Text-only mode preserves all information

**Adoption:**
- [ ] 50%+ of users adopt new formatting (opt-in)
- [ ] User satisfaction rating > 4.0/5.0
- [ ] Zero requests to revert to legacy format
- [ ] Feature requests for additional enhancements

---

## References

### Project Documentation

**Product Context:**
- `.moai/project/product.md` - DuckDB CSV Processor product overview

**Architecture Documentation:**
- `.moai/project/structure.md` - Layered plugin architecture
- `.moai/project/tech.md` - Python 3.10+, DuckDB, pandas technology stack

### Research Documentation

**Research Artifact:**
- `.moai/specs/SPEC-CLI-001/research.md` - Comprehensive CLI output formatting research

**Implementation Plan:**
- `.moai/specs/SPEC-CLI-001/plan.md` - Detailed three-phase implementation approach

### External References

**Rich Library Documentation:**
- https://rich.readthedocs.io/ - Official Rich library documentation
- https://github.com/Textualize/rich - Rich GitHub repository

**Best Practices:**
- DuckDB CLI - Reference implementation for terminal formatting
- psql - PostgreSQL CLI with advanced formatting features
- csvkit - CSV processing suite with output formatting

---

**END OF SPEC DOCUMENT**
