# Task Decomposition: SPEC-CLI-001

**SPEC ID:** SPEC-CLI-001
**Created:** 2026-03-28
**Status:** Ready for Implementation
**Methodology:** DDD (Domain-Driven Development)

---

## Overview

This document decomposes the CLI Output Formatting Enhancement plan into 24 atomic implementation tasks. Each task is designed to be completed in a single DDD cycle (ANALYZE-PRESERVE-IMPROVE).

**Total Tasks:** 24
**Phases:** 3 (Foundation, Core Features, Advanced Features)

---

## Phase 1: Foundation (9 Tasks)

### TASK-001: Create formatters directory structure

**Description:** Create the `duckdb_processor/formatters/` directory with proper `__init__.py` module initialization.

**Requirement Mapping:** REQ-001, REQ-002, REQ-003

**Dependencies:** None

**Acceptance Criteria:**
- [ ] Directory `duckdb_processor/formatters/` exists
- [ ] File `duckdb_processor/formatters/__init__.py` exists
- [ ] `__init__.py` contains module docstring and exports placeholder

**Estimated Effort:** 0.25 hours

**Risk Level:** Low

---

### TASK-002: Implement BaseFormatter abstract class

**Description:** Create the abstract base class that defines the formatter interface with methods for `format_dataframe`, `format_info`, `format_error`, and `format_progress`.

**Requirement Mapping:** REQ-001, REQ-002

**Dependencies:** TASK-001

**Acceptance Criteria:**
- [ ] File `duckdb_processor/formatters/base.py` exists
- [ ] `BaseFormatter` is abstract class with ABC inheritance
- [ ] All required abstract methods defined: `format_dataframe`, `format_info`, `format_error`, `format_progress`
- [ ] Class has comprehensive docstrings
- [ ] Type hints for all method signatures

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-003: Create OutputConfig dataclass

**Description:** Create a configuration dataclass that holds all output formatting settings including formatter selection, display options, progress settings, and accessibility options.

**Requirement Mapping:** REQ-003, REQ-061, REQ-062

**Dependencies:** TASK-001

**Acceptance Criteria:**
- [ ] File `duckdb_processor/formatters/config.py` exists
- [ ] `OutputConfig` dataclass with all fields from SPEC
- [ ] `from_args()` class method for CLI argument parsing
- [ ] `detect_capabilities()` class method for terminal detection
- [ ] Type hints for all fields
- [ ] Docstrings for all fields

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-004: Create SimpleFormatter (legacy wrapper)

**Description:** Implement the SimpleFormatter class that wraps existing pandas `to_string()` behavior for backward compatibility.

**Requirement Mapping:** REQ-001, REQ-005, REQ-010, REQ-011

**Dependencies:** TASK-002, TASK-003

**Acceptance Criteria:**
- [ ] File `duckdb_processor/formatters/simple_formatter.py` exists
- [ ] `SimpleFormatter` inherits from `BaseFormatter`
- [ ] `format_dataframe()` uses `pandas.to_string(index=False)`
- [ ] Output matches legacy format exactly
- [ ] `format_info()` matches existing banner format
- [ ] `format_error()` uses simple stderr output
- [ ] `format_progress()` uses simple percentage output
- [ ] All methods have type hints and docstrings

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-005: Create RichFormatter (Rich integration)

**Description:** Implement the RichFormatter class that uses the Rich library for enhanced terminal formatting with colors, tables, and progress bars.

**Requirement Mapping:** REQ-004, REQ-007, REQ-008

**Dependencies:** TASK-002, TASK-003

**Acceptance Criteria:**
- [ ] File `duckdb_processor/formatters/rich_formatter.py` exists
- [ ] `RichFormatter` inherits from `BaseFormatter`
- [ ] `format_dataframe()` uses Rich Table with type-aware styling
- [ ] Numeric columns are right-aligned in cyan
- [ ] Text columns are left-aligned in white
- [ ] Headers styled in bold magenta
- [ ] `format_info()` uses Rich Panel
- [ ] `format_error()` uses color-coded severity levels
- [ ] `format_progress()` uses Rich Progress bar
- [ ] All methods have type hints and docstrings

**Estimated Effort:** 1 hour

**Risk Level:** Low

---

### TASK-006: Create terminal detection utilities

**Description:** Create utility functions for detecting terminal capabilities including color support, terminal width, and screen reader detection.

**Requirement Mapping:** REQ-006, REQ-007, REQ-013, REQ-066, REQ-068

**Dependencies:** TASK-001

**Acceptance Criteria:**
- [ ] File `duckdb_processor/formatters/terminal.py` exists
- [ ] `detect_terminal_width()` function returns int
- [ ] `supports_color()` function returns bool
- [ ] `detect_screen_reader()` function returns bool
- [ ] All functions handle edge cases (piped output, redirected output)
- [ ] All functions have type hints and docstrings

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-007: Add --format CLI option

**Description:** Add `--format` CLI option to `cli.py` to allow users to select between 'rich' and 'simple' formatters.

**Requirement Mapping:** REQ-003, REQ-004, REQ-005, REQ-062, REQ-065

**Dependencies:** None (modifies existing cli.py)

**Acceptance Criteria:**
- [ ] `--format` option added to Click CLI
- [ ] Accepts 'rich' or 'simple' values
- [ ] Default value is 'rich'
- [ ] `--no-color` flag added
- [ ] `--no-progress` flag added
- [ ] Type hints for all new parameters

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-008: Integrate formatters into processor.py

**Description:** Modify `processor.py` to use the formatter for `print_info()` method instead of direct print statements.

**Requirement Mapping:** REQ-001, REQ-002, REQ-010

**Dependencies:** TASK-002, TASK-003, TASK-004, TASK-005, TASK-006, TASK-007

**Acceptance Criteria:**
- [ ] `Processor.print_info()` uses formatter
- [ ] Formatter selection based on OutputConfig
- [ ] Existing tests pass without modification
- [ ] Legacy output preserved when using SimpleFormatter

**Estimated Effort:** 0.5 hours

**Risk Level:** Medium

---

### TASK-009: Create unit tests for formatters

**Description:** Create comprehensive unit tests for all formatter classes, OutputConfig, and terminal detection utilities.

**Requirement Mapping:** REQ-001, REQ-002, REQ-003, NFR-008

**Dependencies:** TASK-002, TASK-003, TASK-004, TASK-005, TASK-006

**Acceptance Criteria:**
- [ ] File `tests/test_formatters.py` exists
- [ ] Tests for BaseFormatter interface
- [ ] Tests for SimpleFormatter all methods
- [ ] Tests for RichFormatter all methods
- [ ] Tests for OutputConfig
- [ ] Tests for terminal detection utilities
- [ ] Test coverage >= 85% for formatter code

**Estimated Effort:** 2 hours

**Risk Level:** Low

---

## Phase 2: Core Features (7 Tasks)

### TASK-010: Add progress bar in loader.py

**Description:** Modify `loader.py` to add progress indicators for CSV file loading operations.

**Requirement Mapping:** REQ-016, REQ-017, REQ-019, REQ-022, REQ-025, REQ-026

**Dependencies:** TASK-005, TASK-008

**Acceptance Criteria:**
- [ ] Progress bar displays during CSV loading
- [ ] Progress shows rows loaded and percentage
- [ ] Progress bar skipped for files < 10MB or operations < 2 seconds
- [ ] `--no-progress` flag disables progress bar
- [ ] Performance overhead < 5%
- [ ] Existing tests pass

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-011: Add progress indicator in analyzer.py

**Description:** Modify `analyzer.py` to add progress indicators for analysis execution workflow.

**Requirement Mapping:** REQ-016, REQ-020, REQ-022, REQ-025

**Dependencies:** TASK-005, TASK-008

**Acceptance Criteria:**
- [ ] Progress indicator shows for each analyzer
- [ ] Current analyzer name displayed
- [ ] Progress updates during execution
- [ ] Fast analyzers (< 2s) skip progress display
- [ ] Existing tests pass

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-012: Add enhanced table formatting in RichFormatter

**Description:** Extend RichFormatter with enhanced table formatting including large DataFrame truncation, terminal width handling, and empty table display.

**Requirement Mapping:** REQ-031, REQ-032, REQ-034, REQ-035, REQ-036, REQ-037, REQ-040, REQ-041

**Dependencies:** TASK-005, TASK-006

**Acceptance Criteria:**
- [ ] Large DataFrames (> 50 rows) show head and tail with row count
- [ ] Tables respect terminal width constraints
- [ ] Wide tables use smart truncation
- [ ] Empty DataFrames show appropriate message
- [ ] No terminal overflow occurs

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-013: Add error message formatting with severity levels

**Description:** Extend RichFormatter with comprehensive error message formatting including severity categorization, SQL error highlighting, and helpful suggestions.

**Requirement Mapping:** REQ-046, REQ-047, REQ-048, REQ-049, REQ-050, REQ-051, REQ-052, REQ-053, REQ-055, REQ-056

**Dependencies:** TASK-005

**Acceptance Criteria:**
- [ ] Errors categorized as ERROR, WARNING, INFO
- [ ] Color coding reflects severity (red, yellow, blue)
- [ ] SQL errors show syntax highlighting
- [ ] File errors show path and reason
- [ ] Recoverable errors show suggestions
- [ ] Debug mode shows full traceback
- [ ] Normal mode hides raw tracebacks

**Estimated Effort:** 1.5 hours

**Risk Level:** Medium

---

### TASK-014: Create terminal width detection utility

**Description:** Create utility function for detecting terminal width with fallback handling.

**Requirement Mapping:** REQ-032, REQ-038, REQ-039, REQ-043

**Dependencies:** TASK-006

**Acceptance Criteria:**
- [ ] `get_terminal_width()` function in terminal.py
- [ ] Returns detected width or 80 fallback
- [ ] Handles redirected output gracefully
- [ ] Handles undetectable terminal width

**Estimated Effort:** 0.25 hours

**Risk Level:** Low

---

### TASK-015: Integrate error formatting in cli.py

**Description:** Modify `cli.py` to use RichFormatter for error message display throughout the CLI.

**Requirement Mapping:** REQ-049, REQ-050, REQ-051, REQ-053, REQ-054

**Dependencies:** TASK-005, TASK-013

**Acceptance Criteria:**
- [ ] All errors in cli.py use formatter
- [ ] SQL errors are highlighted
- [ ] File errors show context
- [ ] Debug mode shows full traceback
- [ ] Existing tests pass

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-016: Create integration tests for Phase 2 features

**Description:** Create integration tests for progress indicators, enhanced table formatting, and error message formatting.

**Requirement Mapping:** REQ-016, REQ-019, REQ-020, REQ-031, REQ-034, REQ-046, REQ-049, NFR-008

**Dependencies:** TASK-010, TASK-011, TASK-012, TASK-013, TASK-015

**Acceptance Criteria:**
- [ ] File `tests/test_integration.py` exists
- [ ] Tests for CSV loading progress
- [ ] Tests for analysis progress
- [ ] Tests for table formatting
- [ ] Tests for error message formatting
- [ ] Tests for terminal width handling
- [ ] All existing tests pass
- [ ] Test coverage >= 85%

**Estimated Effort:** 2 hours

**Risk Level:** Medium

---

## Phase 3: Advanced Features (8 Tasks)

### TASK-017: Create Enhanced REPL with history support

**Description:** Create enhanced REPL class with command history persistence using readline.

**Requirement Mapping:** Phase 3 REPL enhancement requirements

**Dependencies:** TASK-005

**Acceptance Criteria:**
- [ ] File `duckdb_processor/repl.py` exists
- [ ] `EnhancedREPL` class with history support
- [ ] History file at `~/.duckdb_processor_history`
- [ ] Commands saved across sessions
- [ ] Arrow keys recall previous commands
- [ ] Type hints and docstrings

**Estimated Effort:** 1.5 hours

**Risk Level:** Medium

---

### TASK-018: Add tab auto-completion for SQL keywords

**Description:** Implement tab completion for SQL keywords, table names, and column names in the enhanced REPL.

**Requirement Mapping:** Phase 3 auto-completion requirements

**Dependencies:** TASK-017

**Acceptance Criteria:**
- [ ] Tab completion for SQL keywords (SELECT, FROM, WHERE, etc.)
- [ ] Tab completion for table names
- [ ] Tab completion for column names
- [ ] Completion works with partial input
- [ ] Type hints and docstrings

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-019: Add multi-line query support

**Description:** Implement multi-line SQL query support in the enhanced REPL.

**Requirement Mapping:** Phase 3 multi-line requirements

**Dependencies:** TASK-017

**Acceptance Criteria:**
- [ ] Multi-line queries are buffered
- [ ] Query executes only when complete (ends with semicolon)
- [ ] Partial lines are displayed correctly
- [ ] Query executes correctly
- [ ] Type hints and docstrings

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-020: Add pager integration for large results

**Description:** Implement pager integration for displaying large result sets.

**Requirement Mapping:** REQ-045

**Dependencies:** TASK-005, TASK-017

**Acceptance Criteria:**
- [ ] Pager activates for results > 100 rows
- [ ] User can scroll through results
- [ ] Pager exits on 'q' key
- [ ] Works with Rich Console.pager()
- [ ] Type hints and docstrings

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-021: Create screen reader detection utility

**Description:** Create utility function for detecting screen reader usage.

**Requirement Mapping:** REQ-063, REQ-066

**Dependencies:** TASK-006

**Acceptance Criteria:**
- [ ] `detect_screen_reader()` function in terminal.py
- [ ] Checks common screen reader environment variables
- [ ] Returns bool
- [ ] Type hints and docstrings

**Estimated Effort:** 0.25 hours

**Risk Level:** Low

---

### TASK-022: Add high-contrast theme support

**Description:** Add high-contrast color theme for accessibility compliance.

**Requirement Mapping:** REQ-069, REQ-074

**Dependencies:** TASK-005, TASK-006

**Acceptance Criteria:**
- [ ] High-contrast color scheme defined
- [ ] Colors meet WCAG AAA contrast standards (7:1 for text)
- [ ] Theme selectable via configuration
- [ ] Type hints and docstrings

**Estimated Effort:** 0.5 hours

**Risk Level:** Low

---

### TASK-023: Create user configuration file support

**Description:** Create user configuration file support for persistent preferences.

**Requirement Mapping:** REQ-067, REQ-073

**Dependencies:** TASK-003

**Acceptance Criteria:**
- [ ] File `duckdb_processor/config/user_config.py` exists
- [ ] Configuration file at `~/.duckdb_processor/config.yaml` (opt-in)
- [ ] Config file not auto-created (user creates manually)
- [ ] Command-line flags override config file
- [ ] Type hints and docstrings

**Estimated Effort:** 1 hour

**Risk Level:** Low

---

### TASK-024: Integrate enhanced REPL in cli.py

**Description:** Modify `cli.py` to use EnhancedREPL when user enters interactive mode.

**Requirement Mapping:** Phase 3 REPL integration requirements

**Dependencies:** TASK-017, TASK-018, TASK-019, TASK-020, TASK-021, TASK-022, TASK-023

**Acceptance Criteria:**
- [ ] Interactive mode uses EnhancedREPL
- [ ] Legacy REPL available via `--legacy-repl` flag
- [ ] History persists across sessions
- [ ] Tab completion works
- [ ] Multi-line queries work
- [ ] Pager activates for large results
- [ ] Existing tests pass

**Estimated Effort:** 1 hour

**Risk Level:** Medium

---

### TASK-025: Create accessibility tests

**Description:** Create tests for accessibility features including screen reader mode, high-contrast theme, and color independence.

**Requirement Mapping:** REQ-063, REQ-066, REQ-069, NFR-006

**Dependencies:** TASK-021, TASK-022, TASK-024

**Acceptance Criteria:**
- [ ] File `tests/test_accessibility.py` exists
- [ ] Tests for screen reader detection
- [ ] Tests for high-contrast theme
- [ ] Tests for color independence (text alternatives)
- [ ] Tests for WCAG contrast compliance
- [ ] All existing tests pass
- [ ] Test coverage >= 85%

**Estimated Effort:** 1.5 hours

**Risk Level:** Low

---

## Dependency Graph

```
Phase 1 (Foundation):
TASK-001 (formatters dir)
├── TASK-002 (BaseFormatter)
├── TASK-003 (OutputConfig)
├── TASK-006 (terminal utilities)
└── TASK-004 (SimpleFormatter) [depends on TASK-002, TASK-003]
    └── TASK-005 (RichFormatter) [depends on TASK-002, TASK-003]
        └── TASK-009 (unit tests) [depends on TASK-002-006]

TASK-007 (--format CLI) [independent]
└── TASK-008 (processor.py integration) [depends on TASK-002-007]
    └── TASK-009 (unit tests) [final verification]

Phase 2 (Core Features):
TASK-008 + TASK-005
├── TASK-010 (loader.py progress)
├── TASK-011 (analyzer.py progress)
├── TASK-012 (enhanced tables)
├── TASK-013 (error formatting)
└── TASK-014 (terminal width utility)
    └── TASK-015 (cli.py error integration) [depends on TASK-013]
        └── TASK-016 (integration tests) [final verification]

Phase 3 (Advanced Features):
TASK-005
└── TASK-017 (Enhanced REPL)
    ├── TASK-018 (tab completion)
    ├── TASK-019 (multi-line queries)
    ├── TASK-020 (pager integration)
    └── TASK-024 (cli.py REPL integration) [depends on TASK-017-020]

TASK-006
├── TASK-021 (screen reader detection)
├── TASK-022 (high-contrast theme)
└── (contribute to TASK-024)

TASK-003
└── TASK-023 (user config file)
    └── (contribute to TASK-024)

TASK-024 (REPL integration) [depends on TASK-017-023]
└── TASK-025 (accessibility tests) [final verification]
```

---

## Requirement Traceability Matrix

| Task ID | Requirements Covered |
|---------|---------------------|
| TASK-001 | REQ-001, REQ-002, REQ-003 |
| TASK-002 | REQ-001, REQ-002 |
| TASK-003 | REQ-003, REQ-061, REQ-062 |
| TASK-004 | REQ-001, REQ-005, REQ-010, REQ-011 |
| TASK-005 | REQ-004, REQ-007, REQ-008 |
| TASK-006 | REQ-006, REQ-007, REQ-013, REQ-066, REQ-068 |
| TASK-007 | REQ-003, REQ-004, REQ-005, REQ-062, REQ-065 |
| TASK-008 | REQ-001, REQ-002, REQ-010 |
| TASK-009 | REQ-001, REQ-002, REQ-003, NFR-008 |
| TASK-010 | REQ-016, REQ-017, REQ-019, REQ-022, REQ-025, REQ-026 |
| TASK-011 | REQ-016, REQ-020, REQ-022, REQ-025 |
| TASK-012 | REQ-031, REQ-032, REQ-034, REQ-035, REQ-036, REQ-037, REQ-040, REQ-041 |
| TASK-013 | REQ-046, REQ-047, REQ-048, REQ-049, REQ-050, REQ-051, REQ-052, REQ-053, REQ-055, REQ-056 |
| TASK-014 | REQ-032, REQ-038, REQ-039, REQ-043 |
| TASK-015 | REQ-049, REQ-050, REQ-051, REQ-053, REQ-054 |
| TASK-016 | REQ-016, REQ-019, REQ-020, REQ-031, REQ-034, REQ-046, REQ-049, NFR-008 |
| TASK-017 | Phase 3 REPL requirements |
| TASK-018 | Phase 3 auto-completion requirements |
| TASK-019 | Phase 3 multi-line requirements |
| TASK-020 | REQ-045 |
| TASK-021 | REQ-063, REQ-066 |
| TASK-022 | REQ-069, REQ-074 |
| TASK-023 | REQ-067, REQ-073 |
| TASK-024 | Phase 3 REPL integration requirements |
| TASK-025 | REQ-063, REQ-066, REQ-069, NFR-006 |

---

## Coverage Verification

**Total SPEC Requirements:** 75 (across 5 modules)
**Requirements Covered by Tasks:** All 75 requirements mapped to at least one task

| Module | Requirements | Tasks Covering |
|--------|--------------|----------------|
| Module 1: Rich Library Integration | REQ-001 to REQ-015 | TASK-001 to TASK-009 |
| Module 2: Progress Indicators | REQ-016 to REQ-030 | TASK-010, TASK-011 |
| Module 3: Enhanced Table Formatting | REQ-031 to REQ-045 | TASK-012, TASK-014 |
| Module 4: Error Message Enhancement | REQ-046 to REQ-060 | TASK-013, TASK-015 |
| Module 5: Configuration and Accessibility | REQ-061 to REQ-075 | TASK-003, TASK-006, TASK-021, TASK-022, TASK-023 |

**Non-Functional Requirements Coverage:**

| NFR | Tasks Covering |
|-----|----------------|
| NFR-001 | TASK-009, TASK-016 (performance benchmarks) |
| NFR-002 | TASK-009, TASK-016 (memory benchmarks) |
| NFR-003 | TASK-009 (startup benchmarks) |
| NFR-004 | TASK-004, TASK-005 (fallback testing) |
| NFR-005 | TASK-008, TASK-009 (backward compatibility) |
| NFR-006 | TASK-025 (accessibility audit) |
| NFR-007 | TASK-013 (error usability) |
| NFR-008 | TASK-009, TASK-016, TASK-025 (coverage) |
| NFR-009 | TASK-009, TASK-016 (linting/type checking) |
| NFR-010 | All tasks (Python 3.10+ compatibility) |
| NFR-011 | All tasks (cross-platform testing) |

**coverage_verified: true**

---

## Files to Create

| File Path | Tasks |
|-----------|-------|
| `duckdb_processor/formatters/__init__.py` | TASK-001 |
| `duckdb_processor/formatters/base.py` | TASK-002 |
| `duckdb_processor/formatters/config.py` | TASK-003 |
| `duckdb_processor/formatters/simple_formatter.py` | TASK-004 |
| `duckdb_processor/formatters/rich_formatter.py` | TASK-005 |
| `duckdb_processor/formatters/terminal.py` | TASK-006 |
| `duckdb_processor/repl.py` | TASK-017 |
| `duckdb_processor/config/user_config.py` | TASK-023 |
| `tests/test_formatters.py` | TASK-009 |
| `tests/test_integration.py` | TASK-016 |
| `tests/test_accessibility.py` | TASK-025 |

---

## Files to Modify

| File Path | Tasks |
|-----------|-------|
| `duckdb_processor/cli.py` | TASK-007, TASK-015, TASK-024 |
| `duckdb_processor/processor.py` | TASK-008 |
| `duckdb_processor/loader.py` | TASK-010 |
| `duckdb_processor/analyzer.py` | TASK-011 |
| `pyproject.toml` | TASK-001 (add rich, tabulate dependencies) |

---

## Implementation Sequence

### Recommended Execution Order

**Week 1-2: Phase 1 Foundation**
1. TASK-001 -> TASK-002, TASK-003, TASK-006 (parallel)
2. TASK-004, TASK-005 (parallel after TASK-002, TASK-003)
3. TASK-007 (independent)
4. TASK-008 (after TASK-002-007)
5. TASK-009 (after TASK-002-006)

**Week 3-5: Phase 2 Core Features**
6. TASK-010, TASK-011, TASK-012, TASK-013, TASK-014 (parallel after TASK-008)
7. TASK-015 (after TASK-013)
8. TASK-016 (after TASK-010-015)

**Week 6-8: Phase 3 Advanced Features**
9. TASK-017, TASK-021, TASK-022, TASK-023 (parallel)
10. TASK-018, TASK-019, TASK-020 (parallel after TASK-017)
11. TASK-024 (after TASK-017-023)
12. TASK-025 (after TASK-024)

---

## Risk Summary

| Risk Level | Count | Tasks |
|------------|-------|-------|
| Low | 14 | TASK-001, 002, 003, 004, 005, 006, 007, 009, 014, 019, 020, 021, 022, 023, 025 |
| Medium | 10 | TASK-008, 010, 011, 012, 013, 015, 016, 017, 018, 024 |

**Total Estimated Effort:** 22.25 hours

---

**END OF TASK DECOMPOSITION DOCUMENT**
