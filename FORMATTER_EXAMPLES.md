# Formatter Integration Examples

## Overview

All built-in analysts now use the configured formatter for output. This means:
- **Rich formatter** (`--format rich`): Beautiful tables with borders and colors
- **Simple formatter** (`--format simple`): Plain text output for backward compatibility

## Before vs After

### Before (Plain Text Output)
```
id     name    category price quantity created_at  _row
 1 Widget A Electronics 29.99      100 2024-01-15     2
 2 Gadget B Electronics 49.99       50 2024-01-16     3
 3   Tool C    Hardware 15.99      200 2024-01-17     4
```

### After (Rich Formatter - Default)
```
┏━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━┓
┃ id ┃ name     ┃ category    ┃ price ┃ quantity ┃ created_at ┃ _row ┃
┡━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━┩
│ 1  │ Widget A │ Electronics │ 29.99 │ 100      │ 2024-01-15 │    2 │
│ 2  │ Gadget B │ Electronics │ 49.99 │ 50       │ 2024-01-16 │    3 │
│ 3  │ Tool C   │ Hardware    │ 15.99 │ 200      │ 2024-01-17 │    4 │
└────┴──────────┴─────────────┴───────┴──────────┴────────────┴──────┘
```

## Updated Analysts

### 1. `sql_examples` Analyst

**What Changed:**
- All query results now use `p.formatter.format_dataframe()` instead of `.to_string()`
- Includes fallback for cases where formatter is None

**Example Usage:**
```bash
# Rich formatter (default)
python3 -m duckdb_processor test_data.csv --run sql_examples

# Simple formatter
python3 -m duckdb_processor test_data.csv --run sql_examples --format simple
```

### 2. `demo` Analyst

**What Changed:**
- All Processor method results now use formatter
- Maintains same functionality but with better output

**Example Usage:**
```bash
python3 -m duckdb_processor your_data.csv --run demo
```

## Code Pattern for Analysts

When creating custom analysts, use this pattern:

```python
from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class MyAnalysis(BaseAnalyzer):
    """My custom analysis."""

    name = "my_analysis"
    description = "Description of what it does"

    def run(self, p):
        """Execute analysis using configured formatter."""

        # Helper function for consistent formatting
        def format_result(df):
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Run queries and format results
        result = p.sql("SELECT * FROM data LIMIT 10")
        format_result(result)
```

## Benefits

1. **Consistent Output**: All analysts use the same formatter
2. **User Choice**: Users can select output format via `--format` flag
3. **Backward Compatible**: Simple formatter maintains legacy output
4. **Type Safe**: Fallback handles cases where formatter is None
5. **Beautiful Tables**: Rich formatter provides professional output

## Testing

Test the different formatters:

```bash
# Rich formatter with colors
python3 -m duckdb_processor test_data.csv --run sql_examples

# Rich formatter without colors
python3 -m duckdb_processor test_data.csv --run sql_examples --no-color

# Simple formatter (legacy)
python3 -m duckdb_processor test_data.csv --run sql_examples --format simple
```

## Type Safety

The code includes proper type checking and fallbacks:
- `p.formatter` can be `None` (when Processor is used without CLI)
- Helper function `format_result()` safely handles both cases
- Type checker will not complain about the Optional attribute
