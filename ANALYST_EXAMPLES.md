# Business Logic Pattern Examples

This directory contains example analysts that demonstrate common data analysis patterns. You can use these as templates for your own analysis.

## Available Analysts

### Built-in Examples

#### `sample_data_demo.py` - Comprehensive Demo
**Usage:** `python -m duckdb_processor sample_data.csv --run sample_data_demo`

Demonstrates all major Processor API methods:
- Coverage analysis
- Data preview
- Adding derived columns
- Filtering
- Aggregation
- Pivot tables
- Ad-hoc SQL

**Best for:** Learning the full capabilities of the Processor API

---

#### `sample_data_sql_examples.py` - Common SQL Queries
**Usage:** `python -m duckdb_processor sample_data.csv --run sample_data_sql_examples`

Collection of frequently-used SQL queries:
- Basic SELECT and filtering
- GROUP BY aggregations
- JOIN operations
- Window functions
- CASE statements
- Date/time operations

**Best for:** Learning SQL patterns and testing queries

---

### Pattern Libraries

#### `basic_patterns.py` - Fundamental Analysis Patterns
**Usage:** `python -m duckdb_processor data.csv --run basic_patterns`

Essential patterns for beginners:
1. **Filter by Value Threshold** - Find records above/below a value
2. **Single Column Grouping** - Calculate totals by category
3. **Multi-Column Grouping** - Break down by multiple dimensions
4. **Finding Top N** - Get top performers
5. **Percentage Calculation** - Calculate share of total
6. **Multiple Aggregates** - Count, sum, and average together

**Expected columns:** `category`, `amount`, `status` (optional)

**Best for:** Beginners learning basic analysis

---

#### `time_analysis.py` - Time Series Patterns
**Usage:** `python -m duckdb_processor data.csv --run time_analysis`

Temporal analysis patterns:
1. **Daily Trend Analysis** - Variation by day
2. **Monthly Aggregation** - Monthly performance
3. **Day of Week Patterns** - Which days perform best
4. **Moving Average (7-day)** - Smooth out fluctuations
5. **Period-over-Period Growth** - Month-over-month comparison
6. **Hourly Distribution** - Activity by hour (if timestamp)

**Expected columns:** `date`, `timestamp`, `created_at`, or similar

**Best for:** Analyzing trends over time

---

#### `data_quality.py` - Data Validation
**Usage:** `python -m duckdb_processor data.csv --run data_quality`

Data quality checks:
1. **Overall Statistics** - Row and column counts
2. **Missing Value Analysis** - Column fill rates
3. **Duplicate Detection** - Find duplicate records
4. **Data Type Consistency** - Validate types
5. **Outlier Detection** - IQR method for extreme values
6. **Value Distribution** - Most common values
7. **Data Freshness** - How current is the data?

**Expected columns:** Any (analyzes all available data)

**Best for:** Understanding data quality before analysis

---

#### `business_metrics.py` - KPI Calculations
**Usage:** `python -m duckdb_processor data.csv --run business_metrics`

Common business metrics:
1. **Pareto Analysis (80/20)** - Do top 20% contribute 80%?
2. **Percentile Rankings** - Where does each entity rank?
3. **Period-over-Period Growth** - Growth rates
4. **Concentration Ratio** - Top 3/10/25 entities
5. **Mean vs Median** - Detect skewness

**Expected columns:** `entity` (customer, product, etc.), `amount`

**Best for:** Business reporting and KPIs

---

#### `python_patterns.py` - Python-Native Analysis
**Usage:** `python -m duckdb_processor data.csv --run python_patterns`

Pure Python patterns (no SQL required):
1. **Load to Pandas** - Convert to DataFrame
2. **Filter with Python** - Boolean indexing
3. **Group and Aggregate** - pandas groupby
4. **Custom Business Logic** - Apply functions
5. **Data Transformation** - String operations
6. **Merge/Join Operations** - Combine DataFrames
7. **Sorting and Ranking** - pandas methods
8. **Pivot Tables** - Multi-dimensional analysis
9. **Missing Data Handling** - isna, fillna, dropna
10. **Export Back to SQL** - Create views

**Expected columns:** Any (works with any data)

**Best for:** Users who prefer Python over SQL

---

## How to Use These Examples

### 1. Run an Existing Analyst

```bash
python -m duckdb_processor your_data.csv --run basic_patterns
```

### 2. View Available Analysts

```bash
python -m duckdb_processor --list-analyzers
```

### 3. Create Your Own Analyst

Copy one of the examples as a template:

```bash
# 1. Copy an example
cp duckdb_processor/analysts/basic_patterns.py \
   duckdb_processor/analysts/my_analysis.py

# 2. Edit the file
# - Change the class name
# - Update the 'name' and 'description'
# - Modify the run() method with your logic

# 3. Run your analyst
python -m duckdb_processor data.csv --run my_analysis
```

### 4. Analyst Template

```python
"""My custom analysis."""
from duckdb_processor.analyzer import BaseAnalyzer, register

@register
class MyAnalysis(BaseAnalyzer):
    """Description of what it does."""

    name = "my_analysis"
    description = "One-line summary"

    def run(self, p):
        """Execute analysis."""
        # Use Processor API methods:
        # - p.filter(where_clause)
        # - p.aggregate(group_by, agg_field, func)
        # - p.pivot(row_key, col_key, val)
        # - p.sql(query)
        # - p.preview(n)
        # - p.coverage()
        # - p.schema()

        # Example:
        result = p.aggregate("category", "amount", "SUM")

        # Display results:
        if p.formatter:
            p.formatter.format_dataframe(result)
        else:
            print(result.to_string(index=False))
```

## Choosing the Right Example

| Your Goal | Use This Example |
|-----------|------------------|
| Learning the basics | `demo.py` |
| First-time analysis | `basic_patterns.py` |
| Working with time data | `time_analysis.py` |
| Checking data quality | `data_quality.py` |
| Business KPIs | `business_metrics.py` |
| Prefer Python over SQL | `python_patterns.py` |
| Need SQL examples | `sql_examples.py` |

## Tips for Success

1. **Start Simple** - Begin with `basic_patterns.py` to learn fundamentals
2. **Check Your Data** - Run `data_quality.py` before analysis
3. **Use Python Patterns** - If you're not comfortable with SQL
4. **Copy and Modify** - Use examples as starting points
5. **Test Incrementally** - Run your analyst frequently while developing
6. **Read the Code** - Example code includes detailed comments
7. **Use `--list-analyzers`** - See all available analysts

## Advanced Topics

### Chaining Multiple Analysts

```bash
python -m duckdb_processor data.csv --run data_quality,basic_patterns
```

### Saving Results

```bash
# Save info banner to file
python -m duckdb_processor data.csv --run basic_patterns -o results.txt

# Export query results
python -m duckdb_processor data.csv --run basic_patterns --export-format json
```

### Interactive Analysis

```bash
# Run analyst then enter REPL
python -m duckdb_processor data.csv --run basic_patterns --interactive
```

## Getting Help

- **List analysts:** `python -m duckdb_processor --list-analyzers`
- **REPL help:** Type `\help` in interactive mode
- **Documentation:** See README.md for detailed usage
- **Examples:** Each analyst file contains inline documentation
