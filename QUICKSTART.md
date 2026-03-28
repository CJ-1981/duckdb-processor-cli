# Quick Test Examples for DuckDB CSV Processor

## Understanding the CLI

The `--run` flag executes **named analyzers** (plugins), not SQL queries directly.
For SQL queries, use **interactive mode** or Python directly.

---

## Method 1: Interactive SQL REPL (Recommended for Testing)

```bash
# Start interactive mode
python3 -m duckdb_processor test_data.csv
```

Then type SQL queries at the prompt:
```
sql> SELECT * FROM data LIMIT 5;
sql> SELECT category, COUNT(*) FROM data GROUP BY category;
sql> exit
```

---

## Method 2: Python Direct (Best for Scripts)

```bash
# Run SQL directly via Python
python3 -c "
import duckdb_processor
p = duckdb_processor.load('test_data.csv')
print(p.sql('SELECT category, COUNT(*) AS count FROM data GROUP BY category').to_string(index=False))
"
```

---

## Method 3: Using Built-in Demo Analyzer

```bash
# Run the built-in demo analyzer
python3 -m duckdb_processor test_data.csv --run demo
```

---

## Method 4: Custom Analyzer Script

Create `my_analysis.py`:
```python
from duckdb_processor import Processor

def analyze(p: Processor) -> None:
    """My custom analysis."""
    # Query 1: Category statistics
    result1 = p.sql('''
        SELECT
            category,
            COUNT(*) AS count,
            SUM(price * quantity) AS total_value
        FROM data
        GROUP BY category
        ORDER BY total_value DESC
    ''')
    print("\n=== Category Statistics ===")
    print(result1.to_string(index=False))

    # Query 2: Price ranking
    result2 = p.sql('''
        SELECT
            name,
            category,
            price,
            RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rank
        FROM data
        ORDER BY category, rank
    ''')
    print("\n=== Price Ranking by Category ===")
    print(result2.to_string(index=False))
```

Then run it:
```bash
python3 -m duckdb_processor test_data.csv --run my_analysis
```

---

## Example SQL Queries for Interactive Mode

### 1. Preview data
```sql
SELECT * FROM data LIMIT 5;
```

### 2. Filter by category
```sql
SELECT * FROM data WHERE category = 'Electronics';
```

### 3. Calculate totals
```sql
SELECT name, price, quantity, (price * quantity) AS total FROM data;
```

### 4. Group and aggregate
```sql
SELECT category, COUNT(*) AS count, AVG(price) AS avg_price
FROM data
GROUP BY category
ORDER BY avg_price DESC;
```

### 5. Find expensive items
```sql
SELECT name, price
FROM data
WHERE price > (SELECT AVG(price) FROM data)
ORDER BY price DESC;
```

### 6. Price tier classification
```sql
SELECT
    name,
    price,
    CASE
        WHEN price < 10 THEN 'Budget'
        WHEN price < 50 THEN 'Standard'
        ELSE 'Premium'
    END AS tier
FROM data
ORDER BY price DESC;
```

### 7. Low stock alert
```sql
SELECT name, quantity
FROM data
WHERE quantity < 100
ORDER BY quantity ASC;
```

### 8. Date filtering
```sql
SELECT name, created_at
FROM data
WHERE created_at >= '2024-01-20'
ORDER BY created_at;
```

### 9. Window function - ranking
```sql
SELECT
    name,
    category,
    price,
    RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rank_in_category
FROM data
ORDER BY category, rank_in_category;
```

### 10. Complex aggregation
```sql
SELECT
    category,
    COUNT(*) AS product_count,
    SUM(quantity) AS total_quantity,
    SUM(price * quantity) AS total_value,
    AVG(price) AS avg_price
FROM data
GROUP BY category
HAVING COUNT(*) >= 3
ORDER BY total_value DESC;
```

---

## Formatter Options

### Use Rich formatter (default)
```bash
python3 -m duckdb_processor test_data.csv
```

### Use Simple formatter (legacy)
```bash
python3 -m duckdb_processor test_data.csv --format simple
```

### Disable colors
```bash
python3 -m duckdb_processor test_data.csv --no-color
```

### Disable progress indicators
```bash
python3 -m duckdb_processor test_data.csv --no-progress
```

---

## Testing Checklist

- [ ] Start interactive REPL: `python3 -m duckdb_processor test_data.csv`
- [ ] Run `help` command in REPL
- [ ] Run `list` command to see available analysts
- [ ] Execute SQL queries in interactive mode
- [ ] Try `--format simple` for legacy output
- [ ] Try `--no-color` for non-color terminals
- [ ] Create and run custom analyzer script

---

## Files Created

- `test_data.csv` - Sample dataset (10 products)
- `my_analysis.py` - Custom analyzer template
- `QUICKSTART.md` - This guide
