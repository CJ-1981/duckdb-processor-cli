"""Basic filtering and aggregation patterns.

This analyst demonstrates fundamental data analysis patterns:
- Filtering data by conditions
- Grouping and aggregating
- Sorting results
- Calculating percentages and ratios

Run via::
    python -m duckdb_processor data.csv --run basic_patterns

Expected columns:
    - category (categorical): Product category, region, department, etc.
    - amount (numeric): Sales amount, quantity, revenue, etc.
    - status (categorical, optional): Order status, completion flag, etc.
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class BasicPatterns(BaseAnalyzer):
    """Essential data analysis patterns for beginners."""

    name = "basic_patterns"
    description = "Basic filtering, grouping, and aggregation examples"

    def run(self, p):
        """Execute basic analysis patterns using Processor API."""
        # Helper function for consistent output formatting
        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Auto-detect columns
        amount_col = None
        for col in p.columns:
            if col.lower() in ["amount", "price", "quantity", "value", "total", "cost", "revenue"]:
                amount_col = col
                break

        category_col = None
        for col in p.columns:
            if col.lower() in ["category", "region", "status", "department", "type", "group"]:
                category_col = col
                break

        if not amount_col:
            print("Error: No numeric column found.")
            print(f"Expected: amount, price, quantity, value, total, cost, or revenue")
            print(f"Available columns: {', '.join(p.columns)}")
            return

        print(f"\n### Using columns:")
        print(f"  Numeric: '{amount_col}'")
        if category_col:
            print(f"  Categorical: '{category_col}'")
        else:
            print(f"  Categorical: (using first column)")
            category_col = p.columns[0]

        # ── Pattern 1: Simple Filter ─────────────────────────────
        # Business Question: What are our high-value transactions?
        print("\n### Pattern 1: Filter by Value Threshold")
        print("Business Question: Show all records above a threshold")
        print(f"Python API:")
        print(f'    p.filter("CAST({amount_col} AS DOUBLE) >= 1000")')

        # Get median to use as threshold
        median_val = p.sql(f"SELECT MEDIAN(TRY_CAST(\"{amount_col}\" AS DOUBLE)) as med FROM data").iloc[0]['med']
        threshold = int(median_val) if median_val else 1000

        high_value = p.filter(f'CAST("{amount_col}" AS DOUBLE) >= {threshold}')
        show(f"High-Value Records (>= {threshold})", high_value)

        # ── Pattern 2: Single Column Grouping ────────────────────
        # Business Question: What's the total by category?
        print("\n### Pattern 2: Group and Sum by Category")
        print("Business Question: Calculate totals per category")
        print(f"Python API:")
        print(f'    p.aggregate("{category_col}", "{amount_col}", "SUM")')
        by_category = p.aggregate(category_col, amount_col, "SUM")
        show(f"Total by {category_col}", by_category)

        # ── Pattern 3: Multi-Column Grouping ────────────────────
        # Business Question: Break down by multiple dimensions?
        if len(p.columns) >= 3:
            second_cat = None
            for col in p.columns:
                if col != category_col and col != amount_col:
                    second_cat = col
                    break

            if second_cat:
                print("\n### Pattern 3: Multi-Column Grouping")
                print(f"Business Question: Analyze by {category_col} and {second_cat}")
                print(f"Python API:")
                print(f'    p.aggregate(["{category_col}", "{second_cat}"], "{amount_col}", "SUM")')
                by_category_status = p.aggregate([category_col, second_cat], amount_col, "SUM")
                show(f"Breakdown by {category_col} and {second_cat}", by_category_status)

        # ── Pattern 4: Finding Top N ────────────────────────────
        # Business Question: Who are our top performers?
        print("\n### Pattern 4: Top N Performers")
        print("Business Question: Find top 5 categories by total")
        print("SQL (for LIMIT):")
        print(f"    p.sql(\"SELECT * FROM (")
        print(f"      SELECT {category_col}, SUM({amount_col}) as total")
        print(f"      FROM data GROUP BY {category_col}")
        print(f"    ) ORDER BY total DESC LIMIT 5\")")
        top_5 = p.sql(f"""
            SELECT {category_col}, SUM(CAST("{amount_col}" AS DOUBLE)) as total
            FROM data
            GROUP BY {category_col}
            ORDER BY total DESC
            LIMIT 5
        """)
        show(f"Top 5 {category_col}s by Total", top_5)

        # ── Pattern 5: Percentage Calculation ───────────────────
        # Business Question: What's each category's share of total?
        print("\n### Pattern 5: Percentage Share Calculation")
        print("Business Question: Calculate each category's % of total")
        print("SQL (using window functions):")
        print(f"    p.sql(\"SELECT")
        print(f"      {category_col},")
        print(f"      SUM({amount_col}) as total,")
        print(f"      ROUND(100 * SUM({amount_col}) / SUM(SUM({amount_col})) OVER(), 2) as pct")
        print(f"    FROM data GROUP BY {category_col}\")")
        with_share = p.sql(f"""
            SELECT
                {category_col},
                SUM(CAST("{amount_col}" AS DOUBLE)) as total,
                ROUND(100 * SUM(CAST("{amount_col}" AS DOUBLE)) /
                    SUM(SUM(CAST("{amount_col}" AS DOUBLE))) OVER(), 2) as percentage
            FROM data
            GROUP BY {category_col}
            ORDER BY total DESC
        """)
        show(f"{category_col} Share of Total (%)", with_share)

        # ── Pattern 6: Count and Average Together ────────────────
        # Business Question: What's the volume and average per category?
        print("\n### Pattern 6: Multiple Aggregates at Once")
        print("Business Question: Count, sum, and average by category")
        print("SQL:")
        print(f"    p.sql(\"SELECT")
        print(f"      {category_col},")
        print(f"      COUNT(*) as count,")
        print(f"      SUM({amount_col}) as total,")
        print(f"      AVG({amount_col}) as average")
        print(f"    FROM data GROUP BY {category_col}\")")
        multi_agg = p.sql(f"""
            SELECT
                {category_col},
                COUNT(*) as record_count,
                SUM(CAST("{amount_col}" AS DOUBLE)) as total,
                ROUND(AVG(CAST("{amount_col}" AS DOUBLE)), 2) as average
            FROM data
            GROUP BY {category_col}
            ORDER BY total DESC
        """)
        show(f"Count, Sum, and Average by {category_col}", multi_agg)

        print("\n### Tips for Beginners")
        print(f"1. Detected columns: numeric='{amount_col}', categorical='{category_col}'")
        print("2. Use p.aggregate() for simple group-by operations")
        print("3. Use p.filter() for WHERE clauses")
        print("4. Use p.sql() for complex queries with multiple aggregations")
        print("5. Always CAST() string columns to numbers for math")
        print("6. Use ROUND() to control decimal places")
        print(f"7. Adjust column names in examples above to match your data")
