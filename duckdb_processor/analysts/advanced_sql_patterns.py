"""Advanced SQL analytical patterns (CTEs, Window Functions, etc.).

This analyst demonstrates how to perform complex data analysis using 
advanced SQL features available in DuckDB:
- Window functions for running totals and moving averages
- Common Table Expressions (CTEs) for multi-step logic
- Advanced aggregations
- Ranking and pagination

Run via::
    python -m duckdb_processor data.csv --run advanced_sql_patterns
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class AdvancedSqlPatterns(BaseAnalyzer):
    """Advanced SQL analysis including Window Functions and CTEs."""

    name = "advanced_sql_patterns"
    description = "Advanced SQL: Window Functions, CTEs, Running Totals"

    def run(self, p):
        """Execute advanced SQL analysis patterns."""
        print("\n### Advanced SQL Analysis Patterns")
        print("=" * 58)
        print("This module demonstrates powerful SQL features like")
        print("Window Functions, CTEs (WITH clauses), and complex aggregations.")
        print("=" * 58)

        # Helper for printing
        def show(title, query):
            print(f"\n── {title} ─────────────────────────────────────")
            print(f"SQL Concept Highlight:")
            for line in query.split('\n'):
                if 'OVER' in line.upper() or 'WITH' in line.upper():
                    print(f"  > {line.strip()}")
            try:
                result = p.sql(query)
                if p.formatter:
                    p.formatter.format_dataframe(result.head(10))
                else:
                    print(result.head(10).to_string(index=False))
            except Exception as e:
                print(f"Note: Query could not be executed on this dataset.")
                print(f"Reason: {e}")
                print(f"SQL Query:\n{query}\n")

        # ── Pattern 1: Running Totals (Cumulative Sum) ───────────────────────
        q1 = """
        SELECT 
            *,
            -- The SUM() function with an OVER() clause creates a running total
            SUM(amount) OVER (
                ORDER BY timestamp 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as running_total
        FROM data
        WHERE amount IS NOT NULL AND timestamp IS NOT NULL
        ORDER BY timestamp
        """
        show("Pattern 1: Running Totals using Window Functions", q1)

        # ── Pattern 2: Moving Averages (Rolling Mean) ────────────────────────
        q2 = """
        SELECT 
            *,
            -- A 3-row moving average (current row and 2 previous)
            AVG(amount) OVER (
                ORDER BY timestamp 
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) as moving_avg_3_period
        FROM data
        WHERE amount IS NOT NULL AND timestamp IS NOT NULL
        ORDER BY timestamp
        """
        show("Pattern 2: Moving Averages using Window Bounds", q2)

        # ── Pattern 3: Ranking Functions ─────────────────────────────────────
        q3 = """
        SELECT 
            *,
            -- ROW_NUMBER: Unique sequential integer
            ROW_NUMBER() OVER(ORDER BY amount DESC) as row_num,
            -- RANK: Allows ties (1, 2, 2, 4)
            RANK() OVER(ORDER BY amount DESC) as rank_amount,
            -- DENSE_RANK: Allows ties but no gaps (1, 2, 2, 3)
            DENSE_RANK() OVER(ORDER BY amount DESC) as dense_rank_amount
        FROM data
        WHERE amount IS NOT NULL
        ORDER BY amount DESC
        """
        show("Pattern 3: Ranking (ROW_NUMBER vs RANK vs DENSE_RANK)", q3)

        # ── Pattern 4: Comparing Row-over-Row (LEAD / LAG) ───────────────────
        q4 = """
        SELECT 
            *,
            -- LAG: Get the amount from the previous row
            LAG(amount) OVER(ORDER BY timestamp) as prev_amount,
            -- Calculate the difference between current and previous
            amount - LAG(amount) OVER(ORDER BY timestamp) as growth
        FROM data
        WHERE amount IS NOT NULL AND timestamp IS NOT NULL
        ORDER BY timestamp
        """
        show("Pattern 4: Row-over-Row Differences (LAG)", q4)

        # ── Pattern 5: CTEs (Common Table Expressions) ───────────────────────
        q5 = """
        -- WITH clause defines a temporary result set
        WITH CategoryStats AS (
            SELECT 
                status, 
                AVG(amount) as avg_amount
            FROM data
            WHERE status IS NOT NULL AND amount IS NOT NULL
            GROUP BY status
        )
        -- The main query joins back to the CTE
        SELECT 
            d.*,
            c.avg_amount as category_avg,
            -- Compare individual row to category average
            d.amount - c.avg_amount as diff_from_avg
        FROM data d
        JOIN CategoryStats c ON d.status = c.status
        ORDER BY diff_from_avg DESC
        """
        show("Pattern 5: Multi-Step Logic using CTEs (WITH)", q5)

        print("\n### Tips for Advanced SQL Users")
        print("1. Window Functions (`OVER`) do not reduce rows like `GROUP BY` does.")
        print("2. `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is the default for Running Totals.")
        print("3. CTEs make your SQL much more readable than nested subqueries.")
