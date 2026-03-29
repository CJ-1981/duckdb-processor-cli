"""SQL example queries for sample_data.csv.

This analyst module provides several example queries that demonstrate
DuckDB SQL capabilities with the CSV processor.

Run via::

    python -m duckdb_processor sample_data.csv --run sample_data_sql_examples
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class SampleDataSQLExamples(BaseAnalyzer):
    """Run example SQL queries for sample_data.csv."""

    name = "sample_data_sql_examples"
    description = "Example SQL queries for sample_data.csv"

    def run(self, p) -> None:
        """Execute example queries using configured formatter."""
        # Helper function to format with fallback
        def format_result(df):
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Query 1: Basic preview
        print("\n" + "─" * 60)
        print("Query 1: Basic Preview (First 5 Rows)")
        print("─" * 60)
        result1 = p.sql("SELECT * FROM data LIMIT 5")
        format_result(result1)

        # Query 2: Category statistics
        print("\n" + "─" * 60)
        print("Query 2: Category Statistics")
        print("─" * 60)
        result2 = p.sql("""
            SELECT
                category,
                COUNT(*) AS product_count,
                SUM(CAST(price AS DOUBLE) * CAST(quantity AS INTEGER)) AS total_value,
                AVG(CAST(price AS DOUBLE)) AS avg_price
            FROM data
            GROUP BY category
            ORDER BY total_value DESC
        """)
        format_result(result2)

        # Query 3: Price ranking by category
        print("\n" + "─" * 60)
        print("Query 3: Price Ranking by Category")
        print("─" * 60)
        result3 = p.sql("""
            SELECT
                name,
                category,
                CAST(price AS DOUBLE) AS price,
                RANK() OVER (PARTITION BY category ORDER BY CAST(price AS DOUBLE) DESC) AS rank_in_category
            FROM data
            ORDER BY category, rank_in_category
        """)
        format_result(result3)

        # Query 4: Price tier classification
        print("\n" + "─" * 60)
        print("Query 4: Price Tier Classification")
        print("─" * 60)
        result4 = p.sql("""
            SELECT
                name,
                CAST(price AS DOUBLE) AS price,
                CASE
                    WHEN CAST(price AS DOUBLE) < 10 THEN 'Budget'
                    WHEN CAST(price AS DOUBLE) < 50 THEN 'Standard'
                    ELSE 'Premium'
                END AS price_tier
            FROM data
            ORDER BY price DESC
        """)
        format_result(result4)

        # Query 5: Low stock alert
        print("\n" + "─" * 60)
        print("Query 5: Low Stock Alert (Quantity < 100)")
        print("─" * 60)
        result5 = p.sql("""
            SELECT name, CAST(quantity AS INTEGER) AS quantity, category
            FROM data
            WHERE CAST(quantity AS INTEGER) < 100
            ORDER BY quantity ASC
        """)
        format_result(result5)

        # Query 6: Above average pricing
        print("\n" + "─" * 60)
        print("Query 6: Products Above Average Price")
        print("─" * 60)
        result6 = p.sql("""
            SELECT name, CAST(price AS DOUBLE) AS price, category
            FROM data
            WHERE CAST(price AS DOUBLE) > (SELECT AVG(CAST(price AS DOUBLE)) FROM data)
            ORDER BY price DESC
        """)
        format_result(result6)

        print("\n" + "─" * 60)
        print("All queries completed!")
        print("─" * 60)
