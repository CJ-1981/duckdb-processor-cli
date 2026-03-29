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

        # Query 2: Region statistics
        print("\n" + "─" * 60)
        print("Query 2: Region Statistics")
        print("─" * 60)
        result2 = p.sql("""
            SELECT
                region,
                COUNT(*) AS count,
                SUM(CAST(amount AS DOUBLE)) AS total_amount,
                AVG(CAST(amount AS DOUBLE)) AS avg_amount
            FROM data
            GROUP BY region
            ORDER BY total_amount DESC
        """)
        format_result(result2)

        # Query 3: Amount ranking by region
        print("\n" + "─" * 60)
        print("Query 3: Amount Ranking by Region")
        print("─" * 60)
        result3 = p.sql("""
            SELECT
                id,
                region,
                CAST(amount AS DOUBLE) AS amount,
                RANK() OVER (PARTITION BY region ORDER BY CAST(amount AS DOUBLE) DESC) AS rank_in_region
            FROM data
            ORDER BY region, rank_in_region
        """)
        format_result(result3)

        # Query 4: Value classification
        print("\n" + "─" * 60)
        print("Query 4: Value Classification")
        print("─" * 60)
        result4 = p.sql("""
            SELECT
                id,
                CAST(amount AS DOUBLE) AS amount,
                CASE
                    WHEN CAST(amount AS DOUBLE) < 1000 THEN 'Low'
                    WHEN CAST(amount AS DOUBLE) < 10000 THEN 'Medium'
                    ELSE 'High'
                END AS value_tier
            FROM data
            ORDER BY amount DESC
        """)
        format_result(result4)

        # Query 5: High value alert
        print("\n" + "─" * 60)
        print("Query 5: High Value Alert (Amount > 10000)")
        print("─" * 60)
        result5 = p.sql("""
            SELECT id, CAST(amount AS DOUBLE) AS amount, region
            FROM data
            WHERE CAST(amount AS DOUBLE) > 10000
            ORDER BY amount DESC
        """)
        format_result(result5)

        # Query 6: Above average amounts
        print("\n" + "─" * 60)
        print("Query 6: Above Average Amounts")
        print("─" * 60)
        result6 = p.sql("""
            SELECT id, CAST(amount AS DOUBLE) AS amount, region
            FROM data
            WHERE CAST(amount AS DOUBLE) > (SELECT AVG(CAST(amount AS DOUBLE)) FROM data)
            ORDER BY amount DESC
        """)
        format_result(result6)

        print("\n" + "─" * 60)
        print("All queries completed!")
        print("─" * 60)
