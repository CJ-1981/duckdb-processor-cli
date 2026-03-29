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
        """Execute example queries using configured formatter.

        NOTE: This analyst demonstrates SQL patterns for sample_data.csv but
        includes auto-detection to work with similar datasets. It looks for
        common column names and adapts queries accordingly.
        """
        # Auto-detect columns
        amount_col = None
        for col in p.columns:
            if col.lower() in ["amount", "price", "value", "total", "revenue", "cost"]:
                amount_col = col
                break

        region_col = None
        for col in p.columns:
            if col.lower() in ["region", "area", "location", "category"]:
                region_col = col
                break

        # Fall back to first numeric column if amount not found
        if not amount_col:
            for col in p.columns:
                if col.lower() not in ["id", "name", "description", "date", "created_at"]:
                    amount_col = col
                    break

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
        if region_col and amount_col:
            print("\n" + "─" * 60)
            print(f"Query 2: {region_col.title()} Statistics")
            print("─" * 60)
            result2 = p.sql(f"""
                SELECT
                    "{region_col}",
                    COUNT(*) AS count,
                    SUM(CAST("{amount_col}" AS DOUBLE)) AS total_amount,
                    AVG(CAST("{amount_col}" AS DOUBLE)) AS avg_amount
                FROM data
                GROUP BY "{region_col}"
                ORDER BY total_amount DESC
            """)
            format_result(result2)

        # Query 3: Amount ranking by region
        if region_col and amount_col:
            print("\n" + "─" * 60)
            print(f"Query 3: {amount_col.title()} Ranking by {region_col.title()}")
            print("─" * 60)
            result3 = p.sql(f"""
                SELECT
                    id,
                    "{region_col}",
                    CAST("{amount_col}" AS DOUBLE) AS {amount_col},
                    RANK() OVER (PARTITION BY "{region_col}" ORDER BY CAST("{amount_col}" AS DOUBLE) DESC) AS rank_in_{region_col}
                FROM data
                ORDER BY "{region_col}", rank_in_{region_col}
            """)
            format_result(result3)

        # Query 4: Value classification
        if amount_col:
            print("\n" + "─" * 60)
            print("Query 4: Value Classification")
            print("─" * 60)
            result4 = p.sql(f"""
                SELECT
                    id,
                    CAST("{amount_col}" AS DOUBLE) AS {amount_col},
                    CASE
                        WHEN CAST("{amount_col}" AS DOUBLE) < 1000 THEN 'Low'
                        WHEN CAST("{amount_col}" AS DOUBLE) < 10000 THEN 'Medium'
                        ELSE 'High'
                    END AS value_tier
                FROM data
                ORDER BY {amount_col} DESC
            """)
            format_result(result4)

        # Query 5: High value alert
        if amount_col:
            print("\n" + "─" * 60)
            print(f"Query 5: High Value Alert ({amount_col.title()} > 10000)")
            print("─" * 60)
            result5 = p.sql(f"""
                SELECT id, CAST("{amount_col}" AS DOUBLE) AS {amount_col}{f', "{region_col}"' if region_col else ''}
                FROM data
                WHERE CAST("{amount_col}" AS DOUBLE) > 10000
                ORDER BY {amount_col} DESC
            """)
            format_result(result5)

        # Query 6: Above average amounts
        if amount_col:
            print("\n" + "─" * 60)
            print(f"Query 6: Above Average {amount_col.title()}s")
            print("─" * 60)
            result6 = p.sql(f"""
                SELECT id, CAST("{amount_col}" AS DOUBLE) AS {amount_col}{f', "{region_col}"' if region_col else ''}
                FROM data
                WHERE CAST("{amount_col}" AS DOUBLE) > (SELECT AVG(CAST("{amount_col}" AS DOUBLE)) FROM data)
                ORDER BY {amount_col} DESC
            """)
            format_result(result6)

        print("\n" + "─" * 60)
        print("All queries completed!")
        print("─" * 60)
