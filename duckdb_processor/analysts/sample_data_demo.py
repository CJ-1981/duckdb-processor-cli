"""Built-in demo analysis for sample_data.csv.

Run via::

    python -m duckdb_processor sample_data.csv --run sample_data_demo

This analyzer demonstrates the key methods available on the
:class:`~duckdb_processor.processor.Processor` class:
coverage, preview, add_column, filter, aggregate, pivot, and
ad-hoc SQL.  Data analysts can copy this file as a starting
point for their own analyses.
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class SampleDataDemo(BaseAnalyzer):
    """Example analysis for sample_data.csv demonstrating Processor capabilities."""

    name = "sample_data_demo"
    description = "Demo for sample_data.csv: coverage, filtering, aggregation, and pivot"

    def run(self, p):
        """Execute the full demo sequence using configured formatter.

        NOTE: This analyst is designed for sample_data.csv but includes
        auto-detection to work with similar datasets. It looks for common
        column names: amount, price, value, total, revenue.
        """
        # Auto-detect the numeric column to use
        amount_col = None
        for col in p.columns:
            if col.lower() in ["amount", "price", "value", "total", "revenue", "cost"]:
                amount_col = col
                break

        if not amount_col:
            # Fall back to first numeric-looking column
            for col in p.columns:
                if col.lower() not in ["id", "name", "description", "category", "region", "date"]:
                    amount_col = col
                    break

        if not amount_col:
            print("Error: Could not find a suitable numeric column for analysis.")
            print("Expected: amount, price, value, total, revenue, or cost")
            print(f"Available columns: {', '.join(p.columns)}")
            return

        # Helper function to format with fallback
        def format_result(df):
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        print("\n── Key coverage ──────────────────────────────────")
        format_result(p.coverage())

        print("\n── Preview ───────────────────────────────────────")
        format_result(p.preview(5))

        print(f"\n── Add derived column: tier (based on '{amount_col}') ──────────────────────")
        p.add_column(
            "tier",
            f"""
            CASE
                WHEN TRY_CAST("{amount_col}" AS DOUBLE) >= 10000 THEN 'PLATINUM'
                WHEN TRY_CAST("{amount_col}" AS DOUBLE) >= 5000  THEN 'GOLD'
                WHEN TRY_CAST("{amount_col}" AS DOUBLE) >= 1000  THEN 'SILVER'
                ELSE 'BRONZE'
            END
            """,
        )

        # Auto-detect status column if it exists
        status_col = None
        for col in p.columns:
            if col.lower() in ["status", "state", "active"]:
                status_col = col
                break

        # Auto-detect region column if it exists
        region_col = None
        for col in p.columns:
            if col.lower() in ["region", "area", "location", "category"]:
                region_col = col
                break

        if status_col:
            print(f"\n── Filter: active + {amount_col} >= 500 ────────────────")
            filtered = p.filter(
                f'"{status_col}" = \'active\' AND TRY_CAST("{amount_col}" AS DOUBLE) >= 500'
            )
            format_result(filtered)
        else:
            print(f"\n── Filter: {amount_col} >= 500 ────────────────")
            filtered = p.filter(
                f'TRY_CAST("{amount_col}" AS DOUBLE) >= 500'
            )
            format_result(filtered)

        if region_col:
            print(f"\n── Aggregate: SUM({amount_col}) by {region_col} ──────────────")
            format_result(p.aggregate(region_col, amount_col, "SUM"))

        print(f"\n── Aggregate: AVG({amount_col}) by tier ────────────────")
        format_result(p.aggregate("tier", amount_col, "AVG"))

        if region_col:
            print(f"\n── Pivot: {region_col} x tier -> SUM({amount_col}) ────────────")
            format_result(p.pivot(region_col, "tier", amount_col))

        print("\n── Ad-hoc SQL ────────────────────────────────────")
        if region_col and status_col:
            result = p.sql(f"""
                SELECT
                    "{region_col}",
                    tier,
                    COUNT(*)                                   AS n,
                    ROUND(SUM(TRY_CAST("{amount_col}" AS DOUBLE)), 2)  AS total_amount,
                    ROUND(AVG(TRY_CAST("{amount_col}" AS DOUBLE)), 2)  AS avg_amount
                FROM data
                WHERE "{status_col}" = 'active'
                GROUP BY "{region_col}", tier
                ORDER BY total_amount DESC
            """)
        else:
            result = p.sql(f"""
                SELECT
                    tier,
                    COUNT(*)                                   AS n,
                    ROUND(SUM(TRY_CAST("{amount_col}" AS DOUBLE)), 2)  AS total_amount,
                    ROUND(AVG(TRY_CAST("{amount_col}" AS DOUBLE)), 2)  AS avg_amount
                FROM data
                GROUP BY tier
                ORDER BY total_amount DESC
            """)
        format_result(result)
