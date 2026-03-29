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
        """Execute the full demo sequence using configured formatter."""
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

        print("\n── Add derived column: tier ──────────────────────")
        p.add_column(
            "tier",
            """
            CASE
                WHEN TRY_CAST(amount AS DOUBLE) >= 10000 THEN 'PLATINUM'
                WHEN TRY_CAST(amount AS DOUBLE) >= 5000  THEN 'GOLD'
                WHEN TRY_CAST(amount AS DOUBLE) >= 1000  THEN 'SILVER'
                ELSE 'BRONZE'
            END
            """,
        )

        print("\n── Filter: active + amount >= 500 ────────────────")
        filtered = p.filter(
            "status = 'active' AND TRY_CAST(amount AS DOUBLE) >= 500"
        )
        format_result(filtered)

        print("\n── Aggregate: SUM(amount) by region ──────────────")
        format_result(p.aggregate("region", "amount", "SUM"))

        print("\n── Aggregate: AVG(amount) by tier ────────────────")
        format_result(p.aggregate("tier", "amount", "AVG"))

        print("\n── Pivot: region x tier -> SUM(amount) ────────────")
        format_result(p.pivot("region", "tier", "amount"))

        print("\n── Ad-hoc SQL ────────────────────────────────────")
        result = p.sql("""
            SELECT
                region,
                tier,
                COUNT(*)                                   AS n,
                ROUND(SUM(TRY_CAST(amount AS DOUBLE)), 2)  AS total_amount,
                ROUND(AVG(TRY_CAST(amount AS DOUBLE)), 2)  AS avg_amount
            FROM data
            WHERE status = 'active'
            GROUP BY region, tier
            ORDER BY total_amount DESC
        """)
        format_result(result)
