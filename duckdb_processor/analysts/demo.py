"""Built-in demo analysis showcasing Processor capabilities.

Run via::

    python -m duckdb_processor data.csv --run demo

This analyzer demonstrates the key methods available on the
:class:`~duckdb_processor.processor.Processor` class:
coverage, preview, add_column, filter, aggregate, pivot, and
ad-hoc SQL.  Data analysts can copy this file as a starting
point for their own analyses.
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class DemoAnalysis(BaseAnalyzer):
    """Example analysis that exercises every major Processor method."""

    name = "demo"
    description = "Built-in demo: coverage, filtering, aggregation, and pivot"

    def run(self, p):
        """Execute the full demo sequence."""
        print("\n── Key coverage ──────────────────────────────────")
        print(p.coverage().to_string(index=False))

        print("\n── Preview ───────────────────────────────────────")
        print(p.preview(5).to_string(index=False))

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
        print(filtered.to_string(index=False))

        print("\n── Aggregate: SUM(amount) by region ──────────────")
        print(p.aggregate("region", "amount", "SUM").to_string(index=False))

        print("\n── Aggregate: AVG(amount) by tier ────────────────")
        print(p.aggregate("tier", "amount", "AVG").to_string(index=False))

        print("\n── Pivot: region x tier -> SUM(amount) ────────────")
        print(p.pivot("region", "tier", "amount").to_string(index=False))

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
        print(result.to_string(index=False))
