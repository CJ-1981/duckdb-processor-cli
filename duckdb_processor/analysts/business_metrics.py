"""Business metrics and KPI calculations.

This analyst demonstrates common business metrics:
- Pareto analysis (80/20 rule)
- Customer segmentation (RFM-like)
- Growth rates and trends
- Ranking and percentiles

Run via::
    python -m duckdb_processor data.csv --run business_metrics

Expected columns:
    - entity (categorical): Customer, product, region, etc.
    - amount (numeric): Revenue, quantity, score, etc.
    - date (optional): For time-based metrics
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class BusinessMetrics(BaseAnalyzer):
    """Common business KPIs and metrics."""

    name = "business_metrics"
    description = "Business metrics: Pareto, RFM, growth, ranking"

    def run(self, p):
        """Execute business metric calculations."""
        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Find entity and amount columns
        entity_col = None
        for col in p.columns:
            if col.lower() in ["customer", "product", "item", "region", "category", "entity"]:
                entity_col = col
                break

        amount_col = None
        for col in p.columns:
            if col.lower() in ["amount", "revenue", "price", "quantity", "value", "total"]:
                amount_col = col
                break

        if not entity_col:
            entity_col = p.columns[0]  # Use first column as entity
            print(f"Using '{entity_col}' as entity column")

        if not amount_col:
            print("Warning: No amount column found. Some metrics will be skipped.")
            print("Expected: amount, revenue, price, quantity, value, or total")
            return

        print(f"\n### Business Metrics Analysis")
        print(f"Entity: '{entity_col}'")
        print(f"Metric: '{amount_col}'")

        # ── Metric 1: Pareto Analysis (80/20 Rule) ────────────────
        print("\n### Metric 1: Pareto Analysis (80/20 Rule)")
        print("Business Question: Do top 20% of entities contribute 80% of value?")
        print("SQL (with running totals):")
        print(f"    p.sql(\"SELECT")
        print(f"      {entity_col},")
        print(f"      {amount_col},")
        print(f"      SUM({amount_col}) OVER (ORDER BY {amount_col} DESC) as running_total")
        print(f"    FROM data\")")

        pareto = p.sql(f"""
            WITH entity_totals AS (
                SELECT
                    "{entity_col}",
                    SUM(CAST("{amount_col}" AS DOUBLE)) as total
                FROM data
                WHERE "{amount_col}" != '' AND "{amount_col}" IS NOT NULL
                GROUP BY "{entity_col}"
            ),
            ranked_entities AS (
                SELECT
                    "{entity_col}",
                    total,
                    ROW_NUMBER() OVER (ORDER BY total DESC) as rank,
                    SUM(total) OVER (ORDER BY total DESC) as running_total,
                    SUM(total) OVER () as grand_total
                FROM entity_totals
            )
            SELECT
                "{entity_col}",
                ROUND(total, 2) as total,
                ROUND(100 * total / grand_total, 2) as pct_of_total,
                ROUND(100 * running_total / grand_total, 2) as cumulative_pct,
                ROUND(100 * rank / (SELECT COUNT(*) FROM entity_totals), 2) as percentile_rank
            FROM ranked_entities
            ORDER BY total DESC
        """)
        show("Pareto Analysis (80/20)", pareto)

        # Calculate actual 80/20 split
        top_20_pct = p.sql(f"""
            WITH entity_totals AS (
                SELECT
                    "{entity_col}",
                    SUM(CAST("{amount_col}" AS DOUBLE)) as total
                FROM data
                GROUP BY "{entity_col}"
            ),
            with_rank AS (
                SELECT
                    total,
                    NTILE(5) OVER (ORDER BY total DESC) as quintile
                FROM entity_totals
            )
            SELECT
                quintile,
                COUNT(*) as entity_count,
                ROUND(SUM(total), 2) as group_total,
                ROUND(100 * SUM(total) / (SELECT SUM(total) FROM entity_totals), 2) as contribution_pct
            FROM with_rank
            GROUP BY quintile
            ORDER BY quintile
        """)
        show("80/20 Split by Quintile", top_20_pct)

        # ── Metric 2: Percentile Rankings ─────────────────────────
        print("\n### Metric 2: Percentile Rankings")
        print("Business Question: Where does each entity rank?")
        print("SQL (using PERCENT_RANK):")
        print(f"    p.sql(\"SELECT")
        print(f"      {entity_col},")
        print(f"      {amount_col},")
        print(f"      PERCENT_RANK() OVER (ORDER BY {amount_col}) as percentile")
        print(f"    FROM data\")")

        percentiles = p.sql(f"""
            WITH entity_totals AS (
                SELECT
                    "{entity_col}",
                    SUM(CAST("{amount_col}" AS DOUBLE)) as total
                FROM data
                GROUP BY "{entity_col}"
            )
            SELECT
                "{entity_col}",
                ROUND(total, 2) as total,
                ROUND(100 * PERCENT_RANK() OVER (ORDER BY total), 2) as percentile_rank,
                CASE
                    WHEN PERCENT_RANK() OVER (ORDER BY total) >= 0.9 THEN 'Top 10%'
                    WHEN PERCENT_RANK() OVER (ORDER BY total) >= 0.75 THEN 'Top 25%'
                    WHEN PERCENT_RANK() OVER (ORDER BY total) >= 0.5 THEN 'Top 50%'
                    ELSE 'Bottom 50%'
                END as tier
            FROM entity_totals
            ORDER BY total DESC
        """)
        show("Percentile Rankings", percentiles)

        # ── Metric 3: Year-over-Year Growth ────────────────────────
        date_col = None
        for col in p.columns:
            if col.lower() in ["date", "timestamp", "created_at", "year", "month"]:
                date_col = col
                break

        if date_col:
            print("\n### Metric 3: Period-over-Period Growth")
            print("Business Question: How are we growing vs previous period?")
            print(f"Using date column: '{date_col}'")

            # Try to extract year from the date column
            yoy_growth = p.sql(f"""
                WITH period_totals AS (
                    SELECT
                        DATE_PART('year', CAST(\"{date_col}\" AS DATE))::int as period,
                        SUM(CAST(\"{amount_col}\" AS DOUBLE)) as total
                    FROM data
                    WHERE \"{date_col}\" != ''
                    GROUP BY period
                )
                SELECT
                    period,
                    ROUND(total, 2) as current_value,
                    ROUND(LAG(total) OVER (ORDER BY period), 2) as previous_value,
                    ROUND(
                        100 * (total - LAG(total) OVER (ORDER BY period)) /
                        NULLIF(LAG(total) OVER (ORDER BY period), 0),
                        2
                    ) as growth_pct
                FROM period_totals
                ORDER BY period DESC
                LIMIT 5
            """)
            show("Period-over-Period Growth", yoy_growth)

        # ── Metric 4: Concentration Ratio ─────────────────────────
        print("\n### Metric 4: Concentration Ratio")
        print("Business Question: How concentrated is the value among top entities?")
        print("Top 3 entities / Top 10 entities / All entities")

        concentration = p.sql(f"""
            WITH entity_totals AS (
                SELECT
                    "{entity_col}",
                    SUM(CAST(\"{amount_col}\" AS DOUBLE)) as total
                FROM data
                GROUP BY "{entity_col}"
            ),
            ranked AS (
                SELECT
                    total,
                    ROW_NUMBER() OVER (ORDER BY total DESC) as rank,
                    SUM(total) OVER () as grand_total
                FROM entity_totals
            )
            SELECT
                'Top 3' as metric,
                ROUND(SUM(total) / ANY_VALUE(grand_total) * 100, 2) as concentration_pct
            FROM ranked
            WHERE rank <= 3
            GROUP BY 1
            UNION ALL
            SELECT
                'Top 10',
                ROUND(SUM(total) / ANY_VALUE(grand_total) * 100, 2)
            FROM ranked
            WHERE rank <= 10
            GROUP BY 1
            UNION ALL
            SELECT
                'Top 25',
                ROUND(SUM(total) / ANY_VALUE(grand_total) * 100, 2)
            FROM ranked
            WHERE rank <= 25
            GROUP BY 1
        """)
        show("Concentration Ratio", concentration)

        # ── Metric 5: Average vs Median Comparison ─────────────────
        print("\n### Metric 5: Central Tendency Comparison")
        print("Business Question: Is the distribution skewed?")
        print("Compare mean (sensitive to outliers) vs median (robust)")

        central = p.sql(f"""
            SELECT
                ROUND(AVG(CAST(\"{amount_col}\" AS DOUBLE)), 2) as mean_value,
                ROUND(MEDIAN(CAST(\"{amount_col}\" AS DOUBLE)), 2) as median_value,
                ROUND(
                    100 * (AVG(CAST(\"{amount_col}\" AS DOUBLE)) -
                           MEDIAN(CAST(\"{amount_col}\" AS DOUBLE))) /
                           NULLIF(MEDIAN(CAST(\"{amount_col}\" AS DOUBLE)), 0),
                    2
                ) as pct_difference,
                COUNT(*) as total_records,
                COUNT(DISTINCT "{entity_col}") as unique_entities
            FROM data
            WHERE \"{amount_col}\" != '' AND \"{amount_col}\" IS NOT NULL
        """)
        show("Mean vs Median", central)

        print("\n### Business Metrics Tips")
        print("1. Use Pareto analysis to focus on high-impact entities")
        print("2. Monitor concentration ratio for risk assessment")
        print("3. Compare mean vs median to detect skewness")
        print("4. Track percentile rankings for performance")
        print("5. Calculate growth rates over consistent periods")
        print("6. Use quintiles/deciles for segmentation")
