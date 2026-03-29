"""Time-based analysis patterns.

This analyst demonstrates how to analyze data with temporal dimensions:
- Extracting date parts (year, month, day, weekday)
- Time-based grouping and trending
- Period-over-period comparisons
- Moving averages

Run via::
    python -m duckdb_processor data.csv --run time_analysis

Expected columns:
    - date/timestamp/temporal: Date or timestamp column
    - amount (numeric): Metric to analyze over time
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class TimeAnalysis(BaseAnalyzer):
    """Time series analysis and temporal patterns."""

    name = "time_analysis"
    description = "Time-based analysis: trends, periods, moving averages"

    def run(self, p):
        """Execute time-based analysis patterns."""
        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        # Find the date column
        date_col = None
        for col in p.columns:
            if col.lower() in ["date", "timestamp", "created_at", "time", "datetime"]:
                date_col = col
                break

        if not date_col:
            print("Error: No date column found. Expected: date, timestamp, created_at, time, or datetime")
            print(f"Available columns: {', '.join(p.columns)}")
            return

        print(f"\n### Using date column: '{date_col}'")

        # Find a numeric column to aggregate
        numeric_col = None
        numeric_candidates = ["amount", "total", "value", "price", "revenue", "sales", "cost", "quantity", "count"]
        # First try well-known names
        for candidate in numeric_candidates:
            if candidate in [c.lower() for c in p.columns]:
                numeric_col = next(c for c in p.columns if c.lower() == candidate)
                break
        # Fall back to first numeric-looking column that isn't the date col
        if not numeric_col:
            for col in p.columns:
                if col != date_col and col.lower() not in ["id", "name", "description", "category", "status"]:
                    numeric_col = col
                    break
        if not numeric_col:
            print("Error: No numeric column found for aggregation.")
            print(f"Available columns: {', '.join(p.columns)}")
            return

        print(f"### Using numeric column: '{numeric_col}'")

        # ── Pattern 1: Daily Trend ─────────────────────────────
        print("\n### Pattern 1: Daily Trend Analysis")
        print("Business Question: How does the metric vary by day?")
        print("SQL:")
        print(f"    p.sql(\"SELECT")
        print(f"      DATE_TRUNC('day', {date_col}) as day,")
        print(f"      SUM({numeric_col}) as daily_total")
        print(f"    FROM data GROUP BY day ORDER BY day\")")
        daily = p.sql(f"""
            SELECT
                DATE_TRUNC('day', CAST("{date_col}" AS DATE)) as day,
                COUNT(*) as records,
                ROUND(SUM(CAST("{numeric_col}" AS DOUBLE)), 2) as daily_total
            FROM data
            GROUP BY day
            ORDER BY day
            LIMIT 30
        """)
        show("Daily Trend (Last 30 Days)", daily)

        # ── Pattern 2: Monthly Aggregation ──────────────────────
        print("\n### Pattern 2: Monthly Aggregation")
        print("Business Question: What's the monthly performance?")
        print("SQL:")
        print(f"    p.sql(\"SELECT")
        print(f"      DATE_PART('year', {date_col}) as year,")
        print(f"      DATE_PART('month', {date_col}) as month,")
        print(f"      SUM({numeric_col}) as monthly_total")
        print(f"    FROM data GROUP BY year, month\")")
        monthly = p.sql(f"""
            SELECT
                DATE_PART('year', CAST({date_col} AS DATE))::int as year,
                DATE_PART('month', CAST({date_col} AS DATE))::int as month,
                COUNT(*) as records,
                ROUND(SUM(CAST({numeric_col} AS DOUBLE)), 2) as monthly_total
            FROM data
            GROUP BY year, month
            ORDER BY year DESC, month DESC
            LIMIT 12
        """)
        show("Monthly Performance", monthly)

        # ── Pattern 3: Day of Week Analysis ─────────────────────
        print("\n### Pattern 3: Day of Week Patterns")
        print("Business Question: Which days perform best?")
        print("SQL:")
        print(f"    p.sql(\"SELECT")
        print(f"      DAYNAME({date_col}) as day_of_week,")
        print(f"      AVG({numeric_col}) as average")
        print(f"    FROM data GROUP BY day_of_week\")")
        dow = p.sql(f"""
            SELECT
                DAYNAME(CAST({date_col} AS DATE)) as day_of_week,
                COUNT(*) as record_count,
                ROUND(AVG(CAST({numeric_col} AS DOUBLE)), 2) as daily_avg,
                ROUND(SUM(CAST({numeric_col} AS DOUBLE)), 2) as total
            FROM data
            GROUP BY day_of_week
            ORDER BY
                CASE day_of_week
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                END
        """)
        show("Day of Week Analysis", dow)

        # ── Pattern 4: Moving Average (7-day) ───────────────────
        print("\n### Pattern 4: Moving Average (Smoothing)")
        print("Business Question: What's the trend with noise removed?")
        print("SQL (using window functions):")
        print(f"    p.sql(\"SELECT")
        print(f"      day, daily_total,")
        print(f"      AVG(daily_total) OVER (")
        print(f"        ORDER BY day")
        print(f"        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW")
        print(f"      ) as moving_avg_7d")
        print(f"    FROM daily_totals\")")
        moving_avg = p.sql(f"""
            WITH daily_totals AS (
                SELECT
                    DATE_TRUNC('day', CAST("{date_col}" AS DATE)) as day,
                    SUM(CAST("{numeric_col}" AS DOUBLE)) as daily_total
                FROM data
                GROUP BY day
            )
            SELECT
                day,
                ROUND(daily_total, 2) as daily_total,
                ROUND(AVG(daily_total) OVER (
                    ORDER BY day
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) as moving_avg_7d
            FROM daily_totals
            ORDER BY day DESC
            LIMIT 14
        """)
        show("7-Day Moving Average (Recent 14 Days)", moving_avg)

        # ── Pattern 5: Period-over-Period Growth ────────────────
        print("\n### Pattern 5: Month-over-Month Growth")
        print("Business Question: How much did we grow vs last month?")
        print("SQL (using LAG window function):")
        print(f"    p.sql(\"SELECT")
        print(f"      month, total,")
        print(f"      LAG(total) OVER (ORDER BY month) as prev_month,")
        print(f"      100 * (total - prev_month) / prev_month as growth_pct")
        print(f"    FROM monthly_totals\")")
        mom_growth = p.sql(f"""
            WITH monthly_totals AS (
                SELECT
                    DATE_TRUNC('month', CAST("{date_col}" AS DATE)) as month,
                    SUM(CAST("{numeric_col}" AS DOUBLE)) as total
                FROM data
                GROUP BY month
            )
            SELECT
                month,
                ROUND(total, 2) as current_month,
                ROUND(LAG(total) OVER (ORDER BY month), 2) as previous_month,
                ROUND(
                    100 * (total - LAG(total) OVER (ORDER BY month)) /
                    LAG(total) OVER (ORDER BY month),
                    2
                ) as growth_pct
            FROM monthly_totals
            ORDER BY month DESC
            LIMIT 6
        """)
        show("Month-over-Month Growth", mom_growth)

        # ── Pattern 6: Hourly Distribution (if timestamp) ───────
        if "timestamp" in date_col.lower() or "datetime" in date_col.lower():
            print("\n### Pattern 6: Hourly Distribution")
            print("Business Question: What's the activity by hour?")
            hourly = p.sql(f"""
                SELECT
                    DATE_PART('hour', CAST({date_col} AS TIMESTAMP))::int as hour,
                    COUNT(*) as record_count,
                    ROUND(SUM(CAST({numeric_col} AS DOUBLE)), 2) as hourly_total
                FROM data
                GROUP BY hour
                ORDER BY hour
            """)
            show("Hourly Distribution", hourly)

        print("\n### Time Analysis Tips")
        print("1. Use DATE_TRUNC() for grouping by day/week/month")
        print("2. Use DATE_PART() to extract specific components")
        print("3. Use DAYNAME() for day-of-week analysis")
        print("4. Use window functions (LAG, AVG with OVER) for trends")
        print("5. Moving averages smooth out daily fluctuations")
        print("6. Always ORDER BY time for trend visualization")
