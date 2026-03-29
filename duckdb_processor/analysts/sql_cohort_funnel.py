"""SQL Cohort and Funnel analysis patterns.

This analyst demonstrates how to perform complex product/behavioral analysis
using SQL features in DuckDB:
- Cohort Retention Analysis
- Setup of Acquisition Cohorts
- Funnel Analysis (Step-by-Step Conversion)
- Conditional Aggregations

Run via::
    python -m duckdb_processor data.csv --run sql_cohort_funnel
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class SqlCohortFunnel(BaseAnalyzer):
    """SQL product analytics: Cohort and Funnel analysis."""

    name = "sql_cohort_funnel"
    description = "Product Analytics: Cohort Retention and Funnel Analysis using SQL"

    def run(self, p):
        """Execute behavioral SQL analysis patterns."""
        print("\n### Product Analytics: Cohorts and Funnels (SQL)")
        print("=" * 58)
        print("This module demonstrates how to track user retention over time")
        print("and how to measure conversion funnels step-by-step.")
        print("=" * 58)

        def show(title, query):
            print(f"\n── {title} ─────────────────────────────────────")
            try:
                result = p.sql(query)
                if p.formatter:
                    p.formatter.format_dataframe(result.head(15))
                else:
                    print(result.head(15).to_string(index=False))
            except Exception as e:
                print(f"Note: Query could not be executed on this dataset.")
                print(f"Reason: {e}")
                print(f"SQL Query:\n{query}\n")

        # ── Pattern 1: User/Entity Acquisition Cohorts ───────────────────────
        q1 = """
        -- A 'Cohort' is a group of subjects who share a defining characteristic.
        -- Often, this is the date/month they were first acquired.
        WITH FirstSeen AS (
            -- For each ID, find their FIRST activity timestamp
            SELECT 
                id,
                MIN(timestamp) as acquisition_date
            FROM data
            WHERE timestamp IS NOT NULL
            GROUP BY id
        )
        -- Then count how many users belong to each cohort
        SELECT 
            -- Truncate to month for monthly cohorts
            DATE_TRUNC('month', CAST(acquisition_date AS DATE)) as cohort_month,
            COUNT(DISTINCT id) as new_users
        FROM FirstSeen
        GROUP BY DATE_TRUNC('month', CAST(acquisition_date AS DATE))
        ORDER BY cohort_month
        """
        show("Pattern 1: Acquisition Cohorts (By Month)", q1)

        # ── Pattern 2: Funnel Analysis ───────────────────────────────────────
        # Using a simulated funnel structure based on status or conditional logic
        q2 = """
        -- A Funnel measures the progression of users through a series of steps.
        -- We use conditional aggregation (FILTER) to count progression.
        SELECT 
            -- Step 1: All available records (Total Impressions/Events)
            COUNT(*) as step_1_total_events,
            
            -- Step 2: Only active records
            COUNT(id) FILTER (WHERE status = 'active') as step_2_active_users,
            
            -- Step 3: High value actions (> 5000)
            COUNT(id) FILTER (WHERE amount > 5000) as step_3_high_value,
            
            -- Conversion Rates
            CAST(COUNT(id) FILTER (WHERE status = 'active') AS FLOAT) / 
            NULLIF(COUNT(*), 0) * 100 as step1_to_2_conv_rate,

            CAST(COUNT(id) FILTER (WHERE amount > 5000) AS FLOAT) / 
            NULLIF(COUNT(id) FILTER (WHERE status = 'active'), 0) * 100 as step2_to_3_conv_rate

        FROM data
        WHERE status IS NOT NULL AND amount IS NOT NULL
        """
        show("Pattern 2: Conversion Funnel Analysis", q2)

        # ── Pattern 3: Cohort Retention Over Time ────────────────────────────
        q3 = """
        -- Combining Acquisition Date with subsequent Activity Dates
        WITH FirstSeen AS (
            SELECT id, MIN(timestamp) as acquisition_date
            FROM data
            WHERE timestamp IS NOT NULL
            GROUP BY id
        ),
        Activity AS (
            SELECT 
                data.id, 
                data.timestamp as activity_date,
                f.acquisition_date
            FROM data
            JOIN FirstSeen f ON data.id = f.id
            WHERE data.timestamp IS NOT NULL
        )
        -- Calculate the difference between activity and acquisition
        SELECT 
            -- Truncate to month for monthly cohorts
            DATE_TRUNC('month', CAST(acquisition_date AS DATE)) as cohort_month,
            -- Calculate month difference
            DATE_DIFF('month', CAST(acquisition_date AS DATE), CAST(activity_date AS DATE)) as month_index,
            -- Count active distinct users in that specific month index
            COUNT(DISTINCT id) as active_users
        FROM Activity
        GROUP BY 
            DATE_TRUNC('month', CAST(acquisition_date AS DATE)),
            DATE_DIFF('month', CAST(acquisition_date AS DATE), CAST(activity_date AS DATE))
        ORDER BY cohort_month, month_index
        """
        show("Pattern 3: Cohort Retention (Months Since Acquisition)", q3)

        print("\n### Tips for Analytics")
        print("1. Cohort Analysis requires identifying an entity's 'Origin/First Seen' date.")
        print("2. Funnel conversion rates often use `NULLIF(denominator, 0)` to prevent Division By Zero.")
        print("3. `COUNT(...) FILTER (WHERE condition)` is the cleanest way to do conditional pivots.")
