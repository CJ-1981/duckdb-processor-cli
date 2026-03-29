"""Advanced Python Analytics (Pandas-only).

This analyst demonstrates how to perform complex data analysis using 
pure Python and pandas. No SQL required. 
- Rolling Calculations (Moving Averages)
- Advanced Grouping and Map-Reduce Patterns
- Handling Time-Series Data

Run via::
    python -m duckdb_processor data.csv --run python_advanced_analytics
"""
import pandas as pd
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class PythonAdvancedAnalytics(BaseAnalyzer):
    """Advanced pandas-based analytics for Python power users."""

    name = "python_advanced_analytics"
    description = "Advanced Python: Rolling metrics, DateTime manipulation, Complex Grouping"

    def run(self, p):
        """Execute advanced Python analysis patterns."""
        print("\n### Advanced Analytics using Python (No SQL)")
        print("=" * 58)
        print("This module demonstrates pandas functions for time-series,")
        print("rolling metrics, and complex map-reduce behavior.")
        print("=" * 58)

        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df.head(10))
            else:
                print(df.head(10).to_string(index=False))

        # ── Setup: Load Data into Pandas ───────────────────────
        df = p.sql("SELECT * FROM data")
        
        # Identify numeric and datetime columns for examples
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        date_cols = []
        for c in df.columns:
            if df[c].dtype == 'object':
                try:
                    pd.to_datetime(df[c])
                    date_cols.append(c)
                except:
                    pass

        if not numeric_cols:
            print("No numeric columns found. Advanced Python analytics require numeric data.")
            return

        date_col = date_cols[0] if date_cols else None
        num_col = numeric_cols[0]

        # ── Pattern 1: Advanced DateTime Manipulation ─────────────────────────
        if date_col:
            print("\n### Pattern 1: Date/Time Feature Engineering")
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df_dates = df.dropna(subset=[date_col]).copy()
            
            # Extract new features
            df_dates['year'] = df_dates[date_col].dt.year
            df_dates['month'] = df_dates[date_col].dt.month
            df_dates['day_of_week'] = df_dates[date_col].dt.day_name()
            df_dates['is_weekend'] = df_dates[date_col].dt.weekday >= 5
            
            show("Datetime Feature Extraction", df_dates[[date_col, 'year', 'month', 'day_of_week', 'is_weekend']])

            # ── Pattern 2: Rolling Calculations (Time Series) ─────────────────
            print("\n### Pattern 2: Rolling Metrics (Moving Averages)")
            # Sort by date for rolling windows
            df_ts = df_dates.sort_values(by=date_col)
            
            # Simple 3-period rolling average
            df_ts['rolling_3_avg'] = df_ts[num_col].rolling(window=3, min_periods=1).mean()
            
            # Rolling sum
            df_ts['rolling_3_sum'] = df_ts[num_col].rolling(window=3, min_periods=1).sum()

            show("Time-Series Rolling Window (3-period)", df_ts[[date_col, num_col, 'rolling_3_avg', 'rolling_3_sum']])

        # ── Pattern 3: Advanced `.agg()` and GroupBy ─────────────────────────
        print("\n### Pattern 3: Complex Group/Aggregate (Map-Reduce)")
        # Identify a categorical column
        cat_cols = [c for c in df.columns if c not in numeric_cols and c not in date_cols]
        cat_col = cat_cols[0] if cat_cols else None

        if cat_col:
            # Custom Aggregation Function setup
            def percent_positive(series):
                return (series > 0).mean() * 100

            df_grouped = df.groupby(cat_col).agg(
                count_rows=(num_col, 'size'),
                sum_values=(num_col, 'sum'),
                mean_values=(num_col, 'mean'),
                max_value=(num_col, 'max'),
                percent_positive=(num_col, percent_positive) # Custom applying lambda logic
            ).reset_index()
            
            show(f"Multi-metric Aggregation on '{cat_col}'", df_grouped)

            # ── Pattern 4: Cross Tabulation ──────────────────────────────────
            print("\n### Pattern 4: Cross-Tabulation (Frequency tables)")
            cat_col_2 = cat_cols[1] if len(cat_cols) > 1 else None
            
            if cat_col_2:
                # pd.crosstab computes a simple cross-tabulation of two (or more) factors
                cross_tab = pd.crosstab(df[cat_col], df[cat_col_2], normalize='index') * 100
                # Flatten the index for display
                cross_tab = cross_tab.reset_index()
                show(f"CrossTab (% format) between {cat_col} and {cat_col_2}", cross_tab)
            else:
                print(f"Skipping CrossTab: Needs at least two categorical columns.")

        # ── Pattern 5: Window Operations via `.groupby().transform()` ────────
        print("\n### Pattern 5: Pandas Equivalent of SQL Window Functions")
        if cat_col:
            df_window = df.copy()
            # Calculate the group mean and broadcast it back to EVERY row (equiv to OVER(PARTITION BY))
            df_window['group_mean'] = df_window.groupby(cat_col)[num_col].transform('mean')
            
            # Calculate difference from group mean
            df_window['diff_from_group_mean'] = df_window[num_col] - df_window['group_mean']
            
            show("Row-Level vs Group-Level using .transform()", df_window[[cat_col, num_col, 'group_mean', 'diff_from_group_mean']])

        print("\n### Summary of Advanced pandas Tricks:")
        print("1. `.dt` accessor provides fast operations on datetime strings.")
        print("2. `.rolling(window=X)` allows computation of sliding-window moving averages.")
        print("3. `.agg()` can take a dictionary to apply different functions to different columns.")
        print("4. `.transform()` allows aggregations to retain the original dataframe length (Window Functions).")
