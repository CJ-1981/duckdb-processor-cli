"""Pure Python data analysis patterns (no SQL required).

This analyst demonstrates how to perform analysis using only Python/pandas:
- Data manipulation without SQL
- Custom business logic
- Complex calculations
- Machine learning preparation

Run via::
    python -m duckdb_processor data.csv --run python_patterns

No specific columns required - works with any data.
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class PythonPatterns(BaseAnalyzer):
    """Pure Python analysis patterns for non-SQL users."""

    name = "python_patterns"
    description = "Python-only analysis: no SQL required"

    def run(self, p):
        """Execute Python-native analysis patterns."""
        import pandas as pd

        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        print("\n### Python Data Analysis (No SQL)")
        print("=" * 58)

        # ── Pattern 1: Load Data into Pandas ───────────────────────
        print("\n### Pattern 1: Load Data into Pandas DataFrame")
        print("Python API:")
        print("    df = p.sql('SELECT * FROM data')  # Load all data")
        print("    # Now use any pandas method!")

        df = p.sql("SELECT * FROM data")
        print(f"\nLoaded {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {', '.join(df.columns)}")

        # ── Pattern 2: Filter with Python ─────────────────────────
        print("\n### Pattern 2: Filter Data (Python Way)")
        print("Python API:")
        print("    df[df['amount'] > 1000]")
        print("    # vs SQL: WHERE amount > 1000")

        # Find a numeric column
        numeric_col = None
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64', 'object']:
                try:
                    pd.to_numeric(df[col], errors='coerce')
                    numeric_col = col
                    break
                except:
                    continue

        if numeric_col:
            df_numeric = df.copy()
            df_numeric[numeric_col] = pd.to_numeric(df_numeric[numeric_col], errors='coerce')

            # Filter high values
            high_values = df_numeric[df_numeric[numeric_col] > df_numeric[numeric_col].median()].head()
            show(f"Rows where '{numeric_col}' > median", high_values)

        # ── Pattern 3: Group and Aggregate (Python) ────────────────
        print("\n### Pattern 3: Group and Aggregate (Python Way)")
        print("Python API:")
        print("    df.groupby('category')['amount'].agg(['sum', 'mean', 'count'])")

        # Find a categorical column
        cat_col = None
        for col in df.columns:
            if df[col].dtype == 'object' and col != numeric_col:
                cat_col = col
                break

        if cat_col and numeric_col:
            # Convert to numeric and handle errors
            df_agg = df.copy()
            df_agg[numeric_col] = pd.to_numeric(df_agg[numeric_col], errors='coerce')

            # Group and aggregate
            try:
                grouped = df_agg.groupby(cat_col)[numeric_col].agg(['sum', 'mean', 'count']).reset_index()
                grouped.columns = [cat_col, 'total', 'average', 'count']
                grouped = grouped.sort_values('total', ascending=False)
                show(f"Aggregation by '{cat_col}' (Python)", grouped.head())
            except Exception as e:
                print(f"Note: Could not aggregate '{numeric_col}' - {e}")
                # Fallback to count only
                grouped = df.groupby(cat_col).size().reset_index()
                grouped.columns = [cat_col, 'count']
                grouped = grouped.sort_values('count', ascending=False)
                show(f"Count by '{cat_col}' (Python)", grouped.head())

        # ── Pattern 4: Custom Business Logic ───────────────────────
        print("\n### Pattern 4: Custom Business Logic")
        print("Python API: Create calculated columns")
        print("    df['new_col'] = df['col1'] * df['col2']")

        if numeric_col:
            df_calc = df.copy()
            df_calc[numeric_col] = pd.to_numeric(df_calc[numeric_col], errors='coerce')

            # Create custom categories
            def categorize_value(val):
                if pd.isna(val):
                    return 'Missing'
                elif val == 0:
                    return 'Zero'
                elif val < df_calc[numeric_col].median():
                    return 'Below Median'
                else:
                    return 'Above Median'

            df_calc['value_category'] = df_calc[numeric_col].apply(categorize_value)
            category_dist = df_calc['value_category'].value_counts().reset_index()
            category_dist.columns = ['category', 'count']
            show("Custom Categorization", category_dist)

        # ── Pattern 5: Data Transformation ─────────────────────────
        print("\n### Pattern 5: Data Transformation")
        print("Python API: String operations, date parsing")
        print("    df['text'] = df['text'].str.lower()")
        print("    df['date'] = pd.to_datetime(df['date'])")

        # Demonstrate string operations
        text_col = None
        for col in df.columns:
            if df[col].dtype == 'object':
                text_col = col
                break

        if text_col:
            df_text = df.copy()
            df_text[f'{text_col}_upper'] = df_text[text_col].str.upper()
            df_text[f'{text_col}_length'] = df_text[text_col].str.len()
            show("String Operations Example", df_text[[text_col, f'{text_col}_upper', f'{text_col}_length']].head())

        # ── Pattern 6: Merge/Join Operations ───────────────────────
        print("\n### Pattern 6: Self-Join (Compare Rows)")
        print("Python API:")
        print("    pd.merge(df1, df2, on='key', how='inner')")

        if cat_col and numeric_col:
            try:
                # Create summary and merge back
                df_merge = df.copy()
                df_merge[numeric_col] = pd.to_numeric(df_merge[numeric_col], errors='coerce')
                summary = df_merge.groupby(cat_col)[numeric_col].agg(['mean']).reset_index()
                summary.columns = [cat_col, 'avg_value']
                merged = pd.merge(df_merge, summary, on=cat_col, how='left')
                show("Merge Example (with average)", merged[[cat_col, numeric_col, 'avg_value']].head())
            except Exception as e:
                print(f"Note: Merge operation skipped - {e}")

        # ── Pattern 7: Sorting and Ranking ─────────────────────────
        print("\n### Pattern 7: Sorting and Ranking")
        print("Python API:")
        print("    df.sort_values('col', ascending=False)")
        print("    df['rank'] = df['col'].rank()")

        if numeric_col:
            try:
                df_rank = df.copy()
                df_rank[numeric_col] = pd.to_numeric(df_rank[numeric_col], errors='coerce')
                df_rank = df_rank.sort_values(numeric_col, ascending=False)
                df_rank['rank'] = df_rank[numeric_col].rank(ascending=False)
                df_rank['percentile'] = df_rank[numeric_col].rank(pct=True)
                display_col = cat_col if cat_col else df_rank.columns[0]
                show("Ranking Example", df_rank[[display_col, numeric_col, 'rank', 'percentile']].head())
            except Exception as e:
                print(f"Note: Ranking operation skipped - {e}")

        # ── Pattern 8: Pivot Tables ─────────────────────────────────
        print("\n### Pattern 8: Pivot Tables")
        print("Python API:")
        print("    df.pivot_table(index='row', columns='col', values='val', aggfunc='sum')")

        if cat_col and numeric_col:
            try:
                pivot = df.pivot_table(
                    index=cat_col,
                    values=numeric_col,
                    aggfunc=['sum', 'mean', 'count']
                ).reset_index()
                pivot.columns = ['_'.join(str(col).strip() for col in col_tuple) for col_tuple in pivot.columns.values]
                show("Pivot Table Example", pivot.head())
            except Exception as e:
                print(f"Note: Pivot table operation skipped - {e}")

        # ── Pattern 9: Handling Missing Data ───────────────────────
        print("\n### Pattern 9: Missing Data Handling")
        print("Python API:")
        print("    df.isna().sum()  # Count missing")
        print("    df.fillna(0)     # Fill missing")
        print("    df.dropna()     # Remove missing")

        missing = df.isna().sum().reset_index()
        missing.columns = ['column', 'missing_count']
        missing = missing[missing['missing_count'] > 0]
        if not missing.empty:
            show("Missing Values", missing)
        else:
            print("\n✓ No missing values in dataset")

        # ── Pattern 10: Export Back to SQL ─────────────────────────
        print("\n### Pattern 10: Export Python Results to DuckDB")
        print("Python API:")
        print("    # Create view from pandas DataFrame")
        print("    p.con.execute('CREATE VIEW my_view AS SELECT * FROM df')")

        print("\n### Python vs SQL Comparison")
        print("┌─────────────────────┬─────────────────────┬──────────────────┐")
        print("│ Operation           │ Python              │ SQL              │")
        print("├─────────────────────┼─────────────────────┼──────────────────┤")
        print("│ Filter              │ df[df.col > x]      │ WHERE col > x    │")
        print("│ Group               │ df.groupby('col')   │ GROUP BY col     │")
        print("│ Aggregate           │ .agg(['sum','mean']) │ SUM(), AVG()     │")
        print("│ Sort                │ .sort_values('col') │ ORDER BY col     │")
        print("│ Join                │ pd.merge()          │ JOIN             │")
        print("│ Missing             │ .isna(), .fillna()  │ COALESCE()       │")
        print("└─────────────────────┴─────────────────────┴──────────────────┘")

        print("\n### Tips for Python Users")
        print("1. Start with df = p.sql('SELECT * FROM data')")
        print("2. Use pandas methods for most operations")
        print("3. Switch to SQL only for complex window functions")
        print("4. Export results: df.to_csv('output.csv')")
        print("5. Chain operations: df.filter().groupby().agg()")
        print("6. Use .pipe() for complex transformations")
        print("7. Apply custom functions with .apply() or .transform()")
