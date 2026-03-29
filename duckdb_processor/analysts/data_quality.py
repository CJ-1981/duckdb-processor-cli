"""Data quality and validation patterns.

This analyst demonstrates how to check data quality:
- Missing value analysis
- Duplicate detection
- Data type validation
- Outlier detection
- Data consistency checks

Run via::
    python -m duckdb_processor data.csv --run data_quality

No specific columns required - analyzes all available data.
"""
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class DataQuality(BaseAnalyzer):
    """Data quality checks and validation."""

    name = "data_quality"
    description = "Check data quality: missing values, duplicates, outliers"

    def run(self, p):
        """Execute data quality checks."""
        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df)
            else:
                print(df.to_string(index=False))

        print("\n### Data Quality Report")
        print("=" * 58)

        # ── Check 1: Overall Statistics ─────────────────────────
        print("\n### Check 1: Overall Data Statistics")
        total_rows = p._meta["n_records"]
        total_cols = len(p.columns)
        print(f"Total Rows: {total_rows:,}")
        print(f"Total Columns: {total_cols}")
        print(f"Data Source: {p._meta['source']}")

        # ── Check 2: Column Coverage (Missing Values) ───────────
        print("\n### Check 2: Missing Value Analysis")
        print("Business Question: Which columns have missing data?")
        coverage = p.coverage()
        show("Column Fill Rates", coverage)

        # Flag columns with poor coverage
        poor_coverage = coverage[coverage["coverage_%"] < 90]
        if not poor_coverage.empty:
            print("\n⚠️  WARNING: Columns with < 90% coverage:")
            print(poor_coverage.to_string(index=False))
        else:
            print("\n✓ All columns have ≥ 90% coverage")

        # ── Check 3: Duplicate Detection ─────────────────────────
        print("\n### Check 3: Duplicate Row Detection")
        print("Business Question: Do we have duplicate records?")
        # Build column list for DISTINCT
        col_list = ", ".join(f'"{col}"' for col in p.columns)
        duplicates = p.sql(f"""
            SELECT
                COUNT(*) - COUNT(DISTINCT ({col_list})) as duplicate_count,
                COUNT(*) as total_rows,
                ROUND(100 * (COUNT(*) - COUNT(DISTINCT ({col_list}))) / COUNT(*), 2) as duplicate_pct
            FROM data
        """)
        show("Duplicate Summary", duplicates)

        dup_count = int(duplicates.iloc[0]["duplicate_count"])
        if dup_count > 0:
            print(f"\n⚠️  WARNING: Found {dup_count} duplicate rows")
            print("\nShowing sample duplicate rows (first 5 groups):")
            # Build GROUP BY clause with all columns
            group_by_list = ", ".join(f'"{col}"' for col in p.columns)
            sample_dups = p.sql(f"""
                SELECT *, COUNT(*) as occurrence_count
                FROM data
                GROUP BY {group_by_list}
                HAVING COUNT(*) > 1
                LIMIT 5
            """)
            if not sample_dups.empty:
                show("Sample Duplicate Groups", sample_dups)
        else:
            print("\n✓ No duplicate rows found")

        # ── Check 4: Data Type Consistency ───────────────────────
        print("\n### Check 4: Data Type Patterns")
        print("Business Question: Are data types consistent?")
        for col in p.columns[:5]:  # Check first 5 columns
            # Check if column looks numeric
            numeric_check = p.sql(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN "{col}" ~ '^[0-9]*\\\\.?[0-9]+$' THEN 1 END) as numeric_count,
                    COUNT(CASE WHEN "{col}" = '' THEN 1 END) as empty_count
                FROM data
            """)
            row = numeric_check.iloc[0]
            if row["numeric_count"] == row["total"]:
                print(f"  • '{col}': Appears to be fully numeric")

        # ── Check 5: Outlier Detection (Numeric Columns) ────────
        print("\n### Check 5: Outlier Detection (IQR Method)")
        print("Business Question: Do we have extreme values?")
        print("Method: Values outside Q1-1.5*IQR to Q3+1.5*IQR")

        # Try to find a numeric column
        for col in p.columns[:3]:  # Check first 3 columns
            if col in ["amount", "price", "quantity", "value", "total", "cost"]:
                outliers = p.sql(f"""
                    WITH stats AS (
                        SELECT
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY CAST("{col}" AS DOUBLE)) as q1,
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY CAST("{col}" AS DOUBLE)) as q3,
                            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CAST("{col}" AS DOUBLE)) as median
                        FROM data
                        WHERE "{col}" != '' AND "{col}" IS NOT NULL
                    ),
                    ranges AS (
                        SELECT
                            q1,
                            q3,
                            q3 - q1 as iqr,
                            q1 - 1.5 * (q3 - q1) as lower_bound,
                            q3 + 1.5 * (q3 - q1) as upper_bound,
                            median
                        FROM stats
                    )
                    SELECT
                        COUNT(*) as total_values,
                        SUM(CASE WHEN CAST("{col}" AS DOUBLE) < lower_bound THEN 1 END) as low_outliers,
                        SUM(CASE WHEN CAST("{col}" AS DOUBLE) > upper_bound THEN 1 END) as high_outliers,
                        ROUND(MAX(median), 2) as median,
                        ROUND(MAX(lower_bound), 2) as lower_bound,
                        ROUND(MAX(upper_bound), 2) as upper_bound
                    FROM data, ranges
                    WHERE "{col}" != '' AND "{col}" IS NOT NULL
                """)
                show(f"Outlier Analysis for '{col}'", outliers)

                outlier_count = outliers.iloc[0]["low_outliers"] + outliers.iloc[0]["high_outliers"]
                if outlier_count > 0:
                    print(f"\n⚠️  Found {outlier_count} outliers in '{col}'")
                    print(f"  Range: [{outliers.iloc[0]['lower_bound']}, {outliers.iloc[0]['upper_bound']}]")
                break

        # ── Check 6: Value Distribution (Categorical Columns) ────
        print("\n### Check 6: Categorical Value Distribution")
        print("Business Question: What are the most common values?")
        for col in p.columns[:3]:  # Check first 3 columns
            if col not in ["amount", "price", "quantity", "date", "timestamp"]:
                dist = p.sql(f"""
                    SELECT
                        "{col}" as value,
                        COUNT(*) as count,
                        ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
                    FROM data
                    WHERE "{col}" != '' AND "{col}" IS NOT NULL
                    GROUP BY "{col}"
                    ORDER BY count DESC
                    LIMIT 5
                """)
                show(f"Top 5 Values in '{col}'", dist)

        # ── Check 7: Data Freshness (if date column exists) ─────
        date_col = None
        for col in p.columns:
            if col.lower() in ["date", "timestamp", "created_at", "updated_at"]:
                date_col = col
                break

        if date_col:
            print(f"\n### Check 7: Data Freshness ('{date_col}')")
            print("Business Question: How current is our data?")
            freshness = p.sql(f"""
                SELECT
                    MIN({date_col}) as earliest_date,
                    MAX({date_col}) as latest_date,
                    CURRENT_DATE - MAX(CAST({date_col} AS DATE)) as days_old
                FROM data
            """)
            show("Data Freshness", freshness)

        # ── Summary ────────────────────────────────────────────
        print("\n" + "=" * 58)
        print("### Data Quality Summary")
        print("=" * 58)
        print(f"✓ Total Records: {total_rows:,}")
        print(f"✓ Total Columns: {total_cols}")
        print(f"{'⚠️' if dup_count > 0 else '✓'} Duplicates: {dup_count}")
        print(f"{'⚠️' if not poor_coverage.empty else '✓'} Columns with poor coverage: {len(poor_coverage)}")
        print("\n### Recommendations")

        if dup_count > 0:
            print("  1. Remove or investigate duplicate rows")

        if not poor_coverage.empty:
            print("  2. Impute or clean columns with missing values")

        print("  3. Validate outlier values before analysis")
        print("  4. Establish data quality monitoring alerts")
