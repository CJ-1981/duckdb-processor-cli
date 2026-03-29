"""Python Machine Learning Data Preparation.

This analyst demonstrates how to prepare data for Machine Learning models
using pure Python (pandas).
- Handling Missing Values (Imputation)
- Feature Scaling (MinMax & Standardization)
- Categorical Encoding (One-Hot)
- Train/Test Splits

Run via::
    python -m duckdb_processor data.csv --run python_ml_prep
"""
import pandas as pd
import numpy as np
from duckdb_processor.analyzer import BaseAnalyzer, register


@register
class PythonMlPrep(BaseAnalyzer):
    """Machine learning data preparation using pandas."""

    name = "python_ml_prep"
    description = "Python ML Prep: Feature Engineering, Imputation, One-Hot Encoding"

    def run(self, p):
        """Execute ML data preparation patterns."""
        print("\n### Machine Learning Data Preparation (Python)")
        print("=" * 58)
        print("This module demonstrates standard data cleaning and feature")
        print("engineering patterns required before training ML models.")
        print("=" * 58)

        def show(title, df):
            print(f"\n── {title} ─────────────────────────────────────")
            if p.formatter:
                p.formatter.format_dataframe(df.head(10))
            else:
                print(df.head(10).to_string(index=False))

        # ── Setup: Load Data ─────────────────────────────────────────────────
        df = p.sql("SELECT * FROM data")
        
        # Make a copy so we don't modify the original during our operations
        ml_df = df.copy()

        # Group data by type iteratively
        numeric_cols = [c for c in ml_df.columns if pd.api.types.is_numeric_dtype(ml_df[c])]
        categorical_cols = [c for c in ml_df.columns if pd.api.types.is_object_dtype(ml_df[c])]

        if not numeric_cols:
            print("Skipping: ML Prep requires numeric data features.")
            return

        # ── Pattern 1: Missing Value Imputation ──────────────────────────────
        print("\n### Pattern 1: Missing Data Imputation")
        # Introduce artificial nulls for demonstration
        if len(ml_df) > 5:
            ml_df.loc[ml_df.index[2], numeric_cols[0]] = np.nan
        
        # Create an imputed copy
        imputed_df = ml_df.copy()
        for col in numeric_cols:
            # Impute numbers with Median
            col_median = imputed_df[col].median()
            imputed_df[col] = imputed_df[col].fillna(col_median)
            
        for col in categorical_cols:
            # Impute strings with Most Frequent Category (Mode)
            col_mode = imputed_df[col].mode()[0] if not imputed_df[col].mode().empty else 'Unknown'
            imputed_df[col] = imputed_df[col].fillna(col_mode)

        show("Imputed Feature Dataset (fillna with median/mode)", imputed_df[numeric_cols + categorical_cols])

        # ── Pattern 2: Categorical Encoding (One-Hot) ────────────────────────
        print("\n### Pattern 2: One-Hot Encoding Categories (Dummy Variables)")
        if categorical_cols:
            # Drop columns with too many unique values to prevent high cardinality explosions
            low_card_cats = [col for col in categorical_cols if ml_df[col].nunique() < 10]
            
            if low_card_cats:
                # pd.get_dummies turns categories into 0/1 binary columns
                encoded_df = pd.get_dummies(imputed_df, columns=low_card_cats, drop_first=True)
                show("One-Hot Encoded Dataset", encoded_df)
            else:
                print(f"Skipping: Too many unique values in categorical columns. High cardinality risk.")
        else:
            print("Skipping: No categorical columns found.")
            encoded_df = imputed_df.copy()

        # ── Pattern 3: Feature Scaling (MinMax & Standardization) ────────────
        print("\n### Pattern 3: Feature Scaling (Standardization)")
        num_col = numeric_cols[0]
        
        scaled_df = encoded_df.copy()
        
        # Min-Max Scaling (Scale to 0 - 1)
        # Formula: (X - Min) / (Max - Min)
        col_min = scaled_df[num_col].min()
        col_max = scaled_df[num_col].max()
        scaled_df[f"{num_col}_minmax"] = (scaled_df[num_col] - col_min) / (col_max - col_min)

        # Z-Score Standardization (Scale to Mean 0, Std Dev 1)
        # Formula: (X - Mean) / StdDev
        col_mean = scaled_df[num_col].mean()
        col_std = scaled_df[num_col].std()
        scaled_df[f"{num_col}_standardized"] = (scaled_df[num_col] - col_mean) / col_std

        show(f"Scaled Features for column '{num_col}'", scaled_df[[num_col, f"{num_col}_minmax", f"{num_col}_standardized"]])

        # ── Pattern 4: Target Variable & Correlation Map ─────────────────────
        print("\n### Pattern 4: Feature Correlation")
        # Ensure we're only looking at numeric data
        final_numeric_cols = [c for c in scaled_df.columns if pd.api.types.is_numeric_dtype(scaled_df[c])]
        
        if len(final_numeric_cols) > 1:
            # Compute correlation matrix
            # Use spearman for rank correlation or pearson for linear correlation
            corr_matrix = scaled_df[final_numeric_cols].corr(method='pearson')
            
            # Since correlation matrices are identical on diagonal, display head
            show("Pearson Correlation Matrix (Feature-to-Feature)", corr_matrix)

        print("\n### Summary of ML Engineering Steps:")
        print("1. Fill missing numeric values with .median() to avoid outlier skew.")
        print("2. Encode categories using `pd.get_dummies(..., drop_first=True)` to prevent multicollinearity dummy traps.")
        print("3. Always scale models (Standardization) to allow gradient descents to converge faster.")
