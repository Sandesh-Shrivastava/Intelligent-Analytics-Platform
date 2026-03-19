"""
features.py
Pulls mart_customer_metrics from Athena and prepares
a clean feature matrix for ML models.

Usage:
    from ml.features import load_features
    X, y, df = load_features()
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

# allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))
from notebooks.athena_helper import query


# ── Feature definitions ───────────────────────────────────────────────────────

NUMERIC_FEATURES = [
    "recency_days",
    "total_orders",
    "total_items",
    "total_revenue",
    "avg_order_value",
    "max_order_value",
    "orders_per_month",
    "avg_review_score",
    "positive_review_rate",
    "avg_delivery_days",
    "on_time_rate",
    "recency_score",
    "frequency_score",
    "monetary_score",
]

CATEGORICAL_FEATURES = [
    "customer_state",
]

TARGET = "is_churned"


def load_features() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """
    Returns:
        X   — feature matrix (numeric + encoded categoricals)
        y   — target series (is_churned)
        df  — raw dataframe for reference
    """
    print("Pulling features from Athena...")
    df = query(f"""
        select
            customer_unique_id,
            {', '.join(NUMERIC_FEATURES)},
            {', '.join(CATEGORICAL_FEATURES)},
            {TARGET}
        from dbt_dev.mart_customer_metrics
    """)

    print(f"  Loaded {len(df):,} customers")
    print(f"  Churn rate: {df[TARGET].mean():.1%}")

    # ── Clean ─────────────────────────────────────────────────────────────────
    # fill nulls with median for numeric features
    for col in NUMERIC_FEATURES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].fillna(df[col].median())

    # encode categorical features
    df = pd.get_dummies(df, columns=CATEGORICAL_FEATURES, drop_first=True)

    # get all encoded feature columns
    encoded_cats = [c for c in df.columns if c.startswith("customer_state_")]
    all_features = NUMERIC_FEATURES + encoded_cats

    X = df[all_features].astype(float)
    y = df[TARGET].astype(int)

    print(f"  Feature matrix: {X.shape[0]:,} rows x {X.shape[1]} features")
    return X, y, df


if __name__ == "__main__":
    X, y, df = load_features()
    print("\nFeature summary:")
    print(X.describe().round(2).to_string())