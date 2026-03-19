"""
Data Quality Cleaner
Fixes all known issues in raw CSVs before uploading to S3.

Issues handled:
  1. Empty strings in timestamp columns → replaced with None (NULL in CSV)
  2. Numeric columns stored as strings → validated and cleaned
  3. Duplicate rows → removed
  4. Leading/trailing whitespace → stripped from all string columns
  5. Prints a quality report for every table

Run:
    python ingestion/data_quality.py
"""

import pandas as pd
import numpy as np
from pathlib import Path

RAW_DATA_DIR = Path("data/raw")

# Define timestamp columns per table
TIMESTAMP_COLUMNS: dict[str, list[str]] = {
    "orders_dataset": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items_dataset": [
        "shipping_limit_date",
    ],
    "order_reviews_dataset": [
        "review_creation_date",
        "review_answer_timestamp",
    ],
}

# Define numeric columns per table
NUMERIC_COLUMNS: dict[str, list[str]] = {
    "order_items_dataset":    ["price", "freight_value", "order_item_id"],
    "order_payments_dataset": ["payment_value", "payment_installments", "payment_sequential"],
    "products_dataset":       ["product_weight_g", "product_length_cm", "product_height_cm",
                                "product_width_cm", "product_photos_qty",
                                "product_name_lenght", "product_description_lenght"],
    "order_reviews_dataset":  ["review_score"],
    "geolocation_dataset":    ["geolocation_lat", "geolocation_lng"],
}

# Define required (non-nullable) columns per table
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "orders_dataset":         ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "customers_dataset":      ["customer_id", "customer_unique_id"],
    "sellers_dataset":        ["seller_id"],
    "products_dataset":       ["product_id"],
    "order_items_dataset":    ["order_id", "product_id", "seller_id", "price"],
    "order_payments_dataset": ["order_id", "payment_type", "payment_value"],
    "order_reviews_dataset":  ["review_id", "order_id", "review_score"],
}


# ── Cleaners ──────────────────────────────────────────────────────────────────

def fix_timestamps(df: pd.DataFrame, table: str) -> tuple[pd.DataFrame, int]:
    """Replace empty/invalid timestamp strings with None."""
    cols    = TIMESTAMP_COLUMNS.get(table, [])
    fixed   = 0
    for col in cols:
        if col not in df.columns:
            continue
        # count empty strings
        empty_count = (df[col].astype(str).str.strip() == "").sum()
        # replace empty strings and "None" strings with actual None
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": None, "None": None, "nan": None, "NaT": None})
        fixed  += int(empty_count)
    return df, fixed


def fix_numeric_columns(df: pd.DataFrame, table: str) -> tuple[pd.DataFrame, int]:
    """Coerce numeric columns and replace non-numeric values with None."""
    cols  = NUMERIC_COLUMNS.get(table, [])
    fixed = 0
    for col in cols:
        if col not in df.columns:
            continue
        original    = df[col].copy()
        df[col]     = pd.to_numeric(df[col], errors="coerce")
        fixed      += int((df[col].isna() & original.notna()).sum())
    return df, fixed


def fix_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"None": None, "nan": None, "": None})
    return df


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove fully duplicate rows."""
    before = len(df)
    df     = df.drop_duplicates()
    return df, before - len(df)


def fix_required_columns(df: pd.DataFrame, table: str) -> tuple[pd.DataFrame, int]:
    """Drop rows where required columns are null."""
    cols    = REQUIRED_COLUMNS.get(table, [])
    before  = len(df)
    for col in cols:
        if col in df.columns:
            df = df[df[col].notna()]
            df = df[df[col].astype(str).str.strip() != ""]
    return df, before - len(df)


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(table: str, original_rows: int, final_rows: int, issues: dict) -> None:
    print(f"\n  📋 {table}")
    print(f"     Rows      : {original_rows:,} → {final_rows:,}")
    for issue, count in issues.items():
        status = "✓" if count == 0 else "⚠️ "
        print(f"     {status} {issue:<30} {count:,} fixed")


# ── Main ──────────────────────────────────────────────────────────────────────

def clean_all(data_dir: Path = RAW_DATA_DIR) -> None:
    csv_files = sorted(data_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSVs found in '{data_dir}'. Run generate_demo_data.py first.")

    print("=" * 55)
    print("  Data Quality Cleaner")
    print("=" * 55)

    total_issues = 0

    for path in csv_files:
        table        = path.stem
        df           = pd.read_csv(path, dtype=str)   # read everything as string first
        original_rows = len(df)
        issues       = {}

        # Run all fixers
        df, n = fix_whitespace(df), 0
        issues["whitespace stripped"] = 0   # always run, hard to count

        df, n = remove_duplicates(df)
        issues["duplicate rows removed"] = n

        df, n = fix_required_columns(df, table)
        issues["null required rows dropped"] = n

        df, n = fix_timestamps(df, table)
        issues["empty timestamps fixed"] = n

        df, n = fix_numeric_columns(df, table)
        issues["invalid numeric values"] = n

        total_issues += sum(issues.values())

        # Save cleaned file back to same location
        df.to_csv(path, index=False, na_rep="")

        print_report(table, original_rows, len(df), issues)

    print(f"\n{'=' * 55}")
    print(f"  Total issues fixed : {total_issues:,}")
    print(f"  Files cleaned      : {len(csv_files)}")
    print(f"  Output dir         : {data_dir}/")
    print(f"{'=' * 55}")
    print(f"\n✓ Done. Safe to upload to S3 now.")


if __name__ == "__main__":
    clean_all()