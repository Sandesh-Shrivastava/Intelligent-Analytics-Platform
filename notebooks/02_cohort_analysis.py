"""
02 - Cohort Analysis
Groups customers by their first purchase month and tracks
how many return in subsequent months.

Run:
    python notebooks/02_cohort_analysis.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
import seaborn as sns

from athena_helper import query

OUTPUT_DIR = "notebooks/outputs"
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_cohort_data() -> pd.DataFrame:
    return query("""
        with first_orders as (
            select
                customer_unique_id,
                cast(date_trunc('month', min(purchased_at)) as date) as cohort_month
            from dbt_dev.fct_orders
            where status = 'delivered'
            group by customer_unique_id
        )
        select
            f.customer_unique_id,
            cast(date_trunc('month', o.purchased_at) as date) as order_month,
            f.cohort_month
        from dbt_dev.fct_orders o
        join first_orders f
            on o.customer_unique_id = f.customer_unique_id
        where o.status = 'delivered'
    """)


def build_cohort_table(df: pd.DataFrame) -> pd.DataFrame:
    df["order_month"]  = pd.to_datetime(df["order_month"])
    df["cohort_month"] = pd.to_datetime(df["cohort_month"])

    # period index = months since first purchase
    df["period"] = (
        (df["order_month"].dt.year  - df["cohort_month"].dt.year) * 12 +
        (df["order_month"].dt.month - df["cohort_month"].dt.month)
    )

    # count unique customers per cohort + period
    cohort_data = (
        df.groupby(["cohort_month", "period"])["customer_unique_id"]
        .nunique()
        .reset_index()
    )

    # pivot to matrix
    cohort_table = cohort_data.pivot_table(
        index="cohort_month",
        columns="period",
        values="customer_unique_id"
    )

    # convert to retention % — divide each row by period 0 (cohort size)
    cohort_size   = cohort_table[0]
    retention_pct = cohort_table.divide(cohort_size, axis=0).round(4) * 100

    return retention_pct


def plot_cohort_heatmap(retention: pd.DataFrame):
    # format index as YYYY-MM
    retention.index = retention.index.strftime("%Y-%m")

    print("\n📅 Cohort Retention Table (% returning customers)")
    print("-" * 60)
    print(retention.to_string())

    fig, ax = plt.subplots(figsize=(14, 8))
    sns.heatmap(
        retention,
        annot=True,
        fmt=".0f",
        cmap="YlGnBu",
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "Retention %"}
    )
    ax.set_title("Monthly Cohort Retention (%)", fontsize=14)
    ax.set_xlabel("Months Since First Purchase")
    ax.set_ylabel("Cohort Month")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/cohort_retention.png", dpi=150)
    print(f"\n  ✓ Saved cohort_retention.png")


def print_insights(retention: pd.DataFrame):
    print("\n💡 Key Insights")
    print("-" * 40)

    avg_m1 = retention[1].mean() if 1 in retention.columns else 0
    avg_m3 = retention[3].mean() if 3 in retention.columns else 0

    print(f"  Avg Month-1 retention : {avg_m1:.1f}%")
    print(f"  Avg Month-3 retention : {avg_m3:.1f}%")

    best_cohort = retention[1].idxmax() if 1 in retention.columns else "N/A"
    print(f"  Best cohort (M1)      : {best_cohort}")


if __name__ == "__main__":
    print("=" * 50)
    print("  Cohort Analysis")
    print("=" * 50)

    df        = get_cohort_data()
    retention = build_cohort_table(df)

    plot_cohort_heatmap(retention)
    print_insights(retention)

    print(f"\n✓ Done. Plot saved to '{OUTPUT_DIR}/'")