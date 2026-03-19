"""
03 - RFM Segmentation
Reads pre-computed RFM scores from dim_customers and
visualises customer segments.

Run:
    python notebooks/03_rfm_segmentation.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")

from athena_helper import query

OUTPUT_DIR = "notebooks/outputs"
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_rfm_data() -> pd.DataFrame:
    return query("""
        select
            customer_unique_id,
            recency_days,
            total_orders,
            total_revenue,
            recency_score,
            frequency_score,
            monetary_score,
            customer_segment,
            customer_state
        from dbt_dev.dim_customers
    """)


def print_segment_summary(df: pd.DataFrame):
    summary = (
        df.groupby("customer_segment")
        .agg(
            customers   =("customer_unique_id", "count"),
            avg_revenue =("total_revenue", "mean"),
            avg_orders  =("total_orders", "mean"),
            avg_recency =("recency_days", "mean"),
        )
        .round(2)
        .sort_values("customers", ascending=False)
        .reset_index()
    )

    print("\n👥 Customer Segments")
    print("-" * 70)
    print(summary.to_string(index=False))
    return summary


def plot_segment_distribution(df: pd.DataFrame):
    counts = df["customer_segment"].value_counts()

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # pie chart
    axes[0].pie(
        counts.values,
        labels=counts.index,
        autopct="%1.1f%%",
        startangle=140
    )
    axes[0].set_title("Customer Segment Distribution")

    # bar chart — avg revenue per segment
    summary = df.groupby("customer_segment")["total_revenue"].mean().sort_values(ascending=False)
    axes[1].bar(summary.index, summary.values, color="steelblue")
    axes[1].set_title("Avg Revenue by Segment")
    axes[1].set_xlabel("Segment")
    axes[1].set_ylabel("Avg Revenue ($)")
    plt.xticks(rotation=30)

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/rfm_segments.png", dpi=150)
    print(f"\n  ✓ Saved rfm_segments.png")


def plot_rfm_scatter(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 7))

    segments   = df["customer_segment"].unique()
    colors     = plt.cm.tab10.colors
    color_map  = {seg: colors[i] for i, seg in enumerate(segments)}

    for seg in segments:
        subset = df[df["customer_segment"] == seg]
        ax.scatter(
            subset["recency_days"],
            subset["total_revenue"],
            label=seg,
            alpha=0.6,
            color=color_map[seg],
            s=30
        )

    ax.set_title("RFM Scatter — Recency vs Revenue by Segment")
    ax.set_xlabel("Recency (days since last order)")
    ax.set_ylabel("Total Revenue ($)")
    ax.legend(title="Segment")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/rfm_scatter.png", dpi=150)
    print(f"  ✓ Saved rfm_scatter.png")


if __name__ == "__main__":
    print("=" * 50)
    print("  RFM Segmentation")
    print("=" * 50)

    df = get_rfm_data()
    print(f"\n  Total customers: {len(df):,}")

    print_segment_summary(df)
    plot_segment_distribution(df)
    plot_rfm_scatter(df)

    print(f"\n✓ Done. Plots saved to '{OUTPUT_DIR}/'")