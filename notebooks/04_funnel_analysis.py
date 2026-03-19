"""
04 - Funnel Analysis
Analyses the order status funnel — how many orders
progress through each stage and where drop-offs happen.

Run:
    python notebooks/04_funnel_analysis.py
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")

from athena_helper import query

OUTPUT_DIR = "notebooks/outputs"
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_funnel_data() -> pd.DataFrame:
    return query("""
        select
            status,
            count(*)                                        as orders,
            round(avg(order_value), 2)                      as avg_order_value,
            round(avg(case when delivery_days is not null
                          then cast(delivery_days as double)
                          else null end), 2)                as avg_delivery_days
        from dbt_dev.fct_orders
        group by status
        order by orders desc
    """)


def get_delivery_time_distribution() -> pd.DataFrame:
    return query("""
        select
            delivery_days,
            count(*) as orders
        from dbt_dev.fct_orders
        where status = 'delivered'
          and delivery_days is not null
          and delivery_days between 1 and 60
        group by delivery_days
        order by delivery_days
    """)


def get_payment_type_breakdown() -> pd.DataFrame:
    return query("""
        select
            primary_payment_type            as payment_type,
            count(*)                        as orders,
            round(sum(order_value), 2)      as total_revenue,
            round(avg(max_installments), 1) as avg_installments
        from dbt_dev.fct_orders
        group by primary_payment_type
        order by orders desc
    """)


def plot_funnel(df: pd.DataFrame):
    # Define logical funnel order
    funnel_order = ["approved", "invoiced", "processing", "shipped", "delivered", "canceled"]
    df["order_rank"] = df["status"].map(
        {s: i for i, s in enumerate(funnel_order)}
    )
    df = df.sort_values("order_rank").dropna(subset=["order_rank"])

    print("\n🔽 Order Status Funnel")
    print("-" * 50)
    print(df[["status", "orders", "avg_order_value"]].rename(
        columns={"avg_order_value": "avg_order_value ($)"}).to_string(index=False))

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#F44336", "#607D8B"]
    bars = ax.barh(df["status"], df["orders"], color=colors[:len(df)])

    # add count labels
    for bar, val in zip(bars, df["orders"]):
        ax.text(bar.get_width() + 10, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=9)

    ax.set_title("Order Status Funnel")
    ax.set_xlabel("Number of Orders")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/order_funnel.png", dpi=150)
    print(f"\n  ✓ Saved order_funnel.png")


def plot_delivery_distribution(df: pd.DataFrame):
    print("\n🚚 Delivery Time Distribution")
    print("-" * 40)
    print(f"  Mean  : {(df['delivery_days'] * df['orders']).sum() / df['orders'].sum():.1f} days")
    print(f"  Median: {df.loc[df['orders'].cumsum() >= df['orders'].sum() / 2, 'delivery_days'].iloc[0]} days")

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(df["delivery_days"], df["orders"], color="steelblue", width=0.8)
    ax.set_title("Delivery Time Distribution")
    ax.set_xlabel("Days to Deliver")
    ax.set_ylabel("Number of Orders")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/delivery_distribution.png", dpi=150)
    print(f"  ✓ Saved delivery_distribution.png")


def plot_payment_breakdown(df: pd.DataFrame):
    print("\n💳 Payment Type Breakdown")
    print("-" * 50)
    print(df.to_string(index=False))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(df["payment_type"], df["orders"], color="steelblue")
    ax.set_title("Orders by Payment Type")
    ax.set_xlabel("Payment Type")
    ax.set_ylabel("Orders")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/payment_breakdown.png", dpi=150)
    print(f"\n  ✓ Saved payment_breakdown.png")


if __name__ == "__main__":
    print("=" * 50)
    print("  Funnel Analysis")
    print("=" * 50)

    funnel   = get_funnel_data()
    delivery = get_delivery_time_distribution()
    payments = get_payment_type_breakdown()

    plot_funnel(funnel)
    plot_delivery_distribution(delivery)
    plot_payment_breakdown(payments)

    print(f"\n✓ Done. All plots saved to '{OUTPUT_DIR}/'")