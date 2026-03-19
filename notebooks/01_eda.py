"""
01 - Exploratory Data Analysis
Pulls data from Athena mart tables and prints key business metrics.

Run:
    python notebooks/01_eda.py
"""

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — saves plots as files

from athena_helper import query

OUTPUT_DIR = "notebooks/outputs"
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── 1. Overall KPIs ───────────────────────────────────────────────────────────

def print_kpis():
    df = query("""
        select
            count(distinct order_id)         as total_orders,
            count(distinct customer_unique_id) as unique_customers,
            round(sum(order_value), 2)        as total_revenue,
            round(avg(order_value), 2)        as avg_order_value,
            round(avg(review_score), 2)       as avg_review_score,
            sum(case when delivered_on_time = true
                then 1 else 0 end) * 100.0
                / count(*)                    as on_time_pct
        from dbt_dev.fct_orders
        where status = 'delivered'
    """)

    print("\n📊 Overall KPIs")
    print("-" * 40)
    for col in df.columns:
        print(f"  {col:<25} {df[col].iloc[0]}")


# ── 2. Orders by status ───────────────────────────────────────────────────────

def plot_orders_by_status():
    df = query("""
        select
            status,
            count(*) as order_count
        from dbt_dev.fct_orders
        group by status
        order by order_count desc
    """)

    print("\n📦 Orders by Status")
    print("-" * 40)
    print(df.to_string(index=False))

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(df["status"], df["order_count"], color="steelblue")
    ax.set_title("Orders by Status")
    ax.set_xlabel("Status")
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/orders_by_status.png")
    print(f"\n  ✓ Saved orders_by_status.png")


# ── 3. Revenue by month ───────────────────────────────────────────────────────

def plot_revenue_by_month():
    df = query("""
        select
            purchase_year,
            purchase_month,
            round(sum(order_value), 2)  as revenue,
            count(distinct order_id)    as orders
        from dbt_dev.fct_orders
        where status = 'delivered'
        group by purchase_year, purchase_month
        order by purchase_year, purchase_month
    """)

    df["period"] = df["purchase_year"].astype(str) + "-" + df["purchase_month"].astype(str).str.zfill(2)

    print("\n📅 Revenue by Month")
    print("-" * 40)
    print(df[["period", "revenue", "orders"]].to_string(index=False))

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(df["period"], df["revenue"], marker="o", color="steelblue")
    ax.set_title("Monthly Revenue")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue ($)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/revenue_by_month.png")
    print(f"\n  ✓ Saved revenue_by_month.png")


# ── 4. Top 10 product categories ─────────────────────────────────────────────

def plot_top_categories():
    df = query("""
        select
            product_category,
            count(distinct order_id)    as orders,
            round(sum(total_amount), 2) as revenue
        from dbt_dev.int_order_items_enriched
        group by product_category
        order by revenue desc
        limit 10
    """)

    print("\n🛍️  Top 10 Categories by Revenue")
    print("-" * 40)
    print(df.to_string(index=False))

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(df["product_category"], df["revenue"], color="steelblue")
    ax.set_title("Top 10 Categories by Revenue")
    ax.set_xlabel("Revenue ($)")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/top_categories.png")
    print(f"\n  ✓ Saved top_categories.png")


# ── 5. Orders by state ────────────────────────────────────────────────────────

def plot_orders_by_state():
    df = query("""
        select
            customer_state              as state,
            count(distinct order_id)    as orders,
            round(sum(order_value), 2)  as revenue
        from dbt_dev.fct_orders
        where status = 'delivered'
        group by customer_state
        order by orders desc
        limit 10
    """)

    print("\n🗺️  Top 10 States by Orders")
    print("-" * 40)
    print(df.to_string(index=False))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df["state"], df["orders"], color="steelblue")
    ax.set_title("Top 10 States by Orders")
    ax.set_xlabel("State")
    ax.set_ylabel("Orders")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/orders_by_state.png")
    print(f"\n  ✓ Saved orders_by_state.png")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("  EDA — Customer Analytics Platform")
    print("=" * 50)

    print_kpis()
    plot_orders_by_status()
    plot_revenue_by_month()
    plot_top_categories()
    plot_orders_by_state()

    print(f"\n✓ All plots saved to '{OUTPUT_DIR}/'")