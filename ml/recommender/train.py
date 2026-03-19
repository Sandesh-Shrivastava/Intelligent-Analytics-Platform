"""
Product Category Recommender
Builds a simple collaborative filtering recommender using
customer purchase history by category.

Run:
    python ml/recommender/train.py
"""

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mlflow
from sklearn.metrics.pairwise import cosine_similarity

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from notebooks.athena_helper import query

OUTPUT_DIR = Path("ml/outputs/recommender")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_purchase_data() -> pd.DataFrame:
    print("Pulling purchase data from Athena...")
    df = query("""
        select
            o.customer_unique_id,
            i.product_category,
            count(*)                as purchase_count,
            sum(i.total_amount)     as total_spent
        from dbt_dev.int_order_items_enriched i
        join dbt_dev.fct_orders o on i.order_id = o.order_id
        where o.status = 'delivered'
          and i.product_category is not null
        group by o.customer_unique_id, i.product_category
    """)
    print(f"  Loaded {len(df):,} customer-category pairs")
    print(f"  Unique customers  : {df['customer_unique_id'].nunique():,}")
    print(f"  Unique categories : {df['product_category'].nunique():,}")
    return df


def build_user_item_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Build customer x category matrix with purchase counts."""
    matrix = df.pivot_table(
        index="customer_unique_id",
        columns="product_category",
        values="purchase_count",
        fill_value=0
    )
    print(f"\n  Matrix shape: {matrix.shape[0]:,} customers x {matrix.shape[1]} categories")
    return matrix


def get_similar_customers(
    matrix: pd.DataFrame,
    customer_id: str,
    n: int = 5
) -> pd.DataFrame:
    """Find n most similar customers using cosine similarity."""
    if customer_id not in matrix.index:
        return pd.DataFrame()

    sim_matrix = cosine_similarity(matrix)
    sim_df     = pd.DataFrame(sim_matrix, index=matrix.index, columns=matrix.index)

    similar = (
        sim_df[customer_id]
        .drop(customer_id)
        .sort_values(ascending=False)
        .head(n)
    )
    return similar


def recommend_categories(
    matrix: pd.DataFrame,
    customer_id: str,
    n_recommendations: int = 3
) -> list[str]:
    """Recommend categories the customer hasn't bought yet."""
    if customer_id not in matrix.index:
        return []

    # get similar customers
    similar = get_similar_customers(matrix, customer_id, n=10)
    if similar.empty:
        return []

    # categories this customer already bought
    already_bought = set(matrix.columns[matrix.loc[customer_id] > 0])

    # weighted sum of similar customers' purchases
    weighted = matrix.loc[similar.index].multiply(similar.values, axis=0).sum()

    # remove already bought, sort by score
    recommendations = (
        weighted
        .drop(list(already_bought), errors="ignore")
        .sort_values(ascending=False)
        .head(n_recommendations)
        .index.tolist()
    )
    return recommendations


def plot_category_popularity(df: pd.DataFrame) -> None:
    top = (
        df.groupby("product_category")["purchase_count"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top.index, top.values, color="steelblue")
    ax.set_title("Top 15 Categories by Purchase Count")
    ax.set_xlabel("Total Purchases")
    ax.invert_yaxis()
    plt.tight_layout()
    path = OUTPUT_DIR / "category_popularity.png"
    plt.savefig(path)
    print(f"  ✓ Saved category_popularity.png")


def train():
    print("=" * 55)
    print("  Product Category Recommender")
    print("=" * 55)

    df     = load_purchase_data()
    matrix = build_user_item_matrix(df)

    mlflow.set_experiment("recommender")

    with mlflow.start_run(run_name="cosine_similarity"):

        # test on a sample of customers
        sample_customers = matrix.index[:5].tolist()

        print(f"\n📋 Sample Recommendations")
        print("-" * 50)

        results = []
        for cid in sample_customers:
            bought  = list(matrix.columns[matrix.loc[cid] > 0])
            recs    = recommend_categories(matrix, cid, n_recommendations=3)
            results.append({
                "customer_id":     cid[:12] + "...",
                "bought":          ", ".join(bought[:3]),
                "recommendations": ", ".join(recs) if recs else "none",
            })
            print(f"\n  Customer : {cid[:12]}...")
            print(f"  Bought   : {', '.join(bought[:3])}")
            print(f"  Recommend: {', '.join(recs) if recs else 'none'}")

        # save results
        results_df = pd.DataFrame(results)
        path = OUTPUT_DIR / "sample_recommendations.csv"
        results_df.to_csv(path, index=False)
        mlflow.log_artifact(str(path))
        print(f"\n  ✓ Saved sample_recommendations.csv")

        # save full matrix
        path = OUTPUT_DIR / "user_item_matrix.csv"
        matrix.to_csv(path)
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved user_item_matrix.csv")

        # log metrics
        mlflow.log_metric("n_customers",  matrix.shape[0])
        mlflow.log_metric("n_categories", matrix.shape[1])
        mlflow.log_metric("matrix_sparsity",
            (matrix == 0).sum().sum() / matrix.size)

        # plot
        plot_category_popularity(df)

        print(f"\n✅ MLflow run logged.")
        print(f"\n✓ All outputs saved to '{OUTPUT_DIR}/'")

        return matrix


if __name__ == "__main__":
    train()