"""
CLV Model — Customer Lifetime Value Regression
Predicts total_revenue for each customer using XGBoost regression.
Logs everything to MLflow.

Run:
    python ml/clv/train.py
"""

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shap
import mlflow
import mlflow.xgboost
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from notebooks.athena_helper import query

OUTPUT_DIR = Path("ml/outputs/clv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "recency_days",
    "total_orders",
    "total_items",
    "avg_order_value",
    "orders_per_month",
    "avg_review_score",
    "positive_review_rate",
    "avg_delivery_days",
    "on_time_rate",
    "recency_score",
    "frequency_score",
]

TARGET = "total_revenue"

PARAMS = {
    "n_estimators":     300,
    "max_depth":        5,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "random_state":     42,
}


def load_clv_data():
    print("Pulling CLV data from Athena...")
    df = query(f"""
        select
            customer_unique_id,
            {', '.join(FEATURE_COLS)},
            customer_state,
            {TARGET}
        from dbt_dev.mart_customer_metrics
        where total_revenue > 0
    """)

    print(f"  Loaded {len(df):,} customers")

    # clean numeric
    for col in FEATURE_COLS + [TARGET]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(df[col].median())

    # encode state
    df = pd.get_dummies(df, columns=["customer_state"], drop_first=True)
    encoded = [c for c in df.columns if c.startswith("customer_state_")]

    return df, FEATURE_COLS + encoded


def train():
    print("=" * 55)
    print("  CLV Model — XGBoost Regression")
    print("=" * 55)

    df, features = load_clv_data()

    X = df[features].astype(float)
    y = df[TARGET].astype(float)

    print(f"\n  Revenue stats:")
    print(f"  Mean  : R$ {y.mean():.2f}")
    print(f"  Median: R$ {y.median():.2f}")
    print(f"  Max   : R$ {y.max():.2f}")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    mlflow.set_experiment("clv_prediction")

    with mlflow.start_run(run_name="xgboost_regression"):

        model = XGBRegressor(**PARAMS)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)

        mae  = mean_absolute_error(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred) ** 0.5
        r2   = r2_score(y_test, y_pred)

        print(f"\n📊 Model Performance")
        print(f"  MAE  : R$ {mae:.2f}")
        print(f"  RMSE : R$ {rmse:.2f}")
        print(f"  R²   : {r2:.4f}")

        mlflow.log_params(PARAMS)
        mlflow.log_metric("mae",  mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2",   r2)
        mlflow.xgboost.log_model(model, "model")

        # ── Actual vs Predicted plot ──────────────────────────────────────────
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(y_test, y_pred, alpha=0.4, color="steelblue", s=20)
        ax.plot([y_test.min(), y_test.max()],
                [y_test.min(), y_test.max()], "r--", lw=2)
        ax.set_xlabel("Actual Revenue (R$)")
        ax.set_ylabel("Predicted Revenue (R$)")
        ax.set_title("CLV Model — Actual vs Predicted")
        plt.tight_layout()
        path = OUTPUT_DIR / "actual_vs_predicted.png"
        plt.savefig(path)
        mlflow.log_artifact(str(path))
        print(f"\n  ✓ Saved actual_vs_predicted.png")

        # ── SHAP ──────────────────────────────────────────────────────────────
        print(f"\n🔍 Computing SHAP values...")
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        fig, ax = plt.subplots(figsize=(10, 8))
        shap.summary_plot(shap_values, X_test, show=False, max_display=15)
        plt.title("SHAP — CLV Feature Impact")
        plt.tight_layout()
        path = OUTPUT_DIR / "shap_clv.png"
        plt.savefig(path, bbox_inches="tight")
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved shap_clv.png")

        # ── Predictions ───────────────────────────────────────────────────────
        predictions = pd.DataFrame({
            "customer_unique_id": df.loc[X_test.index, "customer_unique_id"],
            "actual_revenue":     y_test.values.round(2),
            "predicted_revenue":  y_pred.round(2),
            "error":              (y_pred - y_test.values).round(2),
        }).sort_values("predicted_revenue", ascending=False)

        path = OUTPUT_DIR / "predictions.csv"
        predictions.to_csv(path, index=False)
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved predictions.csv")

        print(f"\n✅ MLflow run logged.")
        print(f"\n✓ All outputs saved to '{OUTPUT_DIR}/'")

        return model, r2


if __name__ == "__main__":
    train()