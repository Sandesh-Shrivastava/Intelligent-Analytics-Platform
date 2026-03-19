"""
Churn Model — XGBoost + SHAP Explainability
Trains a binary classifier to predict customer churn.
Logs everything to MLflow.

Definition: churned = no order in last 180 days (is_churned = 1)

Run:
    python ml/churn/train.py
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
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
    ConfusionMatrixDisplay,
)
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ml.features import load_features

OUTPUT_DIR = Path("ml/outputs/churn")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── Model Config ──────────────────────────────────────────────────────────────

PARAMS = {
    "n_estimators":     200,
    "max_depth":        4,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": 2,      # handles class imbalance
    "random_state":     42,
    "eval_metric":      "auc",
}


# ── Training ──────────────────────────────────────────────────────────────────

def train():
    print("=" * 55)
    print("  Churn Model — XGBoost")
    print("=" * 55)

    # load features
    X, y, df = load_features()

    # train/test split — stratified to preserve churn ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"\n  Train: {len(X_train):,} | Test: {len(X_test):,}")
    print(f"  Churn in train: {y_train.mean():.1%} | test: {y_test.mean():.1%}")

    # MLflow experiment
    mlflow.set_experiment("churn_prediction")

    with mlflow.start_run(run_name="xgboost_baseline"):

        # train model
        model = XGBClassifier(**PARAMS)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        # ── Evaluate ─────────────────────────────────────────────────────────
        y_pred      = model.predict(X_test)
        y_pred_prob = model.predict_proba(X_test)[:, 1]

        auc    = roc_auc_score(y_test, y_pred_prob)
        report = classification_report(y_test, y_pred, output_dict=True)

        print(f"\n📊 Model Performance")
        print(f"  ROC-AUC  : {auc:.4f}")
        print(f"  Precision: {report['1']['precision']:.4f}")
        print(f"  Recall   : {report['1']['recall']:.4f}")
        print(f"  F1-Score : {report['1']['f1-score']:.4f}")

        # cross validation
        cv     = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_auc = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
        print(f"\n  CV AUC   : {cv_auc.mean():.4f} ± {cv_auc.std():.4f}")

        # ── Log to MLflow ─────────────────────────────────────────────────────
        mlflow.log_params(PARAMS)
        mlflow.log_metric("auc",           auc)
        mlflow.log_metric("cv_auc_mean",   cv_auc.mean())
        mlflow.log_metric("cv_auc_std",    cv_auc.std())
        mlflow.log_metric("precision",     report['1']['precision'])
        mlflow.log_metric("recall",        report['1']['recall'])
        mlflow.log_metric("f1",            report['1']['f1-score'])
        mlflow.xgboost.log_model(model, "model")

        # ── Confusion Matrix ──────────────────────────────────────────────────
        cm  = confusion_matrix(y_test, y_pred)
        fig, ax = plt.subplots(figsize=(6, 5))
        ConfusionMatrixDisplay(cm, display_labels=["Active", "Churned"]).plot(ax=ax)
        ax.set_title("Churn Model — Confusion Matrix")
        plt.tight_layout()
        path = OUTPUT_DIR / "confusion_matrix.png"
        plt.savefig(path)
        mlflow.log_artifact(str(path))
        print(f"\n  ✓ Saved confusion_matrix.png")

        # ── Feature Importance ────────────────────────────────────────────────
        importance = pd.Series(
            model.feature_importances_,
            index=X.columns
        ).sort_values(ascending=False).head(15)

        fig, ax = plt.subplots(figsize=(10, 6))
        importance.plot(kind="barh", ax=ax, color="steelblue")
        ax.set_title("Top 15 Feature Importances")
        ax.set_xlabel("Importance Score")
        ax.invert_yaxis()
        plt.tight_layout()
        path = OUTPUT_DIR / "feature_importance.png"
        plt.savefig(path)
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved feature_importance.png")

        # ── SHAP Explainability ───────────────────────────────────────────────
        print(f"\n🔍 Computing SHAP values...")
        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        # SHAP summary plot — shows which features drive churn most
        fig, ax = plt.subplots(figsize=(10, 8))
        shap.summary_plot(
            shap_values, X_test,
            plot_type="bar",
            show=False,
            max_display=15,
        )
        plt.title("SHAP Feature Importance — Churn Model")
        plt.tight_layout()
        path = OUTPUT_DIR / "shap_importance.png"
        plt.savefig(path, bbox_inches="tight")
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved shap_importance.png")

        # SHAP beeswarm plot — shows direction of impact
        fig, ax = plt.subplots(figsize=(10, 8))
        shap.summary_plot(
            shap_values, X_test,
            show=False,
            max_display=15,
        )
        plt.title("SHAP Beeswarm — Feature Impact on Churn")
        plt.tight_layout()
        path = OUTPUT_DIR / "shap_beeswarm.png"
        plt.savefig(path, bbox_inches="tight")
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved shap_beeswarm.png")

        # ── Save predictions ──────────────────────────────────────────────────
        predictions = pd.DataFrame({
            "customer_unique_id": df.loc[X_test.index, "customer_unique_id"],
            "actual_churned":     y_test.values,
            "predicted_churned":  y_pred,
            "churn_probability":  y_pred_prob.round(4),
        }).sort_values("churn_probability", ascending=False)

        path = OUTPUT_DIR / "predictions.csv"
        predictions.to_csv(path, index=False)
        mlflow.log_artifact(str(path))
        print(f"  ✓ Saved predictions.csv")

        print(f"\n✅ MLflow run logged.")
        print(f"   View with: mlflow ui")
        print(f"\n✓ All outputs saved to '{OUTPUT_DIR}/'")

        return model, auc


if __name__ == "__main__":
    train()