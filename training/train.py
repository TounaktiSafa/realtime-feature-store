"""Train XGBoost on the point-in-time training set and track everything in MLflow.

Split is by TIME (train on the past, test on the future) - a random split would leak.
"""
import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

from common.signals import MODEL_FEATURES


def split_by_time(df):
    df = df.sort_values("event_time").reset_index(drop=True)
    n = len(df)
    return df.iloc[: int(n * 0.7)], df.iloc[int(n * 0.7): int(n * 0.8)], df.iloc[int(n * 0.8):]


def fit_and_eval(df):
    tr, va, te = split_by_time(df)
    params = dict(
        n_estimators=400, max_depth=4, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
        eval_metric="logloss", early_stopping_rounds=30, tree_method="hist", random_state=42,
    )
    model = xgb.XGBClassifier(**params)
    model.fit(tr[MODEL_FEATURES], tr.is_fraud, eval_set=[(va[MODEL_FEATURES], va.is_fraud)], verbose=False)

    # pick the alert threshold on VALIDATION (best F1), then report on the untouched TEST slice
    pv = model.predict_proba(va[MODEL_FEATURES])[:, 1]
    prec, rec, thr = precision_recall_curve(va.is_fraud, pv)
    f1 = 2 * prec[:-1] * rec[:-1] / (prec[:-1] + rec[:-1] + 1e-9)
    threshold = float(thr[int(np.argmax(f1))])

    pt = model.predict_proba(te[MODEL_FEATURES])[:, 1]
    pred = pt >= threshold
    tp = int((pred & (te.is_fraud == 1)).sum())
    metrics = {
        "test_pr_auc": float(average_precision_score(te.is_fraud, pt)),
        "test_roc_auc": float(roc_auc_score(te.is_fraud, pt)),
        "test_precision": tp / max(int(pred.sum()), 1),
        "test_recall": tp / max(int((te.is_fraud == 1).sum()), 1),
        "threshold": threshold,
        "best_iteration": int(model.best_iteration), "n_train": len(tr), "n_test": len(te), "test_fraud_rate": float(te.is_fraud.mean()),
    }
    return model, params, metrics


def main():
    df = pd.read_parquet("data/training_set.parquet")
    model, params, metrics = fit_and_eval(df)

    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    mlflow.set_experiment("login_fraud")
    with mlflow.start_run(run_name="xgb_point_in_time"):
        mlflow.log_params(params)
        mlflow.log_param("features", ",".join(MODEL_FEATURES))
        mlflow.log_param("training_rows", len(df))
        mlflow.log_metrics(metrics)
        mlflow.log_artifact("common/signals.py")                   # exact feature logic used
        mlflow.log_artifact("feature_repo/features.py")     # exact Feast definitions used
        info = mlflow.xgboost.log_model(model, name="model", registered_model_name="login_fraud")

    client = mlflow.MlflowClient()
    version = info.registered_model_version
    client.set_registered_model_alias("login_fraud", "champion", version)
    client.set_model_version_tag("login_fraud", version, "threshold", f"{metrics['threshold']:.6f}")
    print({k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()})
    print(f"registered login_fraud v{version} as @champion")


if __name__ == "__main__":
    main()
