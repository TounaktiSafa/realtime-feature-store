"""Real-time login scoring service.   Run:  uvicorn api:app --port 8000

POST /score  ->  Redis (Feast online store) -> signals.py -> @champion XGBoost -> score
"""
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.xgboost
import pandas as pd
from fastapi import FastAPI
from feast import FeatureStore
from pydantic import BaseModel

from signals import IP_REFS, MODEL_FEATURES, USER_REFS, build_signals

MODEL_NAME, ALIAS = "login_fraud", "champion"
SERVING_LOG = Path("data/serving_log/requests.jsonl")   # what Evidently will read for drift
S = {}                                                    # model, store, threshold, version
_log_lock = threading.Lock()


def load():
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    mv = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, ALIAS)
    S["model"] = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@{ALIAS}")
    S["version"] = mv.version
    S["threshold"] = float(mv.tags["threshold"])
    S["store"] = FeatureStore("feature_repo")
    SERVING_LOG.parent.mkdir(parents=True, exist_ok=True)


load()  # at import time, before the web server's event loop starts (loading inside it hangs Feast)
app = FastAPI(title="Login fraud scoring")


class Login(BaseModel):
    user_id: str
    ip: str
    country: str
    device_id: str
    event_time: datetime | None = None   # default: now. Pass it to replay history.


@app.get("/health")
def health():
    S["store"].get_online_features(features=IP_REFS, entity_rows=[{"ip": "health_probe"}])  # Redis reachable?
    return {"status": "ok", "model_version": S["version"], "threshold": S["threshold"]}


@app.post("/score")
def score(login: Login):
    t0 = time.perf_counter()
    ts = login.event_time or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    # one Redis round-trip: user features + IP features
    row = S["store"].get_online_features(
        features=USER_REFS + IP_REFS,
        entity_rows=[{"user_id": login.user_id, "ip": login.ip}],
    ).to_df().drop(columns=["user_id", "ip"])
    row["req_ts"], row["req_country"], row["req_device"] = ts.timestamp(), login.country, login.device_id

    X = build_signals(row)                                    # same code as training
    p = float(S["model"].predict_proba(X[MODEL_FEATURES])[0, 1])
    out = {
        "score": round(p, 4),
        "flagged": p >= S["threshold"],
        "threshold": S["threshold"],
        "model_version": S["version"],
        "features": {k: float(v) for k, v in X.iloc[0].items()},
        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
    }
    with _log_lock:
        with SERVING_LOG.open("a") as f:
            f.write(json.dumps({"ts": ts.isoformat(), "user_id": login.user_id, "ip": login.ip,
                                **out["features"], "score": out["score"], "flagged": out["flagged"],
                                "model_version": out["model_version"]}) + "\n")
    return out
