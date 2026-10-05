"""Drift monitor: compares the last N scored logins with the training data (Evidently)
and exposes the results to Prometheus (Grafana reads Prometheus).

Run from the project root, in .venv-feast:
    python drift_job.py                      # loop forever, metrics on :8001/metrics
    python drift_job.py --once               # one check, write the HTML report, exit (good for testing)

Needs: pip install evidently==0.7.23 prometheus_client
"""
import argparse
import json
import time
from collections import deque
from pathlib import Path

import pandas as pd
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset
from prometheus_client import Gauge, start_http_server

from signals import MODEL_FEATURES

TRAIN = Path("data/training_set.parquet")
SERVING_LOG = Path("data/serving_log/requests.jsonl")
REPORT_DIR = Path("data/drift")
CATEGORICAL = ["is_new_device", "country_changed"]   # 0/1 flags; everything else is numeric

G_SHARE = Gauge("fraud_drift_share", "Share of model features that drifted (0-1)")
G_ROWS = Gauge("fraud_drift_window_rows", "Number of live logins in the comparison window")
G_DRIFT = Gauge("fraud_feature_drift_score", "Drift distance per feature (higher = more different)", ["feature"])
G_FLAG = Gauge("fraud_feature_drifted", "1 if the feature is flagged as drifted", ["feature"])
G_FLAGGED = Gauge("fraud_flagged_rate", "Share of live logins the model flagged as fraud")
G_SCORE = Gauge("fraud_mean_score", "Mean fraud score over the window")
G_CHECK = Gauge("fraud_drift_last_check_ts", "Unix time of the last successful drift check")


def tail(path, n):
    """Last n JSON lines of the serving log (ignores a half-written last line)."""
    rows = deque(maxlen=n)
    with path.open() as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return pd.DataFrame(list(rows))


def definition():
    return DataDefinition(
        numerical_columns=[c for c in MODEL_FEATURES if c not in CATEGORICAL],
        categorical_columns=[c for c in MODEL_FEATURES if c in CATEGORICAL],
    )


def check(ref, window, min_rows):
    if not SERVING_LOG.exists():
        print("no serving log yet, waiting for the first scored login")
        return
    live = tail(SERVING_LOG, window)
    if len(live) < min_rows:
        print(f"only {len(live)} live logins so far (need {min_rows}), waiting")
        return

    dd = definition()
    snap = Report([DataDriftPreset()]).run(
        Dataset.from_pandas(live[MODEL_FEATURES].astype(float), data_definition=dd),
        Dataset.from_pandas(ref, data_definition=dd),
    )
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    snap.save_html(str(REPORT_DIR / "latest.html"))

    for m in snap.dict()["metrics"]:
        cfg = m["config"]
        if cfg["type"].endswith("DriftedColumnsCount"):
            G_SHARE.set(m["value"]["share"])
        elif cfg["type"].endswith("ValueDrift"):
            col, score = cfg["column"], float(m["value"])
            G_DRIFT.labels(col).set(score)
            G_FLAG.labels(col).set(1 if score >= cfg["threshold"] else 0)

    G_ROWS.set(len(live))
    G_FLAGGED.set(float(live["flagged"].mean()))
    G_SCORE.set(float(live["score"].mean()))
    G_CHECK.set(time.time())
    drifted = [c for c in MODEL_FEATURES if G_FLAG.labels(c)._value.get() == 1]
    print(f"{len(live)} logins | drift share {G_SHARE._value.get():.2f} | drifted: {drifted or 'none'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--window", type=int, default=300, help="compare the last N scored logins")
    p.add_argument("--min-rows", type=int, default=100, help="don't judge drift on fewer logins than this")
    p.add_argument("--interval", type=int, default=30, help="seconds between checks")
    p.add_argument("--port", type=int, default=8001, help="Prometheus metrics port")
    p.add_argument("--once", action="store_true", help="one check, then exit")
    args = p.parse_args()

    ref = pd.read_parquet(TRAIN)[MODEL_FEATURES].astype(float).sample(3000, random_state=0)
    if not args.once:
        start_http_server(args.port)
        print(f"metrics on http://localhost:{args.port}/metrics")
    while True:
        try:
            check(ref, args.window, args.min_rows)
        except Exception as e:  # keep the monitor alive if one check fails
            print("check failed:", repr(e))
            if args.once:
                raise
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
