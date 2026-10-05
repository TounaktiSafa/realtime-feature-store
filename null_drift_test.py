"""Calibration: how often does the drift check cry wolf on data that is NOT drifted?
Takes random NORMAL (non-fraud) rows from the training set as fake "live" windows and runs the
same Evidently check as drift_job.py. Any feature flagged here is a false alarm by construction.
Run from ~/login-events (venv active):   python null_drift_test.py        (about 1-2 minutes)
"""
import warnings

import pandas as pd
from evidently import DataDefinition, Dataset, Report
from evidently.presets import DataDriftPreset

from signals import MODEL_FEATURES

warnings.filterwarnings("ignore")
CATEGORICAL = ["is_new_device", "country_changed"]
SIZES, TRIALS = (100, 300, 500), 12

train = pd.read_parquet("data/training_set.parquet")
ref = train[MODEL_FEATURES].astype(float).sample(3000, random_state=0)
normal = train[train["is_fraud"] == 0][MODEL_FEATURES].astype(float)
dd = DataDefinition(
    numerical_columns=[c for c in MODEL_FEATURES if c not in CATEGORICAL],
    categorical_columns=[c for c in MODEL_FEATURES if c in CATEGORICAL],
)

print(f"{TRIALS} random windows per size, taken from the training data itself (no real drift):\n")
for n in SIZES:
    flags = {c: 0 for c in MODEL_FEATURES}
    shares = []
    for t in range(TRIALS):
        cur = normal.sample(n, random_state=100 + t)
        snap = Report([DataDriftPreset()]).run(
            Dataset.from_pandas(cur, data_definition=dd), Dataset.from_pandas(ref, data_definition=dd))
        k = 0
        for m in snap.dict()["metrics"]:
            if m["config"]["type"].endswith("ValueDrift"):
                hit = float(m["value"]) >= m["config"]["threshold"]
                flags[m["config"]["column"]] += hit
                k += hit
        shares.append(k / len(MODEL_FEATURES))
    print(f"window {n:>3}: average drift share {sum(shares) / len(shares):.2f}  "
          f"(worst window {max(shares):.2f})")
    print("   false-alarm rate per feature: " +
          ", ".join(f"{c} {flags[c] / TRIALS:.0%}" for c in MODEL_FEATURES if flags[c]) or "none")
    print()
