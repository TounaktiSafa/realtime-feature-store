"""Why does the drift monitor flag features on normal traffic?
Compares the live window with the training set, feature by feature, and prints plain numbers.
Run from ~/login-events (venv active):   python -m diagnostics.compare_distributions
"""
import json
from collections import deque
from pathlib import Path

import pandas as pd

from common.signals import MODEL_FEATURES

TRAIN = Path("data/training_set.parquet")
LOG = Path("data/serving_log/requests.jsonl")
WINDOW = 300
LABELS = ("is_fraud", "label", "fraud", "y")

train_full = pd.read_parquet(TRAIN)
label = next((c for c in LABELS if c in train_full.columns), None)

rows = deque(maxlen=WINDOW)
for line in LOG.open():
    try:
        rows.append(json.loads(line))
    except json.JSONDecodeError:
        pass
live = pd.DataFrame(list(rows))

print(f"training rows: {len(train_full)} | live rows: {len(live)}")
if label:
    print(f"fraud share in training set: {train_full[label].mean():.1%}")
else:
    print(f"(no label column found among {LABELS}; columns: {list(train_full.columns)[:12]})")
print(f"share flagged by the model in the live window: {live['flagged'].mean():.1%}\n")

def stats(df, f):
    s = df[f].astype(float)
    return s.mean(), s.median(), (s == 0).mean()

hdr = f"{'feature':24}{'train mean':>12}{'live mean':>12} | {'train median':>12}{'live median':>12} | {'train %zero':>11}{'live %zero':>11}"
print(hdr); print("-" * len(hdr))
for f in MODEL_FEATURES:
    tm, tmed, tz = stats(train_full, f)
    lm, lmed, lz = stats(live, f)
    print(f"{f:24}{tm:12.3g}{lm:12.3g} | {tmed:12.3g}{lmed:12.3g} | {tz:11.1%}{lz:11.1%}")

if label:
    normal = train_full[train_full[label] == 0]
    print("\nSame table, but training restricted to NORMAL (non-fraud) rows only:")
    print(hdr); print("-" * len(hdr))
    for f in MODEL_FEATURES:
        tm, tmed, tz = stats(normal, f)
        lm, lmed, lz = stats(live, f)
        print(f"{f:24}{tm:12.3g}{lm:12.3g} | {tmed:12.3g}{lmed:12.3g} | {tz:11.1%}{lz:11.1%}")
