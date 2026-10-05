"""Why point-in-time matters: same model, same data, three ways of looking up features.

Only the first row is legitimate. The others show how leakage breaks offline metrics.
"""
import pandas as pd

from build_training_set import build
from train import fit_and_eval

CASES = {
    "correct: features from just BEFORE the login": pd.Timedelta(microseconds=-1),
    "leaky: features include the login itself": pd.Timedelta(0),
    "leaky: features peek 5 min into the FUTURE": pd.Timedelta(minutes=5),
}

for name, offset in CASES.items():
    _, _, m = fit_and_eval(build(offset, tail=60_000))
    print(f"{name:48s} PR-AUC {m['test_pr_auc']:.3f} | precision {m['test_precision']:.3f} | recall {m['test_recall']:.3f}")
