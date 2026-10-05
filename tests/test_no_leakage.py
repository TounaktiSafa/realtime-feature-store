"""Leakage tests. Run: pytest -q

The idea: rebuild each login's features a SECOND, independent way, from raw events that happened
strictly BEFORE it (later events are physically not in the data we pass in), and require the
training set to match exactly. If the pipeline peeked at the future, the numbers would differ.
"""
from bisect import bisect_left

import numpy as np
import pandas as pd
import pytest

from common.signals import MODEL_FEATURES, NEVER_SEEN

try:
    TS = pd.read_parquet("data/training_set.parquet")
    EV = pd.read_parquet("data/offline/login_events")
except FileNotFoundError:
    pytest.skip("run `python -m training.build_training_set` first", allow_module_level=True)

EV = EV.assign(t=EV.event_time.map(lambda x: x.timestamp())).sort_values("t").reset_index(drop=True)
BY_USER = {k: g for k, g in EV.groupby("user_id")}
BY_IP = {k: g for k, g in EV.groupby("ip")}


def features_from_past_only(r):
    t = r.event_time.timestamp()
    f = dict(is_new_device=1, country_changed=0, secs_since_user_last=NEVER_SEEN, user_failed_5m=0,
             user_logins_1h=0, ip_attempts_5m=0, ip_failed_5m=0, ip_distinct_users_5m=0)

    g = BY_USER[r.user_id]
    past = g.iloc[: bisect_left(g.t.values, t)]            # strictly earlier events only
    if len(past):
        prev = past.iloc[-1]
        gap = t - prev.t
        devices = []
        for d in past.device_id:                             # last 10 distinct devices
            if d in devices:
                devices.remove(d)
            devices = (devices + [d])[-10:]
        f["is_new_device"] = int(r.device_id not in devices)
        f["country_changed"] = int(prev.country != r.country)
        f["secs_since_user_last"] = gap
        if gap <= 300:
            f["user_failed_5m"] = int(((past.t > prev.t - 300) & (past.success == 0)).sum())
        if gap <= 3600:
            f["user_logins_1h"] = int((past.t > prev.t - 3600).sum())

    g = BY_IP[r.ip]
    past = g.iloc[: bisect_left(g.t.values, t)]
    if len(past):
        prev = past.iloc[-1]
        if t - prev.t <= 300:
            w = past[past.t > prev.t - 300]
            f["ip_attempts_5m"] = len(w)
            f["ip_failed_5m"] = int((w.success == 0).sum())
            f["ip_distinct_users_5m"] = w.user_id.nunique()
    return f


def test_no_rows_dropped():
    assert len(TS) == len(EV) and TS.event_id.is_unique


def test_features_match_past_only_rebuild():
    pick = pd.concat([
        TS[TS.is_fraud == 1].sample(300, random_state=0),
        TS[TS.is_fraud == 0].sample(700, random_state=0),
    ])
    ev = EV.set_index("event_id")
    bad = []
    for _, row in pick.iterrows():
        exp = features_from_past_only(ev.loc[row.event_id])
        for k, v in exp.items():
            if not np.isclose(row[k], v, atol=1e-3):
                bad.append((row.event_id, k, row[k], v))
    assert not bad, f"{len(bad)} mismatches, e.g. {bad[:3]}"


def test_feature_time_is_strictly_before_login():
    seen = TS.secs_since_user_last < NEVER_SEEN
    assert (TS.loc[seen, "secs_since_user_last"] > 0).all()
