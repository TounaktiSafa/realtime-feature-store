"""Request-time signals. ONE function used by training AND serving -> no train/serve skew.

Input  : a DataFrame with the request columns (req_ts, req_country, req_device) plus the raw
         Feast features (NaN/None when that user or IP has never been seen before).
Output : the 8 columns the model is trained on (MODEL_FEATURES).
"""
import pandas as pd

USER_REFS = [
    "user_features:failed_logins_5m", "user_features:logins_1h", "user_features:last_country",
    "user_features:known_devices", "user_features:user_last_event_ts",
]
IP_REFS = [
    "ip_features:attempts_5m", "ip_features:failed_5m", "ip_features:distinct_users_5m",
    "ip_features:ip_last_event_ts",
]
MODEL_FEATURES = [
    "is_new_device", "country_changed", "secs_since_user_last", "user_failed_5m",
    "user_logins_1h", "ip_attempts_5m", "ip_failed_5m", "ip_distinct_users_5m",
]
NEVER_SEEN = 1e9  # "seconds since last event" for a user we have never seen
NUMERIC = ["failed_logins_5m", "logins_1h", "user_last_event_ts",
           "attempts_5m", "failed_5m", "distinct_users_5m", "ip_last_event_ts"]


def _is_new(device, known):
    try:
        return int(device not in list(known))
    except TypeError:  # known is None/NaN -> user never seen -> every device is new
        return 1


def build_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in NUMERIC:  # missing entity -> None/NaN; make them real floats first
        df[c] = pd.to_numeric(df[c], errors="coerce")
    t = df["req_ts"]
    u_gap = t - df["user_last_event_ts"]
    i_gap = t - df["ip_last_event_ts"]

    def zero_if_stale(col, gap, window):
        # a stored count describes the window ending at the entity's LAST event;
        # if that was longer ago than the window, the window is empty now
        return df[col].where(gap.notna() & (gap <= window), 0).fillna(0).astype("int64")

    out = pd.DataFrame(index=df.index)
    out["is_new_device"] = [_is_new(d, devs) for d, devs in zip(df["req_device"], df["known_devices"])]
    out["country_changed"] = (df["last_country"].notna() & (df["last_country"] != df["req_country"])).astype("int64")
    out["secs_since_user_last"] = u_gap.fillna(NEVER_SEEN).astype("float64")
    out["user_failed_5m"] = zero_if_stale("failed_logins_5m", u_gap, 300)
    out["user_logins_1h"] = zero_if_stale("logins_1h", u_gap, 3600)
    out["ip_attempts_5m"] = zero_if_stale("attempts_5m", i_gap, 300)
    out["ip_failed_5m"] = zero_if_stale("failed_5m", i_gap, 300)
    out["ip_distinct_users_5m"] = zero_if_stale("distinct_users_5m", i_gap, 300)
    return out[MODEL_FEATURES]
