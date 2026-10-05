"""Build the point-in-time-correct training set -> data/training_set.parquet

For every login we ask Feast: "what did this user / this IP look like JUST BEFORE this login?"
(entity timestamp = login time - 1 microsecond), so a login can never see itself or the future.
"""
import time

import pandas as pd
from feast import FeatureStore

from common.signals import IP_REFS, MODEL_FEATURES, USER_REFS, build_signals


JUST_BEFORE = pd.Timedelta(microseconds=-1)


def build(offset=JUST_BEFORE, tail=None):
    """offset = how far from the login time we look up features. The default (-1 microsecond)
    is the correct one. leakage_demo.py passes other values on purpose to show what breaks."""
    st = FeatureStore("feature_repo")
    spine = pd.read_parquet("data/offline/login_events").sort_values("event_time").reset_index(drop=True)
    if tail:  # only used by the demo, to keep it fast
        spine = spine.tail(tail).reset_index(drop=True)

    base = pd.DataFrame({"event_id": spine.event_id, "event_timestamp": spine.event_time + offset})
    # One lookup per entity, then LEFT-merge: Feast's file store silently drops rows whose
    # user/IP has no history, and those (brand-new attacker IPs) are exactly what we must keep.
    u = st.get_historical_features(entity_df=base.assign(user_id=spine.user_id), features=USER_REFS).to_df()
    i = st.get_historical_features(entity_df=base.assign(ip=spine.ip), features=IP_REFS).to_df()

    df = (spine
          .merge(u.drop(columns=["event_timestamp", "user_id"]), on="event_id", how="left")
          .merge(i.drop(columns=["event_timestamp", "ip"]), on="event_id", how="left"))
    df["req_ts"] = df.event_time.map(lambda x: x.timestamp())
    df["req_country"], df["req_device"] = df.country, df.device_id

    X = build_signals(df)
    out = pd.concat([df[["event_id", "event_time", "user_id", "ip", "is_fraud"]], X], axis=1)
    assert len(out) == len(spine) and out.event_id.is_unique, "rows were dropped or duplicated"
    return out


def main():
    t0 = time.time()
    out = build()
    out.to_parquet("data/training_set.parquet", index=False)
    print(f"{len(out):,} rows | fraud {out.is_fraud.mean():.2%} | "
          f"IP quiet in last 5 min: {(out.ip_attempts_5m == 0).mean():.0%} | {time.time() - t0:.0f}s")
    print("saved data/training_set.parquet; features:", MODEL_FEATURES)


if __name__ == "__main__":
    main()
