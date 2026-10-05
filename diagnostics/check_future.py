import sys
from datetime import datetime, timezone
from feast import FeatureStore
s = FeatureStore("feature_repo")
r = s.get_online_features(features=["user_features:user_last_event_ts"],
        entity_rows=[{"user_id": f"user_{i}"} for i in range(1, 200)]).to_dict()
vals = [v for v in r["user_last_event_ts"] if v]
now = datetime.now(timezone.utc).timestamp()
print(len(vals), "users found; newest stored login:", round((now - max(vals)) / 3600, 1), "hours ago")
bad = [v for v in vals if v > now]
print(f"FAIL: {len(bad)} stored logins are in the future" if bad else "OK: nothing stored is in the future")
sys.exit(1 if bad else 0)
