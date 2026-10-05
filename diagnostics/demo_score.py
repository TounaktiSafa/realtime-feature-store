"""Score two hand-picked logins against the running API (uvicorn serving.api:app --port 8000).

Redis holds each user's/IP's LATEST state, so we replay only from events that are still the
latest for both their user and their IP - otherwise "now" would sit before the stored state.
"""
import pandas as pd
import requests

URL = "http://localhost:8000/score"
ev = pd.read_parquet("data/offline/login_events").sort_values("event_time")
latest_user = ev.groupby("user_id").event_time.transform("max")
latest_ip = ev.groupby("ip").event_time.transform("max")
fresh = ev[(ev.event_time == latest_user) & (ev.event_time == latest_ip)]


def next_login(row, delta):
    return dict(user_id=row.user_id, ip=row.ip, country=row.country, device_id=row.device_id,
                event_time=(row.event_time + delta).isoformat())


attack = next_login(fresh[fresh.is_fraud == 1].iloc[-1], pd.Timedelta(seconds=10))   # attack in progress
normal = next_login(fresh[(fresh.is_fraud == 0) & (fresh.success == 1)].iloc[-1], pd.Timedelta(hours=1))

for name, body in [("ATTACK", attack), ("NORMAL", normal)]:
    r = requests.post(URL, json=body).json()
    print(f"{name}: score={r['score']} flagged={r['flagged']} latency={r['latency_ms']}ms")
    print("   ", r["features"])
