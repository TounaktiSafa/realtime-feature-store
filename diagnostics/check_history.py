"""What events does the pipeline actually hold? Compares the replayed history (offline store)
with the training set, and shows how busy the last days/hours were.
Run from ~/login-events (venv active):   python -m diagnostics.check_history
"""
import pandas as pd

ev = pd.read_parquet("data/offline/login_events")
tr = pd.read_parquet("data/training_set.parquet", columns=["event_id", "event_time"])
ev["event_time"] = pd.to_datetime(ev["event_time"], utc=True)
tr["event_time"] = pd.to_datetime(tr["event_time"], utc=True)
now = pd.Timestamp.now(tz="UTC")

print(f"events in offline store (= everything Spark replayed from Kafka): {len(ev):,}")
print(f"rows in training set:                                            {len(tr):,}")
extra = ~ev["event_id"].isin(tr["event_id"])
print(f"events NOT in the training set:                                  {extra.sum():,}\n")

print(f"training set time span: {tr.event_time.min()}  ->  {tr.event_time.max()}")
print(f"offline store time span: {ev.event_time.min()}  ->  {ev.event_time.max()}")
print(f"now: {now}\n")

print("events per day, last 6 days with data:")
print(ev.groupby(ev.event_time.dt.floor("D")).size().tail(6).to_string(), "\n")

recent = ev[ev.event_time > now - pd.Timedelta(hours=3)]
print(f"events in the last 3 hours: {len(recent):,} "
      f"(the current generator produces ~{int(0.33 * 3 * 3600):,} per 3 h)")
if extra.sum():
    e = ev[extra]
    print(f"\nthe {extra.sum():,} extra events span {e.event_time.min()} -> {e.event_time.max()}")
    print("extra events per hour, last 8 hours with data:")
    print(e.groupby(e.event_time.dt.floor('h')).size().tail(8).to_string())
