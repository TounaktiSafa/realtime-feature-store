"""Step 7b patch: live updates (Spark -> Feast push -> Redis) + score-before-publish generator.
Run from the project root:  python patch_7b.py      (safe: every replacement is asserted)
"""
from pathlib import Path


def patch(path, pairs):
    p = Path(path)
    s = p.read_text()
    for old, new in pairs:
        assert old in s, f"{path}: expected text not found:\n{old[:90]}"
        s = s.replace(old, new, 1)
    p.write_text(s)
    print("patched", path)


# ---------------------------------------------------------------- feature_repo/features.py
patch("feature_repo/features.py", [
    ("from feast import Entity, FeatureView, Field, FileSource, ValueType",
     "from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType"),
    ("# ---- feature views",
     '''# ---- push sources: the Spark job pushes fresh rows here (-> Redis) as logins arrive -------
user_push = PushSource(name="user_push", batch_source=user_source)
ip_push = PushSource(name="ip_push", batch_source=ip_source)

# ---- feature views'''),
    ("source=user_source,\n)\nip_features", "source=user_push,\n)\nip_features"),
    ("    source=ip_source,\n)", "    source=ip_push,\n)"),
])

# ---------------------------------------------------------------- spark/features_job.py
patch("spark/features_job.py", [
    ("import pandas as pd\nfrom pyspark.sql import SparkSession",
     "import pandas as pd\nimport requests\nfrom pyspark.sql import SparkSession, Window"),
    ('''def start(df, name, args):
    trigger = {"availableNow": True} if args.mode == "backfill" else {"processingTime": "2 seconds"}
    return (df.writeStream.format("parquet").outputMode("append")
            .option("path", str(DATA / "offline" / name))
            .option("checkpointLocation", str(DATA / "checkpoints" / name))
            .trigger(**trigger).queryName(name).start())
''', '''def trigger_for(args):
    return {"availableNow": True} if args.mode == "backfill" else {"processingTime": "1 second"}


def start(df, name, args):
    """Plain Parquet sink (used for the raw events table)."""
    return (df.writeStream.format("parquet").outputMode("append")
            .option("path", str(DATA / "offline" / name))
            .option("checkpointLocation", str(DATA / "checkpoints" / name))
            .trigger(**trigger_for(args)).queryName(name).start())


# feature table -> (Feast push source, entity column, column renames to match the Feast view)
PUSH = {
    "user_features": ("user_push", "user_id", {"last_event_ts": "user_last_event_ts"}),
    "ip_features": ("ip_push", "ip", {"last_event_ts": "ip_last_event_ts"}),
}


def make_batch_writer(name, args):
    source, key, rename = PUSH[name]
    path = str(DATA / "offline" / name)

    def write(batch_df, batch_id):
        batch_df.persist()
        batch_df.write.mode("append").parquet(path)               # 1) offline store (full history)
        if args.push_url:                                         # 2) online store: latest row per entity
            latest = (batch_df
                      .withColumn("_rn", F.row_number().over(Window.partitionBy(key).orderBy(F.col("event_time").desc())))
                      .filter("_rn = 1").drop("_rn", "event_id"))
            pdf = latest.toPandas().rename(columns=rename)
            if len(pdf):
                pdf["event_time"] = pdf["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
                if "known_devices" in pdf:
                    pdf["known_devices"] = pdf["known_devices"].map(list)
                for i in range(0, len(pdf), 5000):
                    body = {"push_source_name": source, "to": "online",
                            "df": pdf.iloc[i:i + 5000].to_dict(orient="list")}
                    requests.post(args.push_url, json=body, timeout=120).raise_for_status()
        batch_df.unpersist()

    return write


def start_features(df, name, args):
    """Feature tables: every micro-batch goes to Parquet AND (if --push-url) to Redis via Feast."""
    return (df.writeStream.foreachBatch(make_batch_writer(name, args)).outputMode("append")
            .option("checkpointLocation", str(DATA / "checkpoints" / name))
            .trigger(**trigger_for(args)).queryName(name).start())
'''),
    ('''    queries = [start(user_feats, "user_features", args),
               start(ip_feats, "ip_features", args),''',
     '''    queries = [start_features(user_feats, "user_features", args),
               start_features(ip_feats, "ip_features", args),'''),
    ('''    p.add_argument("--fresh", action="store_true", help="wipe ./data (offline store + checkpoints) first")''',
     '''    p.add_argument("--push-url", default=None,
                   help="Feast feature server push endpoint, e.g. http://localhost:6566/push (feeds Redis)")
    p.add_argument("--fresh", action="store_true",
                   help="wipe data/offline and data/checkpoints first (never touches registry/mlflow)")'''),
    ('''    if args.fresh and DATA.exists():
        shutil.rmtree(DATA)''',
     '''    if args.fresh:
        for sub in ("offline", "checkpoints"):
            shutil.rmtree(DATA / sub, ignore_errors=True)'''),
])

# ---------------------------------------------------------------- generator/generate.py
patch("generator/generate.py", [
    ("import uuid\nfrom datetime", "import uuid\nfrom collections import deque\nfrom datetime"),
    ('''                n += 1
                if live:
                    time.sleep(0.1)''', '''                n += 1
                if live:
                    time.sleep(rng.uniform(0.5, 6))  # same pacing as the training data'''),
    ('''    p.add_argument("--rate", type=float, default=20, help="events/sec (live mode)")''',
     '''    p.add_argument("--rate", type=float, default=0.33,
                   help="events/sec (live). Keep ~0.33 = same per-user rate as the training history")'''),
    ('''    p.add_argument("--dry-run", action="store_true", help="print instead of sending to Kafka")''',
     '''    p.add_argument("--dry-run", action="store_true", help="print instead of sending to Kafka")
    p.add_argument("--score-url", default=None,
                   help="score each login via the API BEFORE publishing it (like a real login service)")'''),
    ('''    sent = 0
    for e in stream(args):''', '''    sent = 0
    recent = deque(maxlen=500)  # (flagged, is_fraud) for the last 500 scored logins
    for e in stream(args):
        if args.score_url:
            r = requests.post(args.score_url, timeout=5, json={k: e[k] for k in
                              ("user_id", "ip", "country", "device_id", "event_time")}).json()
            recent.append((bool(r["flagged"]), e["is_fraud"] == 1))'''),
    ('''        if sent % 5000 == 0:
            print(f"sent {sent}")''', '''        if sent % 5000 == 0:
            print(f"sent {sent}")
        if args.score_url and sent % 100 == 0 and recent:
            tp = sum(f and y for f, y in recent)
            flagged, fraud = sum(f for f, _ in recent), sum(y for _, y in recent)
            print(f"[{sent} logins] last {len(recent)}: fraud={fraud} flagged={flagged} "
                  f"precision={tp / max(flagged, 1):.2f} recall={tp / max(fraud, 1):.2f}")'''),
    ("import uuid\nfrom collections import deque", "import requests\nimport uuid\nfrom collections import deque"),
])
