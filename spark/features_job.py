"""Streaming feature job: Kafka `login_events` -> per-event features -> Parquet (offline store).

What it computes (every row is the state AFTER that event, stamped with the event's own time):

  user_features  (entity: user_id)
    failed_logins_5m       failed logins by this user in the 5 min ending at this event
    logins_1h              logins by this user in the last hour
    distinct_countries_1h  distinct countries this user logged in from, last hour
    last_country           country of this user's latest login
    known_devices          this user's last 10 distinct devices
    last_event_ts          epoch seconds of this user's latest login

  ip_features    (entity: ip)
    attempts_5m / failed_5m / distinct_users_5m   same idea, per source IP
    last_event_ts

  login_events   raw events + the is_fraud label (the training "spine")

Everything is computed from EVENT time (when the login happened), never processing time,
so replaying history gives the same numbers as the live stream.

Usage
  python spark/features_job.py --mode backfill --fresh     # drain Kafka once, then stop
  python spark/features_job.py --mode live                 # keep running
  python spark/features_job.py --source json --json-dir /tmp/events --mode backfill --fresh   # no Kafka
"""
import argparse
import json
import shutil
from pathlib import Path

import pandas as pd
import requests
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType, TimestampType)

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
DATA = Path("data")

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_time", StringType()),
    StructField("country", StringType()),
    StructField("device_id", StringType()),
    StructField("ip", StringType()),
    StructField("success", IntegerType()),
    StructField("is_fraud", IntegerType()),
])

USER_OUT = StructType([
    StructField("user_id", StringType()),
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("failed_logins_5m", LongType()),
    StructField("logins_1h", LongType()),
    StructField("distinct_countries_1h", LongType()),
    StructField("last_country", StringType()),
    StructField("known_devices", ArrayType(StringType())),
    StructField("last_event_ts", DoubleType()),
])
IP_OUT = StructType([
    StructField("ip", StringType()),
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("attempts_5m", LongType()),
    StructField("failed_5m", LongType()),
    StructField("distinct_users_5m", LongType()),
    StructField("last_event_ts", DoubleType()),
])
STATE = StructType([StructField("recent", StringType()), StructField("extra", StringType())])


def _load_state(state: GroupState):
    if state.exists:
        recent, extra = state.get
        return json.loads(recent), json.loads(extra)
    return [], []


def user_fn(key, pdfs, state: GroupState):
    """Per-user running state: recent events (<=1h) + last 10 devices."""
    recent, devices = _load_state(state)
    frames = list(pdfs)
    if not frames:
        return
    pdf = pd.concat(frames).sort_values("ts")
    rows = []
    for e in pdf.itertuples(index=False):
        t = float(e.ts)
        recent = [x for x in recent if x[0] > t - 3600]      # forget > 1h old
        recent.append([t, int(e.success), e.country])        # state AFTER this event
        if e.device_id in devices:
            devices.remove(e.device_id)
        devices = (devices + [e.device_id])[-10:]
        rows.append({
            "user_id": e.user_id, "event_id": e.event_id, "event_time": e.event_time,
            "failed_logins_5m": sum(1 for x in recent if x[0] > t - 300 and x[1] == 0),
            "logins_1h": len(recent),
            "distinct_countries_1h": len({x[2] for x in recent}),
            "last_country": e.country,
            "known_devices": list(devices),
            "last_event_ts": t,
        })
    state.update((json.dumps(recent), json.dumps(devices)))
    yield pd.DataFrame(rows, columns=[f.name for f in USER_OUT.fields])


def ip_fn(key, pdfs, state: GroupState):
    """Per-IP running state: recent events (<=5 min)."""
    recent, _ = _load_state(state)
    frames = list(pdfs)
    if not frames:
        return
    pdf = pd.concat(frames).sort_values("ts")
    rows = []
    for e in pdf.itertuples(index=False):
        t = float(e.ts)
        recent = [x for x in recent if x[0] > t - 300]
        recent.append([t, int(e.success), e.user_id])
        rows.append({
            "ip": e.ip, "event_id": e.event_id, "event_time": e.event_time,
            "attempts_5m": len(recent),
            "failed_5m": sum(1 for x in recent if x[1] == 0),
            "distinct_users_5m": len({x[2] for x in recent}),
            "last_event_ts": t,
        })
    state.update((json.dumps(recent), json.dumps([])))
    yield pd.DataFrame(rows, columns=[f.name for f in IP_OUT.fields])


def read_events(spark, args):
    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", "login_events")
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", 50000)
               .load())
        ev = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select("e.*")
    else:
        ev = spark.readStream.schema(EVENT_SCHEMA).json(args.json_dir)
    return (ev.withColumn("event_time", F.to_timestamp("event_time"))
              .withColumn("ts", F.col("event_time").cast("double")))


def trigger_for(args):
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["backfill", "live"], default="backfill")
    p.add_argument("--source", choices=["kafka", "json"], default="kafka")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--json-dir", default="/tmp/login_events_json")
    p.add_argument("--push-url", default=None,
                   help="Feast feature server push endpoint, e.g. http://localhost:6566/push (feeds Redis)")
    p.add_argument("--fresh", action="store_true",
                   help="wipe data/offline and data/checkpoints first (never touches registry/mlflow)")
    args = p.parse_args()

    if args.push_url:  # fail fast: a dead feature server would otherwise kill the stream mid-replay
        try:
            requests.get(args.push_url.rsplit("/", 1)[0] + "/health", timeout=3).raise_for_status()
        except Exception as e:
            raise SystemExit(f"Feast feature server not reachable at {args.push_url} ({e}).\n"
                             "Start it first:  cd feature_repo && feast apply && feast serve -p 6566")

    if args.fresh:
        for sub in ("offline", "checkpoints"):
            shutil.rmtree(DATA / sub, ignore_errors=True)

    b = (SparkSession.builder.appName("login-features").master("local[4]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         # Arrow (used by pandas UDFs) needs this on Java 17+
         .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
         .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true"))
    if args.source == "kafka":
        b = b.config("spark.jars.packages", KAFKA_PKG)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = read_events(spark, args)
    user_feats = events.groupBy("user_id").applyInPandasWithState(
        user_fn, USER_OUT, STATE, "append", GroupStateTimeout.NoTimeout)
    ip_feats = events.groupBy("ip").applyInPandasWithState(
        ip_fn, IP_OUT, STATE, "append", GroupStateTimeout.NoTimeout)
    spine = events.select("event_id", "user_id", "ip", "event_time", "country",
                          "device_id", "success", "is_fraud")

    queries = [start_features(user_feats, "user_features", args),
               start_features(ip_feats, "ip_features", args),
               start(spine, "login_events", args)]
    for q in queries:
        q.awaitTermination()
    print("done ->", DATA / "offline")


if __name__ == "__main__":
    main()
