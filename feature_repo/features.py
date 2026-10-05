from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Array, Float64, Int64, String

# ---- entities: the "things" we keep features about ----------------------------------
user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING)
ip = Entity(name="ip", join_keys=["ip"], value_type=ValueType.STRING)

# ---- offline sources: the Parquet tables written by Spark ---------------------------
user_source = FileSource(
    path="../data/offline/user_features",
    timestamp_field="event_time",
    field_mapping={"last_event_ts": "user_last_event_ts"},  # both tables share this name
)
ip_source = FileSource(
    path="../data/offline/ip_features",
    timestamp_field="event_time",
    field_mapping={"last_event_ts": "ip_last_event_ts"},
)

# ---- push sources: the Spark job pushes fresh rows here (-> Redis) as logins arrive -------
user_push = PushSource(name="user_push", batch_source=user_source)
ip_push = PushSource(name="ip_push", batch_source=ip_source)

# ---- feature views: precomputed features, looked up by entity + time ----------------
user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="failed_logins_5m", dtype=Int64),
        Field(name="logins_1h", dtype=Int64),
        Field(name="distinct_countries_1h", dtype=Int64),
        Field(name="last_country", dtype=String),
        Field(name="known_devices", dtype=Array(String)),
        Field(name="user_last_event_ts", dtype=Float64),
    ],
    source=user_push,
)
ip_features = FeatureView(
    name="ip_features",
    entities=[ip],
    ttl=timedelta(days=30),
    schema=[
        Field(name="attempts_5m", dtype=Int64),
        Field(name="failed_5m", dtype=Int64),
        Field(name="distinct_users_5m", dtype=Int64),
        Field(name="ip_last_event_ts", dtype=Float64),
    ],
    source=ip_push,
)
