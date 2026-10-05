"""Synthetic login-event generator -> Kafka topic `login_events` (account-takeover detection).

Modes
  live      (default): timestamps = now, throttled by --rate
  backfill  (--backfill N): N events, simulated past clock starting at --start, no sleeping

Normal behaviour
  * each user has a home country, 1-3 known devices, 1-3 usual IPs
  * ~5% typo failures, ~3% legit travel (foreign country), ~2% legit new device

Attacks (labelled is_fraud=1, ~1.5% of events)
  * brute force:         one attacker IP hammers ONE user (5-12 attempts, last one may succeed)
  * credential stuffing: one attacker IP tries MANY users (10-30 attempts, ~5% succeed)

Drift (--drift-after K): after K events the "login flow changes" -> failures 5%->15%,
  new devices 2%->12%, travel 3%->15%. Gives Evidently something real to catch.
"""
import argparse
import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

N_USERS = 2000
TOPIC = "login_events"
P_ATTACK = 0.0011  # chance a loop iteration starts an attack burst
COUNTRIES = ["TN", "FR", "DE", "US", "GB", "IT", "ES", "CA"]
COUNTRY_W = [40, 20, 8, 8, 8, 6, 6, 4]
ATTACK_COUNTRIES = ["RU", "CN", "BR", "VN", "NG", "ID"]


def build_world(seed, event_seed=None):
    rng = random.Random(seed)
    users = {
        u: {
            "country": rng.choices(COUNTRIES, COUNTRY_W)[0],
            "devices": [f"dev_{u}_{i}" for i in range(rng.randint(1, 3))],
            "ips": [f"ip_{rng.randrange(50000)}" for _ in range(rng.randint(1, 3))],
        }
        for u in range(N_USERS)
    }
    if event_seed is not None:  # same world (users, devices, IPs), but a fresh stream of events
        rng = random.Random(event_seed)
    return rng, users


def event(rng, user, ts, country, device, ip, success, attack):
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "user_id": f"user_{user}",
        "event_time": ts.isoformat(),
        "country": country,
        "device_id": device,
        "ip": ip,
        "success": int(success),
        "is_fraud": int(attack),
    }


def normal_login(rng, users, u, drift):
    p = users[u]
    fail_p, new_dev_p, travel_p = (0.15, 0.12, 0.15) if drift else (0.05, 0.02, 0.03)
    country, ip, device = p["country"], rng.choice(p["ips"]), rng.choice(p["devices"])
    if rng.random() < travel_p:
        country, ip = rng.choice(COUNTRIES), f"ip_{rng.randrange(50000)}"
    if rng.random() < new_dev_p:
        device = f"dev_{u}_new{rng.randrange(10**6)}"
    return country, device, ip, rng.random() >= fail_p


def attack_burst(rng):
    """Yields (user, country, device, ip, success) for one attack."""
    country = rng.choice(ATTACK_COUNTRIES)
    device = f"dev_atk_{rng.randrange(10**6)}"
    ip = f"ip_atk_{rng.randrange(500)}"
    if rng.random() < 0.5:  # brute force on one user
        u, n = rng.randrange(N_USERS), rng.randint(5, 12)
        for i in range(n):
            yield u, country, device, ip, i == n - 1 and rng.random() < 0.7
    else:  # credential stuffing across many users
        for _ in range(rng.randint(10, 30)):
            yield rng.randrange(N_USERS), country, device, ip, rng.random() < 0.05


def stream(args):
    # live logins must not replay the backfill's random stream (same event_ids, same behaviour)
    rng, users = build_world(args.seed, event_seed=None if args.backfill else time.time_ns())
    live = args.backfill == 0
    now = lambda: datetime.now(timezone.utc)
    clock = now() if live else datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    total = float("inf") if live else args.backfill
    n = 0
    while n < total:
        drift = bool(args.drift_after) and n >= args.drift_after
        if rng.random() < P_ATTACK:
            for u, country, device, ip, ok in attack_burst(rng):
                clock = now() if live else clock + timedelta(seconds=rng.uniform(0.5, 6))
                yield event(rng, u, clock, country, device, ip, ok, True)
                n += 1
                if live:
                    time.sleep(rng.uniform(0.5, 6))  # same pacing as the training data
        else:
            clock = now() if live else clock + timedelta(seconds=rng.expovariate(1 / 3))
            u = rng.randrange(N_USERS)
            country, device, ip, ok = normal_login(rng, users, u, drift)
            yield event(rng, u, clock, country, device, ip, ok, False)
            n += 1
            if live:
                time.sleep(1 / args.rate)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--rate", type=float, default=0.33,
                   help="events/sec (live). Keep ~0.33 = same per-user rate as the training history")
    p.add_argument("--backfill", type=int, default=0, help="N historical events, no sleeping")
    p.add_argument("--start", default="2026-07-01", help="backfill start date")
    p.add_argument("--drift-after", type=int, default=0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dry-run", action="store_true", help="print instead of sending to Kafka")
    p.add_argument("--score-url", default=None,
                   help="score each login via the API BEFORE publishing it (like a real login service)")
    args = p.parse_args()

    producer = None
    if not args.dry_run:
        from confluent_kafka import Producer
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 20,
            "queue.buffering.max.messages": 500_000,
        })

    sent = 0
    recent = deque(maxlen=500)  # (flagged, is_fraud) for the last 500 scored logins
    for e in stream(args):
        if args.score_url:
            import requests
            r = requests.post(args.score_url, timeout=5, json={k: e[k] for k in
                              ("user_id", "ip", "country", "device_id", "event_time")}).json()
            recent.append((bool(r["flagged"]), e["is_fraud"] == 1))
        if producer:
            # key = user_id: one user's events stay ordered inside one partition
            while True:
                try:
                    producer.produce(TOPIC, key=e["user_id"], value=json.dumps(e))
                    break
                except BufferError:  # local queue full: let it drain, then retry
                    producer.poll(0.5)
            producer.poll(0)
        else:
            print(json.dumps(e))
        sent += 1
        if sent % 5000 == 0:
            print(f"sent {sent}")
        if args.score_url and sent % 500 == 0:
            tp = sum(f and y for f, y in recent)
            flagged, fraud = sum(f for f, _ in recent), sum(y for _, y in recent)
            print(f"[{sent} logins] last 500: fraud={fraud} flagged={flagged} "
                  f"precision={tp / max(flagged, 1):.2f} recall={tp / max(fraud, 1):.2f}")
    if producer:
        producer.flush()
    print(f"done: {sent} events")


if __name__ == "__main__":
    main()
