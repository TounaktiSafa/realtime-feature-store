#!/usr/bin/env bash
# Is everything the pipeline needs actually running?  Usage: ./preflight.sh
ok()  { printf "  OK       %s\n" "$1"; }
bad() { printf "  MISSING  %-28s -> %s\n" "$1" "$2"; }

docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null | grep -q login_events \
  && ok "Kafka + topic login_events" || bad "Kafka / topic" "docker compose up -d ; sleep 15 ; make topics"
docker exec redis redis-cli ping 2>/dev/null | grep -q PONG \
  && ok "Redis" || bad "Redis" "docker compose up -d"
curl -s -m 2 -o /dev/null -w "%{http_code}" localhost:6566/health | grep -q 200 \
  && ok "Feast feature server :6566" || bad "Feast feature server :6566" "cd feature_repo && feast apply && feast serve -p 6566"
curl -s -m 2 localhost:8000/health | grep -q '"status":"ok"' \
  && ok "Scoring API :8000" || bad "Scoring API :8000" "uvicorn api:app --port 8000"
pgrep -f "[f]eatures_job.py" >/dev/null \
  && ok "Spark feature job" || bad "Spark feature job" "python spark/features_job.py --mode live --fresh --push-url http://localhost:6566/push"

n=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic login_events 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
echo "  info     Kafka topic holds ${n} events (a clean backfill = about 200000)"
.venv-feast/bin/python check_future.py >/dev/null 2>&1 \
  && ok "No stored timestamps in the future" || bad "Redis timestamps" "run: python check_future.py to see why (future dates, or Redis down)"
