#!/usr/bin/env bash
# Finds the first broken hop between drift_job.py and the Grafana panels.   Usage: ./ops/check_monitoring.sh
step() { printf "\n[%s] %s\n" "$1" "$2"; }

step 1 "drift exporter on :8001 (python -m monitoring.drift_job must be running)"
out=$(curl -s -m 3 localhost:8001/metrics | grep -E "^fraud_(drift_share|drift_window_rows|flagged_rate) ")
[ -n "$out" ] && echo "$out" | sed 's/^/   /' || echo "   FAIL: nothing on :8001  ->  source .venv-feast/bin/activate && python -m monitoring.drift_job"

step 2 "Prometheus scrape target (:9090)"
curl -s -m 3 localhost:9090/api/v1/targets | python3 -c "
import sys, json
try:
    targets = json.load(sys.stdin)['data']['activeTargets']
except Exception:
    print('   FAIL: Prometheus not answering on :9090  ->  docker compose up -d prometheus'); sys.exit()
for t in targets:
    print('  ', t['scrapeUrl'], '->', t['health'].upper(), t.get('lastError') or '')
"

step 3 "Prometheus holds the data"
curl -s -m 3 'localhost:9090/api/v1/query?query=fraud_drift_window_rows' | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)['data']['result']
except Exception:
    print('   FAIL: query failed'); sys.exit()
print(('   OK: window has ' + r[0]['value'][1] + ' logins') if r else '   FAIL: no series (target DOWN, or drift_job has not judged yet)')
"

step 4 "Grafana can query Prometheus (:3000)"
curl -s -m 5 'localhost:3000/api/datasources/proxy/uid/prometheus/api/v1/query?query=fraud_drift_window_rows' \
  | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)['data']['result']
    print('   OK' if r else '   FAIL: Grafana reaches Prometheus but gets no series')
except Exception:
    print('   FAIL: Grafana not answering, or datasource uid is not prometheus')
"

step 5 "Dashboard loaded in Grafana"
curl -s -m 5 'localhost:3000/api/search?query=Login' | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print('   ', [x['title'] for x in d] or 'FAIL: no dashboard found (check ./monitoring/grafana/dashboards)')
except Exception:
    print('   FAIL: Grafana not answering')
"
echo
