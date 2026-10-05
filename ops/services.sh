#!/usr/bin/env bash
# Starts / stops / checks Feast, Spark and the API in the BACKGROUND, so no terminal
# window can kill them by accident (Ctrl+C, closing the window, typing in the wrong one).
#
#   bash ops/services.sh start     start whatever is not already running
#   bash ops/services.sh status    show what is running
#   bash ops/services.sh stop      stop all three
#   bash ops/services.sh logs      follow the three log files (Ctrl+C only stops the viewing)
#
# Run from ~/login-events. Works whether or not a venv is active.
cd "$(dirname "$0")/.." || exit 1
ROOT="$PWD"
VENV="$ROOT/.venv-feast"
mkdir -p logs

# PATH without the venv, for Spark (its packages live outside the venv)
CLEAN_PATH="$(echo "$PATH" | tr ':' '\n' | grep -v '\.venv-feast' | paste -sd: -)"

running() { [ -f "logs/$1.pid" ] && kill -0 "$(cat "logs/$1.pid")" 2>/dev/null; }

start_one() {  # name, command...
  local name="$1"; shift
  if running "$name"; then echo "  already running: $name"; return; fi
  nohup "$@" > "logs/$name.log" 2>&1 &
  echo $! > "logs/$name.pid"
  echo "  started: $name (log: logs/$name.log)"
}

case "$1" in
  start)
    echo "Docker (kafka + redis):"; docker compose up -d >/dev/null 2>&1 && echo "  up" || echo "  FAILED: docker compose up -d"
    echo "Feast:"
    ( cd "$ROOT/feature_repo" && "$VENV/bin/feast" apply >/dev/null 2>&1 )
    start_one feast bash -c "cd '$ROOT/feature_repo' && exec '$VENV/bin/feast' serve -p 6566"
    for i in $(seq 1 30); do curl -s -o /dev/null -m 2 localhost:6566/health && break; sleep 1; done
    echo "Spark (no venv):"
    start_one spark env PATH="$CLEAN_PATH" PYSPARK_PYTHON=/usr/bin/python3 PYTHONWARNINGS=ignore \
      /usr/bin/python3 spark/features_job.py --mode live --push-url http://localhost:6566/push
    echo "API:"
    start_one api "$VENV/bin/python" -m uvicorn serving.api:app --port 8000
    echo "Wait ~20 s for the API to load the model, then run:  bash ops/preflight.sh"
    ;;
  status)
    for n in feast spark api; do running "$n" && echo "  running  $n" || echo "  STOPPED  $n  (see logs/$n.log)"; done ;;
  stop)
    for n in feast spark api; do
      if running "$n"; then
        pkill -P "$(cat logs/$n.pid)" 2>/dev/null; kill "$(cat logs/$n.pid)" 2>/dev/null; echo "  stopped $n"
      fi
      rm -f "logs/$n.pid"
    done
    pkill -f "[f]eatures_job.py" 2>/dev/null; pkill -f "[f]east serve" 2>/dev/null; pkill -f "[u]vicorn serving.api:app" 2>/dev/null
    ;;
  logs) tail -n 5 -F logs/feast.log logs/spark.log logs/api.log ;;
  *) echo "usage: bash ops/services.sh start|status|stop|logs"; exit 1 ;;
esac
