#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
bash services.sh start
echo "waiting for the API to load the model..."
for i in $(seq 1 60); do curl -s -m 2 localhost:8000/health | grep -q '"status":"ok"' && break; sleep 2; done
bash preflight.sh
echo; echo "Now run in this terminal:"
echo "  source .venv-feast/bin/activate && python generator/generate.py --rate 1 --drift-after 300 --score-url http://localhost:8000/score"
