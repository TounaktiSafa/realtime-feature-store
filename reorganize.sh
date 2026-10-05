#!/usr/bin/env bash
# Stage 2: organise the repo into topic folders (history is kept: files are moved with `git mv`).
#
#   common/       signals.py                     shared feature logic (training AND serving)
#   serving/      api.py                         FastAPI /score
#   training/     build_training_set.py, train.py
#   monitoring/   drift_job.py, build_dashboard.py   (+ prometheus.yml, grafana/ already here)
#   diagnostics/  check_*.py, compare_distributions.py, null_drift_test.py, leakage_demo.py, demo_score.py
#   ops/          services.sh, preflight.sh, check_monitoring.sh, demo.sh
#   docs/         Grafana screenshot renamed to docs/grafana.png (the README already points there)
#
# Run from the repository root with the pipeline STOPPED:
#     bash services.sh stop        # (before the move; afterwards it lives at ops/services.sh)
#     bash reorganize.sh
# Nothing is committed. Review with `git status`; to undo everything: git reset --hard HEAD
set -euo pipefail
cd "$(dirname "$0")"
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "not a git repo"; exit 1; }
[ -f signals.py ] || { echo "signals.py not at the root: already reorganised?"; exit 1; }
if pgrep -f "[f]eatures_job.py|[u]vicorn api:app|[f]east serve|[d]rift_job.py" >/dev/null; then
  echo "Pipeline processes are still running. Stop them first:  bash services.sh stop"; exit 1
fi

mv_to() {  # dir file...
  local dir="$1"; shift; mkdir -p "$dir"
  for f in "$@"; do [ -f "$f" ] && git mv "$f" "$dir/$f"; done
  return 0
}
mv_to common      signals.py
mv_to serving     api.py
mv_to training    build_training_set.py train.py
mv_to monitoring  drift_job.py build_dashboard.py
mv_to diagnostics check_future.py check_history.py compare_distributions.py null_drift_test.py leakage_demo.py demo_score.py
mv_to ops         services.sh preflight.sh check_monitoring.sh demo.sh
[ -f "docs/Grafana-Dashboard .png" ] && git mv "docs/Grafana-Dashboard .png" docs/grafana.png || true

for d in common serving training monitoring diagnostics; do
  [ -f "$d/__init__.py" ] || { : > "$d/__init__.py"; git add "$d/__init__.py"; }
done

python3 - <<'PY'
import re, subprocess
from pathlib import Path

MODULES = {  # script name -> package it now lives in
    "build_training_set": "training", "train": "training",
    "drift_job": "monitoring", "build_dashboard": "monitoring",
    "check_future": "diagnostics", "check_history": "diagnostics", "compare_distributions": "diagnostics",
    "null_drift_test": "diagnostics", "leakage_demo": "diagnostics", "demo_score": "diagnostics",
}
REPL = [  # plain-text replacements, applied in this order to .py / .sh / .md / Makefile
    ("from signals import", "from common.signals import"),
    ("from build_training_set import", "from training.build_training_set import"),
    ("from train import", "from training.train import"),
    ("[u]vicorn api:app", "[u]vicorn serving.api:app"),
    ("uvicorn api:app", "uvicorn serving.api:app"),
    ("bash services.sh", "bash ops/services.sh"),
    ("bash preflight.sh", "bash ops/preflight.sh"),
    ("./preflight.sh", "./ops/preflight.sh"),
    ("./check_monitoring.sh", "./ops/check_monitoring.sh"),
    ("./demo.sh", "./ops/demo.sh"),
] + [(f"python {n}.py", f"python -m {p}.{n}") for n, p in MODULES.items()]

files = subprocess.check_output(["git", "ls-files"], text=True).splitlines()
for f in files:
    p = Path(f)
    if f.startswith("data/") or not p.exists():
        continue
    if p.suffix not in {".py", ".sh", ".md"} and p.name != "Makefile":
        continue
    s0 = s = p.read_text()
    for old, new in REPL:
        s = s.replace(old, new)
    s = re.sub(r"(?<!common/)\bsignals\.py", "common/signals.py", s)
    if f.startswith("ops/") and p.suffix == ".sh":
        cd_new = 'cd "$(dirname "$0")/.." || exit 1'
        if 'cd "$(dirname "$0")" || exit 1' in s:
            s = s.replace('cd "$(dirname "$0")" || exit 1', cd_new)
        elif "dirname" not in s and f != "ops/check_monitoring.sh":
            lines = s.split("\n")          # no cd at all: add one right after the shebang
            lines.insert(1, cd_new)
            s = "\n".join(lines)
    if s != s0:
        p.write_text(s)
        print("updated", f)

readme = Path("README.md")
if readme.exists():
    s = readme.read_text()
    block = """## Repository layout

```
common/        signals.py: the one feature function shared by training and serving
serving/       api.py: FastAPI /score service (Redis -> signals -> champion model)
training/      build_training_set.py (point-in-time join), train.py (XGBoost + MLflow)
monitoring/    drift_job.py (Evidently -> Prometheus), build_dashboard.py, prometheus.yml, grafana/
diagnostics/   check_future.py, check_history.py, compare_distributions.py, null_drift_test.py,
               leakage_demo.py, demo_score.py
generator/     synthetic login events -> Kafka
spark/         features_job.py: Kafka -> Parquet + Feast push -> Redis
feature_repo/  Feast entity and feature view definitions
ops/           services.sh, preflight.sh, check_monitoring.sh, demo.sh
tests/         leakage tests
docs/          screenshots
```

Run every command from the repository root. Python scripts in packages run as modules (`python -m training.train`).

"""
    if "## Repository layout" not in s and "## Run it" in s:
        readme.write_text(s.replace("## Run it", block + "## Run it", 1))
        print("updated README.md (layout section)")
PY

echo
echo "Moved. Quick self-check for leftovers of the old names:"
grep -rn -E "python (build_training_set|train|drift_job|build_dashboard|check_future|check_history|compare_distributions|null_drift_test|leakage_demo|demo_score)\.py|from signals import|uvicorn api:app|bash (services|preflight)\.sh" \
  --include='*.py' --include='*.sh' --include='*.md' --include='Makefile' --exclude=reorganize.sh . 2>/dev/null | grep -v '^./data/' || echo "  none"
echo
git status --short | grep -v '^[DR]  data/' | head -40
echo
echo "Review, then:  git add -A && git commit -m 'Organise repo into topic folders' && git push"
