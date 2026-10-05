"""Writes the Grafana dashboard JSON.  Run:  python -m monitoring.build_dashboard"""
import json

DS = {"type": "prometheus", "uid": "prometheus"}
OK_DRIFT = {"type": "value", "options": {"0": {"text": "ok", "color": "green"}, "1": {"text": "DRIFT", "color": "red"}}}


def panel(pid, title, kind, x, y, w, h, targets, defaults=None, options=None):
    return {
        "id": pid, "title": title, "type": kind, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": legend}
                    for i, (e, legend) in enumerate(targets)],
        "fieldConfig": {"defaults": defaults or {}, "overrides": []},
        "options": options or {},
    }


panels = [
    panel(1, "Dataset drift", "stat", 0, 0, 5, 5, [("fraud_drift_share >= bool 0.5", "")],
          {"mappings": [OK_DRIFT], "thresholds": {"mode": "absolute", "steps": [
              {"color": "green", "value": None}, {"color": "red", "value": 1}]}},
          {"colorMode": "background", "reduceOptions": {"calcs": ["lastNotNull"]}}),
    panel(2, "Share of features drifted", "timeseries", 5, 0, 10, 5, [("fraud_drift_share", "drifted")],
          {"unit": "percentunit", "min": 0, "max": 1}),
    panel(3, "Logins in the monitored window", "stat", 15, 0, 4, 5, [("fraud_drift_window_rows", "")],
          {"thresholds": {"mode": "absolute", "steps": [{"color": "blue", "value": None}]}},
          {"reduceOptions": {"calcs": ["lastNotNull"]}}),
    panel(4, "Model alert rate", "stat", 19, 0, 5, 5, [("fraud_flagged_rate", "")],
          {"unit": "percentunit", "thresholds": {"mode": "absolute", "steps": [{"color": "blue", "value": None}]}},
          {"reduceOptions": {"calcs": ["lastNotNull"]}}),
    panel(5, "Which features drifted (per feature)", "state-timeline", 0, 5, 24, 9,
          [("fraud_feature_drifted", "{{feature}}")],
          {"mappings": [OK_DRIFT], "thresholds": {"mode": "absolute", "steps": [
              {"color": "green", "value": None}, {"color": "red", "value": 1}]}},
          {"showValue": "never", "mergeValues": True, "rowHeight": 0.8}),
    panel(6, "Model score and alert rate over time", "timeseries", 0, 14, 24, 8,
          [("fraud_mean_score", "mean score"), ("fraud_flagged_rate", "alert rate")],
          {"min": 0, "max": 1}),
]

dash = {
    "uid": "login-drift", "title": "Login fraud: drift and alerts", "schemaVersion": 39, "version": 1,
    "refresh": "5s", "time": {"from": "now-45m", "to": "now"}, "tags": ["fraud", "drift"],
    "panels": panels,
}
with open("monitoring/grafana/dashboards/login_drift.json", "w") as f:
    json.dump(dash, f, indent=1)
print("wrote monitoring/grafana/dashboards/login_drift.json")
