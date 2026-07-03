import json
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from http.server import BaseHTTPRequestHandler, HTTPServer


ROOT = Path(os.getenv("PROJECT_ROOT", "/workspace"))

OUT_DIR = ROOT / "rafah/modelling/market_flow_outputs"
PUBLIC_OUT_DIR = ROOT / "rafah/dashboard-react/public/market_flow_outputs"
AUDIT_PATH = ROOT / "rafah/orchestration/audit/market_flow_audit.jsonl"
LOG_DIR = ROOT / "rafah/orchestration/logs"

KURS_API = os.getenv("KURS_API", "http://100.118.244.91:8002").rstrip("/")
NEWS_API = os.getenv("NEWS_API", "http://100.118.244.91:8000").rstrip("/")
COMMODITY_API = os.getenv("COMMODITY_API", "http://100.92.242.101:8001").rstrip("/")

API_CHECKS = {
    "news_api": f"{NEWS_API}/api/sentiment/daily",
    "kurs_api": f"{KURS_API}/kurs/daily",
    "commodity_api": f"{COMMODITY_API}/commodity/daily?symbol=SI%3DF&limit=3",
}


def read_json(path):
    try:
        path = Path(path)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def esc(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def metric(name, value, labels=None):
    labels = labels or {}
    if labels:
        label_text = ",".join(f'{k}="{esc(v)}"' for k, v in labels.items())
        return f'{name}{{{label_text}}} {value}'
    return f"{name} {value}"


def check_url(url, timeout=5):
    start = time.time()
    try:
        req = Request(url, headers={"User-Agent": "market-flow-monitor/1.0"})
        with urlopen(req, timeout=timeout) as response:
            code = response.getcode()
            latency = time.time() - start
            return 1 if 200 <= code < 400 else 0, code, latency
    except HTTPError as e:
        latency = time.time() - start
        return 0, e.code, latency
    except (URLError, TimeoutError, Exception):
        latency = time.time() - start
        return 0, 0, latency


def audit_counts():
    counts = {}
    total = 0
    last_ts = 0

    if not AUDIT_PATH.exists():
        return counts, total, last_ts

    for line in AUDIT_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue

        severity = row.get("severity", "UNKNOWN")
        event = row.get("event", "unknown")
        counts[(severity, event)] = counts.get((severity, event), 0) + 1
        total += 1

        ts = row.get("timestamp")
        try:
            last_ts = max(last_ts, datetime.fromisoformat(ts).timestamp())
        except Exception:
            pass

    return counts, total, last_ts


def file_mtime(path):
    try:
        path = Path(path)
        return path.stat().st_mtime if path.exists() else 0
    except Exception:
        return 0


def collect_metrics():
    lines = []
    lines.append("# HELP market_flow_api_up API health status, 1 means reachable.")
    lines.append("# TYPE market_flow_api_up gauge")
    lines.append("# HELP market_flow_api_latency_seconds API request latency in seconds.")
    lines.append("# TYPE market_flow_api_latency_seconds gauge")

    for name, url in API_CHECKS.items():
        up, code, latency = check_url(url)
        lines.append(metric("market_flow_api_up", up, {"service": name, "url": url}))
        lines.append(metric("market_flow_api_http_status", code, {"service": name}))
        lines.append(metric("market_flow_api_latency_seconds", f"{latency:.6f}", {"service": name}))

    report = read_json(OUT_DIR / "market_flow_model_report.json")
    signal = read_json(OUT_DIR / "business_latest_signal.json")
    dq = read_json(OUT_DIR / "data_quality_report.json")

    latest_prediction = report.get("latest_prediction", {}) if isinstance(report, dict) else {}

    predicted_direction = (
        signal.get("predicted_direction")
        or latest_prediction.get("predicted_direction")
        or "unknown"
    )

    predicted_change = safe_float(
        signal.get("predicted_change_pct", latest_prediction.get("predicted_change_pct", 0))
    )

    confidence = safe_float(
        signal.get("confidence", latest_prediction.get("confidence", 0))
    )

    main_driver = signal.get("main_driver", "-")
    main_driver_corr = safe_float(signal.get("main_driver_correlation", 0))
    rows_joined = safe_int(report.get("rows_joined", 0))
    mae = safe_float(report.get("mae", 0))
    r2 = safe_float(report.get("r2", 0))
    direction_accuracy = safe_float(report.get("direction_accuracy", 0))

    lines.append("# HELP market_flow_rows_joined Number of joined rows used by modelling.")
    lines.append("# TYPE market_flow_rows_joined gauge")
    lines.append(metric("market_flow_rows_joined", rows_joined))

    lines.append("# HELP market_flow_predicted_change_pct Latest predicted EUR/USD change percentage.")
    lines.append("# TYPE market_flow_predicted_change_pct gauge")
    lines.append(metric("market_flow_predicted_change_pct", predicted_change, {"direction": predicted_direction}))

    lines.append("# HELP market_flow_prediction_confidence Latest model prediction confidence.")
    lines.append("# TYPE market_flow_prediction_confidence gauge")
    lines.append(metric("market_flow_prediction_confidence", confidence, {"direction": predicted_direction}))

    lines.append("# HELP market_flow_signal_direction One-hot current predicted direction.")
    lines.append("# TYPE market_flow_signal_direction gauge")
    for direction in ["strengthening", "weakening", "stable", "unknown"]:
        lines.append(metric("market_flow_signal_direction", 1 if predicted_direction == direction else 0, {"direction": direction}))

    lines.append("# HELP market_flow_main_driver_correlation Correlation of latest main driver.")
    lines.append("# TYPE market_flow_main_driver_correlation gauge")
    lines.append(metric("market_flow_main_driver_correlation", main_driver_corr, {"driver": main_driver}))

    lines.append("# HELP market_flow_model_mae Model MAE.")
    lines.append("# TYPE market_flow_model_mae gauge")
    lines.append(metric("market_flow_model_mae", mae))

    lines.append("# HELP market_flow_model_r2 Model R2.")
    lines.append("# TYPE market_flow_model_r2 gauge")
    lines.append(metric("market_flow_model_r2", r2))

    lines.append("# HELP market_flow_direction_accuracy Direction accuracy.")
    lines.append("# TYPE market_flow_direction_accuracy gauge")
    lines.append(metric("market_flow_direction_accuracy", direction_accuracy))

    dq_status = dq.get("status", "missing")
    dq_warning_count = safe_int(dq.get("warning_count", 0))
    dq_rows = safe_int(dq.get("row_count", 0))
    dq_nulls = safe_int(dq.get("total_null_count", 0))

    lines.append("# HELP market_flow_dq_status Data quality status, one-hot.")
    lines.append("# TYPE market_flow_dq_status gauge")
    for status in ["passed", "warning", "missing"]:
        lines.append(metric("market_flow_dq_status", 1 if dq_status == status else 0, {"status": status}))

    lines.append("# HELP market_flow_dq_warning_count Number of data quality warnings.")
    lines.append("# TYPE market_flow_dq_warning_count gauge")
    lines.append(metric("market_flow_dq_warning_count", dq_warning_count))

    lines.append("# HELP market_flow_dq_total_null_count Total null count in joined dataset.")
    lines.append("# TYPE market_flow_dq_total_null_count gauge")
    lines.append(metric("market_flow_dq_total_null_count", dq_nulls))

    lines.append("# HELP market_flow_dq_row_count Row count from DQ report.")
    lines.append("# TYPE market_flow_dq_row_count gauge")
    lines.append(metric("market_flow_dq_row_count", dq_rows))

    counts, total_audit, last_audit_ts = audit_counts()

    lines.append("# HELP market_flow_audit_events_total Audit event count by severity and event.")
    lines.append("# TYPE market_flow_audit_events_total counter")
    for (severity, event), count in counts.items():
        lines.append(metric("market_flow_audit_events_total", count, {"severity": severity, "event": event}))

    lines.append("# HELP market_flow_audit_total Total audit records.")
    lines.append("# TYPE market_flow_audit_total counter")
    lines.append(metric("market_flow_audit_total", total_audit))

    lines.append("# HELP market_flow_last_audit_timestamp_seconds Last audit timestamp.")
    lines.append("# TYPE market_flow_last_audit_timestamp_seconds gauge")
    lines.append(metric("market_flow_last_audit_timestamp_seconds", last_audit_ts))

    lines.append("# HELP market_flow_output_file_mtime_seconds Output file last modified time.")
    lines.append("# TYPE market_flow_output_file_mtime_seconds gauge")

    output_files = [
        "market_flow_model_report.json",
        "business_latest_signal.json",
        "data_quality_report.json",
        "model_predictions_daily.csv",
        "market_flow_joined_dataset.csv",
        "correlation_vs_kurs_change.csv",
        "feature_importance.csv",
    ]

    for file_name in output_files:
        lines.append(metric("market_flow_output_file_mtime_seconds", file_mtime(OUT_DIR / file_name), {"file": file_name}))

    public_count = len(list(PUBLIC_OUT_DIR.glob("*"))) if PUBLIC_OUT_DIR.exists() else 0
    log_count = len(list(LOG_DIR.glob("*.log"))) if LOG_DIR.exists() else 0

    lines.append("# HELP market_flow_dashboard_output_files Number of exported dashboard output files.")
    lines.append("# TYPE market_flow_dashboard_output_files gauge")
    lines.append(metric("market_flow_dashboard_output_files", public_count))

    lines.append("# HELP market_flow_execution_log_files Number of modelling execution log files.")
    lines.append("# TYPE market_flow_execution_log_files gauge")
    lines.append(metric("market_flow_execution_log_files", log_count))

    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/metrics"):
            body = collect_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):
        return


if __name__ == "__main__":
    port = int(os.getenv("METRICS_PORT", "8010"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"Market Flow metrics exporter listening on 0.0.0.0:{port}")
    server.serve_forever()
