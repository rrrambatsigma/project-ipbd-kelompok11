import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

KURS_API = os.getenv("KURS_API", "http://100.118.244.91:8002").rstrip("/")
NEWS_API = os.getenv("NEWS_API", "http://100.118.244.91:8000").rstrip("/")
COMMODITY_API = os.getenv("COMMODITY_API", "http://100.92.242.101:8001").rstrip("/")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_MODELLING") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

MOVEMENT_THRESHOLD = float(os.getenv("MARKET_FLOW_MOVEMENT_THRESHOLD", "0.25"))

OUT_DIR = ROOT / "rafah/modelling/market_flow_outputs"
PUBLIC_OUT_DIR = ROOT / "rafah/dashboard-react/public/market_flow_outputs"

LOG_DIR = ROOT / "rafah/orchestration/logs"
STATE_DIR = ROOT / "rafah/orchestration/state"
AUDIT_DIR = ROOT / "rafah/orchestration/audit"

LAST_SIGNAL_PATH = STATE_DIR / "last_signal.json"
AUDIT_LOG_PATH = AUDIT_DIR / "market_flow_audit.jsonl"


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def write_audit(severity, event, details=None):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "timestamp": now_iso(),
        "severity": severity,
        "event": event,
        "details": details or {},
    }

    with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram env is missing. Skip notification.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[WARN] Telegram send failed: {e}")
        return False


def read_json(path):
    path = Path(path)
    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def friendly_driver(name):
    mapping = {
        "change_pct_SIF": "Silver daily move",
        "change_pct_BTC_USD": "Bitcoin daily move",
        "change_pct_GLD": "Gold daily move",
        "volatility_SIF": "Silver volatility",
        "volatility_GLD": "Gold volatility",
        "volatility_BTC_USD": "Bitcoin volatility",
        "avg_pos_prob": "Positive news tone",
        "avg_neg_prob": "Negative news tone",
        "net_sentiment": "Net news sentiment",
        "positive_count": "Positive news count",
        "negative_count": "Negative news count",
        "close_SIF": "Silver price level",
        "close_GLD": "Gold price level",
        "close_BTC_USD": "Bitcoin price level",
    }
    return mapping.get(name, name or "-")


@task(retries=2, retry_delay_seconds=10)
def check_api(name, url):
    logger = get_run_logger()
    logger.info(f"Checking {name}: {url}")

    r = requests.get(url, timeout=25)
    r.raise_for_status()

    write_audit("INFO", "api_check_success", {"name": name, "url": url, "status_code": r.status_code})
    return {"name": name, "url": url, "status_code": r.status_code, "ok": True}


@task
def run_modelling():
    logger = get_run_logger()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"market_flow_modelling_{run_id}.log"

    cmd = [sys.executable, str(ROOT / "rafah/modelling/market_flow_correlation.py")]

    env = os.environ.copy()
    env["SKIP_MODEL_TELEGRAM"] = "1"

    logger.info("Running market_flow_correlation.py")

    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    log_text = (
        "=== STDOUT ===\n"
        + proc.stdout
        + "\n\n=== STDERR ===\n"
        + proc.stderr
        + f"\n\nRETURN_CODE={proc.returncode}\n"
    )

    log_path.write_text(log_text, encoding="utf-8")

    if proc.returncode != 0:
        write_audit("FATAL", "modelling_failed", {"return_code": proc.returncode, "log_path": str(log_path)})
        raise RuntimeError(f"Modelling failed. See log: {log_path}")

    write_audit("INFO", "modelling_success", {"log_path": str(log_path)})
    return str(log_path)


@task
def data_quality_check():
    logger = get_run_logger()

    joined_path = OUT_DIR / "market_flow_joined_dataset.csv"
    prediction_path = OUT_DIR / "model_predictions_daily.csv"
    signal_path = OUT_DIR / "business_latest_signal.json"

    if not joined_path.exists():
        raise FileNotFoundError(f"Missing joined dataset: {joined_path}")

    df = pd.read_csv(joined_path)

    dq = {
        "created_at": now_iso(),
        "dataset": str(joined_path),
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "duplicate_date_count": int(df["date"].duplicated().sum()) if "date" in df.columns else None,
        "total_null_count": int(df.isna().sum().sum()),
        "null_count_by_column": {k: int(v) for k, v in df.isna().sum().to_dict().items()},
        "required_files": {
            "joined_dataset": joined_path.exists(),
            "model_predictions_daily": prediction_path.exists(),
            "business_latest_signal": signal_path.exists(),
        },
    }

    numeric_cols = [c for c in df.columns if c != "date"]
    invalid_numeric = {}

    for col in numeric_cols:
        converted = pd.to_numeric(df[col], errors="coerce")
        invalid_numeric[col] = int(converted.isna().sum() - df[col].isna().sum())

    dq["invalid_numeric_count_by_column"] = invalid_numeric

    warning_count = 0
    if dq["row_count"] == 0:
        warning_count += 1
    if dq["total_null_count"] > 0:
        warning_count += 1
    if dq["duplicate_date_count"] and dq["duplicate_date_count"] > 0:
        warning_count += 1
    if any(v > 0 for v in invalid_numeric.values()):
        warning_count += 1

    dq["status"] = "warning" if warning_count else "passed"
    dq["warning_count"] = warning_count

    dq_path = OUT_DIR / "data_quality_report.json"
    dq_path.write_text(json.dumps(dq, indent=2), encoding="utf-8")

    severity = "WARNING" if warning_count else "INFO"
    write_audit(severity, "data_quality_check", dq)

    logger.info(f"DQ status: {dq['status']} | rows={dq['row_count']} | nulls={dq['total_null_count']}")
    return dq


@task
def export_dashboard_outputs():
    logger = get_run_logger()

    PUBLIC_OUT_DIR.mkdir(parents=True, exist_ok=True)

    exported = []
    for src in OUT_DIR.glob("*"):
        if src.is_file():
            dst = PUBLIC_OUT_DIR / src.name
            shutil.copy2(src, dst)
            exported.append(str(dst))

    write_audit("INFO", "dashboard_outputs_exported", {"count": len(exported), "files": exported})
    logger.info(f"Exported {len(exported)} files to React public output folder.")
    return exported


@task
def send_success_and_event_alerts(dq_report):
    signal_path = OUT_DIR / "business_latest_signal.json"
    report_path = OUT_DIR / "market_flow_model_report.json"

    signal = read_json(signal_path)
    report = read_json(report_path)
    previous_signal = read_json(LAST_SIGNAL_PATH)

    predicted_direction = signal.get("predicted_direction") or report.get("latest_prediction", {}).get("predicted_direction", "-")
    predicted_change_pct = float(signal.get("predicted_change_pct") or report.get("latest_prediction", {}).get("predicted_change_pct", 0))
    confidence = signal.get("confidence") or report.get("latest_prediction", {}).get("confidence", "-")

    main_driver = signal.get("main_driver") or "-"
    main_driver_corr = signal.get("main_driver_correlation")

    try:
        main_driver_corr_float = float(main_driver_corr)
    except Exception:
        main_driver_corr_float = 0.0

    latest_date = signal.get("date") or report.get("latest_prediction", {}).get("date", "-")

    friendly = friendly_driver(main_driver)

    summary = (
        "✅ <b>Market Flow Pipeline Updated</b>\n\n"
        f"Date: <b>{latest_date}</b>\n"
        f"EUR/USD Signal: <b>{str(predicted_direction).title()}</b>\n"
        f"Predicted Change: <b>{predicted_change_pct:.4f}%</b>\n"
        f"Confidence: <b>{confidence}%</b>\n\n"
        f"Main Driver: <b>{friendly}</b>\n"
        f"Correlation: <b>{main_driver_corr_float:.4f}</b>\n\n"
        f"DQ Status: <b>{dq_report.get('status')}</b>\n"
        f"Rows Joined: <b>{report.get('rows_joined', '-')}</b>\n"
        "Dashboard output refreshed."
    )

    send_telegram(summary)
    write_audit("INFO", "telegram_success_summary_sent", {"predicted_direction": predicted_direction})

    previous_direction = previous_signal.get("predicted_direction")
    previous_driver = previous_signal.get("main_driver")

    if previous_direction and previous_direction != predicted_direction:
        msg = (
            "⚠️ <b>EUR/USD Signal Changed</b>\n\n"
            f"Previous: <b>{str(previous_direction).title()}</b>\n"
            f"Current: <b>{str(predicted_direction).title()}</b>\n\n"
            f"Predicted Change: <b>{predicted_change_pct:.4f}%</b>\n"
            f"Main Driver: <b>{friendly}</b>"
        )
        send_telegram(msg)
        write_audit("WARNING", "signal_changed_alert_sent", {
            "previous": previous_direction,
            "current": predicted_direction,
        })

    if previous_driver and previous_driver != main_driver:
        msg = (
            "🔁 <b>Main Driver Changed</b>\n\n"
            f"Previous: <b>{friendly_driver(previous_driver)}</b>\n"
            f"Current: <b>{friendly}</b>\n\n"
            f"Correlation: <b>{main_driver_corr_float:.4f}</b>"
        )
        send_telegram(msg)
        write_audit("WARNING", "main_driver_changed_alert_sent", {
            "previous": previous_driver,
            "current": main_driver,
        })

    if abs(predicted_change_pct) >= MOVEMENT_THRESHOLD:
        msg = (
            "🚨 <b>Strong EUR/USD Movement Signal</b>\n\n"
            f"Signal: <b>{str(predicted_direction).title()}</b>\n"
            f"Predicted Change: <b>{predicted_change_pct:.4f}%</b>\n"
            f"Main Driver: <b>{friendly}</b>\n"
            f"Threshold: <b>{MOVEMENT_THRESHOLD:.2f}%</b>"
        )
        send_telegram(msg)
        write_audit("WARNING", "strong_movement_alert_sent", {
            "predicted_change_pct": predicted_change_pct,
            "threshold": MOVEMENT_THRESHOLD,
        })

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LAST_SIGNAL_PATH.write_text(json.dumps(signal, indent=2), encoding="utf-8")

    return {
        "predicted_direction": predicted_direction,
        "predicted_change_pct": predicted_change_pct,
        "main_driver": main_driver,
    }


@flow(name="Market Flow Modelling Pipeline")
def market_flow_pipeline():
    run_id = str(uuid.uuid4())

    write_audit("INFO", "pipeline_started", {"run_id": run_id})

    try:
        check_api("News API", f"{NEWS_API}/api/sentiment/daily")
        check_api("Kurs API", f"{KURS_API}/kurs/daily")
        check_api("Commodity API", f"{COMMODITY_API}/commodity/daily?symbol=SI%3DF&limit=3")

        log_path = run_modelling()
        dq_report = data_quality_check()
        exported = export_dashboard_outputs()
        alert_result = send_success_and_event_alerts(dq_report)

        result = {
            "run_id": run_id,
            "status": "success",
            "log_path": log_path,
            "exported_count": len(exported),
            "data_quality": dq_report.get("status"),
            "alert_result": alert_result,
        }

        write_audit("INFO", "pipeline_success", result)
        return result

    except Exception as e:
        error_text = str(e)

        write_audit("FATAL", "pipeline_failed", {
            "run_id": run_id,
            "error": error_text,
        })

        send_telegram(
            "❌ <b>Market Flow Pipeline Failed</b>\n\n"
            f"Run ID: <code>{run_id}</code>\n"
            f"Error: <code>{error_text[:900]}</code>"
        )

        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true", help="Serve as scheduled Prefect deployment.")
    args = parser.parse_args()

    if args.serve:
        market_flow_pipeline.serve(
            name="market-flow-modelling-hourly",
            cron="0 * * * *",
        )
    else:
        market_flow_pipeline()
