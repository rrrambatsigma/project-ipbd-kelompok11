import os
import traceback
from datetime import datetime

import requests
from dotenv import load_dotenv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=True)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_MODELLING") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_failure_alert(message: str, title: str = "Market Flow Pipeline Failed") -> bool:
    """
    Send failure notification to Telegram.
    Safe to call from normal Python code or Prefect exception handler.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram env missing. Failure alert not sent.")
        return False

    text = (
        f"❌ <b>{title}</b>\n\n"
        f"Time: <code>{datetime.now().isoformat(timespec='seconds')}</code>\n"
        f"Error:\n<code>{str(message)[:900]}</code>"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=20)
        r.raise_for_status()
        print("[OK] Failure alert sent to Telegram.")
        return True
    except Exception as e:
        print(f"[WARN] Failed to send Telegram failure alert: {e}")
        return False


def prefect_failure_hook(flow, flow_run, state):
    """
    Prefect-compatible failure hook.
    This is intentionally defensive because Prefect passes different objects
    depending on version/context.
    """
    try:
        flow_name = getattr(flow, "name", "unknown-flow")
        run_name = getattr(flow_run, "name", "unknown-run")
        state_name = getattr(state, "name", "Failed")

        message = f"Flow: {flow_name}\nRun: {run_name}\nState: {state_name}"

        try:
            exc = state.result()
            if exc:
                message += f"\nException: {exc}"
        except Exception:
            pass

        return send_failure_alert(message)
    except Exception:
        print("[WARN] prefect_failure_hook failed:")
        traceback.print_exc()
        return False
