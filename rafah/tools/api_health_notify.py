import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

KURS_API = os.getenv("KURS_API", "http://100.96.124.11:8000")
COMMODITY_API = os.getenv("COMMODITY_API", "http://100.96.124.11:8001")
NEWS_API = os.getenv("NEWS_API", "http://100.118.244.91:8000")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_MODELLING") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TARGETS = {
    "Kurs": KURS_API,
    "Commodity": COMMODITY_API,
    "News": NEWS_API,
}


def check_api(name, base_url):
    try:
        r = requests.get(f"{base_url}/", timeout=5)
        return {
            "name": name,
            "url": base_url,
            "ok": r.status_code < 500,
            "status_code": r.status_code,
            "error": None,
        }
    except Exception as e:
        return {
            "name": name,
            "url": base_url,
            "ok": False,
            "status_code": None,
            "error": str(e),
        }


def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is empty. Skipping Telegram send.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    r = requests.post(url, json=payload, timeout=10)
    r.raise_for_status()
    print("[OK] Telegram notification sent.")


def main():
    results = [check_api(name, url) for name, url in TARGETS.items()]

    lines = [
        "📡 <b>IPBD Kelompok 11 — Backend Health Check</b>",
        f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]

    for r in results:
        icon = "✅" if r["ok"] else "❌"
        detail = f"HTTP {r['status_code']}" if r["status_code"] else r["error"]
        lines.append(f"{icon} <b>{r['name']}</b>: {r['url']} — {detail}")

    msg = "\n".join(lines)
    print(msg.replace("<b>", "").replace("</b>", ""))

    # Send only if at least one backend is down, so it acts like alerting.
    if any(not r["ok"] for r in results):
        send_telegram(msg)
    else:
        print("[OK] All APIs reachable. Telegram alert not sent.")


if __name__ == "__main__":
    main()
