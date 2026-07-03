"""
scheduler_watchdog.py — IPBD Kelompok 11
Periodic watchdog untuk pipeline gabungan analisis korelasi.

Fungsi:
  1. Cek Prefect failed runs dalam 24 jam terakhir
  2. Cek health API endpoints (News, Kurs, Commodity, Serving)
  3. Kirim ringkasan ke grup IPBD 11 GACOR jika ada masalah

Usage:
  python scheduler_watchdog.py            # 1x check
  python scheduler_watchdog.py --serve    # loop tiap 30 menit
  python scheduler_watchdog.py --interval 60  # loop tiap 60 menit
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

from failure_notifier import (
    FAILURE_BOT_TOKEN,
    FAILURE_CHAT_ID,
    check_failed_jobs,
    report_to_telegram,
)

# ── Konfigurasi API Endpoints ─────────────────────────────────
KURS_API      = os.getenv("KURS_API", "http://100.118.244.91:8002")
NEWS_API      = os.getenv("NEWS_API", "http://100.118.244.91:8000")
COMMODITY_API = os.getenv("COMMODITY_API", "http://100.92.242.101:8001")
SERVING_API   = os.getenv("SERVING_API", "http://localhost:8000")

TARGETS = {
    "Kurs API":      KURS_API,
    "News API":      NEWS_API,
    "Commodity API": COMMODITY_API,
    "Serving API":   SERVING_API,
}

WATCH_HOURS = int(os.getenv("WATCHDOG_HOURS", "24"))


def _wib_now() -> str:
    from datetime import timedelta
    now = datetime.now(timezone.utc) + timedelta(hours=7)
    return now.strftime("%d/%m/%Y %H:%M:%S")


# ═══════════════════════════════════════════
# API HEALTH CHECK
# ═══════════════════════════════════════════

def check_api_health() -> list[dict]:
    """Cek status semua API endpoints."""
    results = []
    for name, base_url in TARGETS.items():
        try:
            r = requests.get(f"{base_url}/", timeout=10)
            results.append({
                "name": name,
                "url": base_url,
                "ok": r.status_code < 500,
                "status_code": r.status_code,
                "error": None,
            })
        except Exception as e:
            results.append({
                "name": name,
                "url": base_url,
                "ok": False,
                "status_code": None,
                "error": str(e),
            })
    return results


def format_health_report(api_results: list[dict]) -> str:
    """Format hasil health check ke teks."""
    lines = [
        f"📡 <b>API Health Check — Pipeline Korelasi</b>",
        f"🕐 {_wib_now()} WIB",
    ]
    for r in api_results:
        icon = "✅" if r["ok"] else "❌"
        detail = f"HTTP {r['status_code']}" if r["status_code"] else r["error"]
        lines.append(f"{icon} <b>{r['name']}</b>: {detail}")
    return "\n".join(lines)


def send_telegram(message: str):
    """Kirim pesan ke Telegram."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{FAILURE_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": FAILURE_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"[Watchdog] Gagal kirim Telegram: {e}")


# ═══════════════════════════════════════════
# MAIN CHECK
# ═══════════════════════════════════════════

def run_check():
    """Jalankan 1 siklus pengecekan."""
    logger.info("[Watchdog] Memulai pengecekan pipeline...")

    # 1. Cek failed jobs via Prefect API
    failed = check_failed_jobs(hours=WATCH_HOURS)
    if failed:
        logger.warning(f"[Watchdog] Ditemukan {len(failed)} job gagal!")
        report_to_telegram(failed, hours=WATCH_HOURS)
    else:
        logger.info("[Watchdog] Tidak ada job gagal.")

    # 2. Cek health API
    api_results = check_api_health()
    down_apis = [r for r in api_results if not r["ok"]]
    if down_apis:
        logger.warning(f"[Watchdog] {len(down_apis)} API down!")
        msg = format_health_report(api_results)
        send_telegram(msg)
    else:
        logger.info("[Watchdog] Semua API sehat.")

    return len(failed), len(down_apis)


# ═══════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Watchdog pipeline gabungan analisis korelasi IPBD 11"
    )
    parser.add_argument(
        "--serve", action="store_true",
        help="Jalan sebagai service (loop tiap N menit)"
    )
    parser.add_argument(
        "--interval", type=int, default=30,
        help="Interval pengecekan dalam menit (default: 30)"
    )
    args = parser.parse_args()

    if args.serve:
        logger.info(f"[Watchdog] Service mode — interval {args.interval} menit")
        while True:
            run_check()
            logger.info(f"[Watchdog] Tidur {args.interval} menit...")
            time.sleep(args.interval * 60)
    else:
        run_check()


if __name__ == "__main__":
    main()
