"""
failure_notifier.py — IPBD Kelompok 11
Bot notifikasi job scheduling gagal untuk pipeline gabungan analisis korelasi.

Mencakup:
  - Rambat (news): ingestion, preprocessing, modelling
  - Jojo (kurs): streaming, spark stream
  - Rafah (commodity): market flow, streaming, spark stream
  - Serving + Dashboard

Mode notifikasi:
  1. REAL-TIME: Prefect on_failure hook → langsung kirim ke grup
  2. PERIODIK: scheduler_watchdog.py tiap 30 menit query Prefect API
  3. MANUAL: check_failures.py --hours 48
"""

import os
import requests
from datetime import datetime, timezone
from loguru import logger

FAILURE_BOT_TOKEN = os.getenv("FAILURE_BOT_TOKEN", "8621358465:AAE5Pbny2yUzchVo6U9fjJW2eMfV1xmYYSA")
FAILURE_CHAT_ID   = os.getenv("FAILURE_CHAT_ID", "-1004360195236")
PREFECT_API_URL   = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")


def _wib_now() -> str:
    """Return timestamp dalam WIB (UTC+7)."""
    from datetime import timedelta
    now = datetime.now(timezone.utc) + timedelta(hours=7)
    return now.strftime("%d/%m/%Y %H:%M:%S")


# ═══════════════════════════════════════════
# FUNGSI KIRIM NOTIFIKASI
# ═══════════════════════════════════════════

def send_failure_alert(
    flow_name: str,
    error_message: str,
    run_id: str | None = None,
    task_name: str | None = None,
):
    """
    Kirim notifikasi failure ke grup Telegram IPBD 11 GACOR.

    Args:
        flow_name: Nama flow yang gagal
        error_message: Pesan error
        run_id: (opsional) ID run Prefect
        task_name: (opsional) Nama task yang gagal
    """
    msg = (
        "🚨 <b>JOB FAILED — IPBD Kelompok 11</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 Flow   : <code>{flow_name}</code>\n"
    )
    if run_id:
        msg += f"🆔 Run ID  : <code>{run_id[:8]}</code>\n"
    if task_name:
        msg += f"🔧 Task    : <code>{task_name}</code>\n"
    msg += (
        f"🕐 Waktu   : {_wib_now()} WIB\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"❌ Error:\n<code>{str(error_message)[:500]}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔗 Cek: http://localhost:4200"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{FAILURE_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": FAILURE_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"[FailureNotifier] Gagal kirim Telegram: {e}")


# ═══════════════════════════════════════════
# PREFECT ON_FAILURE HOOK
# ═══════════════════════════════════════════

def prefect_failure_hook(flow, flow_run, state):
    """
    Prefect on_failure hook.
    Otomatis dipanggil ketika flow masuk state FAILED.

    Dipasang di @flow(on_failure=[prefect_failure_hook])
    """
    flow_name = flow_run.flow_name
    run_id = str(flow_run.id)
    error_msg = (
        str(state.message)
        if state and state.message
        else str(getattr(state, 'result', 'Unknown error'))
    )
    logger.error(
        f"[FailureNotifier] Flow FAILED: {flow_name} | Run: {run_id} | Error: {error_msg[:200]}"
    )
    send_failure_alert(flow_name, error_msg, run_id=run_id)


# ═══════════════════════════════════════════
# QUERY PREFECT API — Cari Job Gagal
# ═══════════════════════════════════════════

def check_failed_jobs(hours: int = 24) -> list[dict]:
    """
    Query Prefect API untuk cari flow runs yang gagal dalam X jam terakhir.

    Returns:
        list[dict]: [{flow_name, run_id, error, timestamp}, ...]
    """
    try:
        resp = requests.get(
            f"{PREFECT_API_URL}/flow_runs",
            params={
                "state_type": "FAILED",
                "limit": 20,
                "sort": "EXPECTED_START_TIME_DESC",
            },
            timeout=10,
        )
        resp.raise_for_status()
        runs = resp.json()

        if not runs:
            return []

        results = []
        for run in runs:
            results.append({
                "flow_name": run.get("flow_name", "?"),
                "run_id": str(run.get("id", ""))[:8],
                "error": run.get("state", {}).get("message", "Unknown"),
                "timestamp": run.get("expected_start_time", ""),
            })
        return results
    except Exception as e:
        logger.warning(f"[FailureNotifier] Gagal query Prefect API: {e}")
        return []


def format_failure_report(failed_jobs: list[dict], hours: int = 24) -> str:
    """Format daftar job gagal jadi pesan Telegram."""
    if not failed_jobs:
        return (
            f"✅ <b>Tidak ada job gagal</b> dalam {hours} jam terakhir.\n"
            f"🕐 {_wib_now()} WIB"
        )

    lines = [
        f"🔍 <b>Job Failure Report — {hours}h terakhir</b>",
        f"🕐 {_wib_now()} WIB",
        f"📊 Total gagal: {len(failed_jobs)} flow(s)",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    for i, job in enumerate(failed_jobs[:5], 1):
        lines.append(
            f"{i}. <b>{job['flow_name']}</b>\n"
            f"   🆔 {job['run_id']} | ⏰ {job['timestamp'][:19]}\n"
            f"   ❌ {job['error'][:150]}"
        )
    if len(failed_jobs) > 5:
        lines.append(f"\n... dan {len(failed_jobs) - 5} lainnya")
    return "\n".join(lines)


def report_to_telegram(failed_jobs: list[dict], hours: int = 24):
    """Kirim report job gagal ke Telegram."""
    msg = format_failure_report(failed_jobs, hours)
    try:
        requests.post(
            f"https://api.telegram.org/bot{FAILURE_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": FAILURE_CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"[FailureNotifier] Gagal kirim report: {e}")
