"""
utils/telegram_alert.py
─────────────────────────────────────────────────────────
Telegram alert untuk Euro News Ingestion Pipeline
Bot : @rrrambatsigmanewsbot
─────────────────────────────────────────────────────────
"""

import os
import requests
from datetime import datetime, timezone
from loguru import logger

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8963908474:AAF9F7GZFd3Nn4mHVMENNl9XMrOsZEwlkas")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "5974165452")
TELEGRAM_API_URL   = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def _send(message: str, parse_mode: str = "HTML") -> bool:
    """Kirim pesan ke Telegram."""
    try:
        resp = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":              TELEGRAM_CHAT_ID,
                "text":                 message,
                "parse_mode":           parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.warning(f"[Telegram] Gagal kirim: {e}")
        return False


# ─────────────────────────────────────────
# ALERT PER ARTIKEL (seperti contoh kelompok 2)
# ─────────────────────────────────────────

def alert_article(article: dict) -> None:
    """
    Kirim notifikasi per artikel yang masuk ke MinIO.
    Format mirip kelompok 2 — field by field.

    Dipanggil dari minio_client.upload_article()
    """
    source       = article.get("source", "?").upper()
    title        = article.get("title", "-")[:120]
    url          = article.get("url", "-")
    published_at = article.get("published_at", "-")
    category     = article.get("category", "-")
    ingested_at  = article.get("_ingested_at", datetime.now(timezone.utc).isoformat())
    raw_text     = (article.get("raw_text") or "-")[:200]
    provider     = article.get("provider", "")
    tone         = article.get("tone", None)
    tier         = article.get("source_tier", "?")

    # Emoji per source
    source_emoji = {
        "ECB":      "🏦",
        "GUARDIAN": "📰",
        "GDELT":    "🌍",
        "NEWSAPI":  "📡",
    }.get(source, "📄")

    # Tone indicator (khusus GDELT)
    tone_line = ""
    if tone is not None:
        try:
            tone_val = float(tone)
            if tone_val > 1:
                tone_icon = "📈"
            elif tone_val < -1:
                tone_icon = "📉"
            else:
                tone_icon = "➡️"
            tone_line = f"\ntone: {tone_icon} {tone_val:.2f}"
        except Exception:
            pass

    provider_line = f"\nprovider: {provider}" if provider else ""

    message = (
        f"{source_emoji} <b>Euro News Data</b>\n"
        f"{'─' * 30}\n"
        f"source: <b>{source}</b> (Tier {tier})\n"
        f"title: {title}\n"
        f"url: <a href=\"{url}\">{url[:60]}...</a>\n"
        f"published_at: {published_at}"
        f"{provider_line}\n"
        f"category: {category}"
        f"{tone_line}\n"
        f"ingested_at: {ingested_at[:19].replace('T', ' ')} UTC\n"
        f"{'─' * 30}\n"
        f"raw_text:\n<i>{raw_text}...</i>"
    )

    _send(message)


# ─────────────────────────────────────────
# ALERT BATCH SUMMARY
# ─────────────────────────────────────────

def alert_batch_success(summary: dict) -> None:
    """Alert ringkasan setelah 1 sesi batch selesai."""
    session  = summary.get("session", "manual").upper()
    status   = summary.get("status", "UNKNOWN")
    uploaded = summary.get("total_uploaded", 0)
    failed   = summary.get("total_failed", 0)
    total_in = summary.get("total_input", 0)
    start    = summary.get("batch_start", "")[:19].replace("T", " ")
    end      = summary.get("batch_end",   "")[:19].replace("T", " ")

    status_emoji = "✅" if status == "SUCCESS" else "⚠️"

    per_source   = summary.get("per_source", [])
    source_lines = ""
    for s in per_source:
        src  = s.get("source", "?").upper()
        up   = s.get("total_uploaded", 0)
        fail = s.get("failed", 0)
        icon = "✅" if fail == 0 else "⚠️"
        source_lines += f"\n  {icon} <b>{src}</b>: {up} artikel"
        if fail > 0:
            source_lines += f" ({fail} gagal)"

    message = (
        f"{status_emoji} <b>Euro News Pipeline — Batch Selesai</b>\n"
        f"{'─' * 32}\n"
        f"🕐 sesi          : {session}\n"
        f"📅 start         : {start} UTC\n"
        f"📅 end           : {end} UTC\n"
        f"{'─' * 32}\n"
        f"📥 total_input   : {total_in}\n"
        f"💾 total_uploaded: {uploaded}\n"
        f"❌ total_failed  : {failed}\n"
        f"{'─' * 32}\n"
        f"📊 per_source    :{source_lines}\n"
        f"{'─' * 32}\n"
        f"🏁 status        : {status_emoji} {status}"
    )

    if _send(message):
        logger.success("[Telegram] Batch summary terkirim")


def alert_batch_failed(session: str, error: str) -> None:
    """Alert jika batch gagal total."""
    now = datetime.now(timezone.utc).isoformat()[:19].replace("T", " ")
    message = (
        f"🚨 <b>Euro News Pipeline — GAGAL</b>\n"
        f"{'─' * 32}\n"
        f"🕐 sesi    : {session.upper()}\n"
        f"📅 waktu   : {now} UTC\n"
        f"{'─' * 32}\n"
        f"❌ error:\n<code>{error[:300]}</code>\n"
        f"{'─' * 32}\n"
        f"⚡ Cek: http://localhost:4200"
    )
    if _send(message):
        logger.success("[Telegram] Alert gagal terkirim")


def alert_backfill_progress(year: int, month: int, uploaded: int, grand_total: int, pct: float) -> None:
    """Update progress backfill setiap 3 bulan."""
    message = (
        f"📦 <b>Backfill Progress</b>\n"
        f"{'─' * 32}\n"
        f"📅 selesai  : {year}-{month:02d}\n"
        f"💾 bulan ini: {uploaded} artikel\n"
        f"📊 total    : {grand_total} artikel\n"
        f"⏳ progress : {pct:.1f}%"
    )
    _send(message)


def alert_backfill_done(grand_total: int, start_date: str, end_date: str) -> None:
    """Notifikasi backfill selesai."""
    message = (
        f"🎉 <b>Backfill Selesai!</b>\n"
        f"{'─' * 32}\n"
        f"📅 periode      : {start_date} → {end_date}\n"
        f"💾 total artikel: {grand_total}\n"
        f"✅ Data siap untuk preprocessing Spark!"
    )
    _send(message)


if __name__ == "__main__":
    # Test kirim contoh artikel
    test_article = {
        "source":       "ecb",
        "source_tier":  1,
        "title":        "ECB holds interest rates steady amid easing inflation",
        "url":          "https://www.ecb.europa.eu/press/pr/date/2026/html/ecb.mp260608.en.html",
        "published_at": "2026-06-08T12:45:00+00:00",
        "category":     "press_releases",
        "raw_text":     "The Governing Council of the ECB decided to keep the three key interest rates unchanged. Inflation is on track to return sustainably to the 2% target.",
        "provider":     "ecb.europa.eu",
        "tone":         None,
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    print("Mengirim test artikel ke Telegram...")
    alert_article(test_article)

    print("Mengirim test batch summary...")
    alert_batch_success({
        "session":        "pre_market",
        "batch_start":    "2026-06-08T23:00:00Z",
        "batch_end":      "2026-06-08T23:04:32Z",
        "total_input":    57,
        "total_uploaded": 57,
        "total_failed":   0,
        "status":         "SUCCESS",
        "per_source": [
            {"source": "ecb",     "total_uploaded": 15, "failed": 0},
            {"source": "gdelt",   "total_uploaded": 30, "failed": 0},
            {"source": "newsapi", "total_uploaded": 12, "failed": 0},
        ],
    })
    print("Cek Telegram @rrrambatsigmanewsbot!")