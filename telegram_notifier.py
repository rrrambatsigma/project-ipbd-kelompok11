"""
telegram_notifier.py — IPBD Kelompok 11 (JOJO)
Modul shared untuk kirim notifikasi ke Telegram bot.

Dipakai oleh:
  - streaming.py    (notifikasi ingestion)
  - spark_stream.py (notifikasi preprocessing & Gold layer)
"""

import requests
import threading
from datetime import datetime

# ── Konfigurasi Bot ───────────────────────────────────────────
BOT_TOKEN = "8601130724:AAHvGvUBi7Vhg8YVcBoewTYUDe5icUE_kCg"
CHAT_ID   = "6598491019"
BASE_URL  = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# Kirim max 1 notifikasi tiap N detik per kategori (anti spam)
COOLDOWN = {
    "ingestion":     30,   # notif ingestion setiap 30 detik
    "preprocessing": 60,   # notif preprocessing setiap 1 menit
    "gold":          300,  # notif gold setiap 5 menit
    "error":         10,   # notif error setiap 10 detik
    "startup":       0,    # selalu kirim
}
_last_sent = {}


def _can_send(category: str) -> bool:
    cooldown = COOLDOWN.get(category, 30)
    if cooldown == 0:
        return True
    last = _last_sent.get(category, 0)
    now = datetime.now().timestamp()
    if now - last >= cooldown:
        _last_sent[category] = now
        return True
    return False


def send(message: str, category: str = "ingestion", parse_mode: str = "HTML"):
    """
    Kirim pesan ke Telegram secara async (tidak block main thread).
    category: ingestion | preprocessing | gold | error | startup
    """
    if not _can_send(category):
        return

    def _send():
        try:
            requests.post(BASE_URL, data={
                "chat_id":    CHAT_ID,
                "text":       message,
                "parse_mode": parse_mode,
            }, timeout=5)
        except Exception:
            pass  # silent fail — jangan sampai notifikasi crash pipeline

    threading.Thread(target=_send, daemon=True).start()


# ── Template pesan ────────────────────────────────────────────

def notify_startup(service: str):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    send(
        f"🚀 <b>IPBD Kelompok 11 — Pipeline Aktif</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Service  : <code>{service}</code>\n"
        f"🕐 Waktu    : {now}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pipeline EUR/USD streaming dimulai.",
        category="startup"
    )


def notify_ingestion(symbol: str, price: float, event_time: str, tick_count: int):
    emoji = "📈" if "EUR" in symbol else "₿" if "BTC" in symbol else "🥇"
    send(
        f"{emoji} <b>Ingestion — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Harga    : <code>{price:.5f}</code>\n"
        f"🕐 Waktu    : {event_time}\n"
        f"📦 Tick ke  : {tick_count}\n"
        f"🔄 Kafka    : kurs_eur_stream ✅",
        category="ingestion"
    )


def notify_preprocessing(bronze_count: int, silver_count: int, windows: list):
    """
    windows = list of dict hasil silver
    """
    lines = []
    for w in windows[:3]:  # max 3 baris biar tidak terlalu panjang
        label_emoji = "📈" if w["label"] == "menguat" else "📉" if w["label"] == "melemah" else "➡️"
        lines.append(
            f"  {label_emoji} {w['symbol']:12s} "
            f"Δ{w['price_change_pct']:+.4f}% | "
            f"vol={w['volatility']:.5f} | {w['label']}"
        )

    window_text = "\n".join(lines) if lines else "  (belum ada window)"

    send(
        f"⚙️ <b>Preprocessing Selesai</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🥉 Bronze   : +{bronze_count} tick → kurs_raw\n"
        f"🥈 Silver   : +{silver_count} window → kurs_silver\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Window terbaru:</b>\n"
        f"{window_text}",
        category="preprocessing"
    )


def notify_gold(daily_entries: list):
    """
    daily_entries = list of (symbol, trade_date, close, chg_pct, label, ma5)
    """
    lines = []
    for entry in daily_entries[:3]:
        symbol, trade_date, close, chg_pct, label, ma5 = entry
        label_emoji = "📈" if label == "menguat" else "📉" if label == "melemah" else "➡️"
        lines.append(
            f"  {label_emoji} <b>{symbol}</b> | close={close:.5f} "
            f"| Δ{chg_pct:+.4f}% | MA5={ma5:.5f}\n"
            f"     Label: <b>{label.upper()}</b>"
        )

    entries_text = "\n".join(lines) if lines else "  (belum ada data)"
    now = datetime.now().strftime("%H:%M:%S")

    send(
        f"🥇 <b>Gold Layer Updated</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Waktu    : {now}\n"
        f"📊 Ringkasan harian:\n"
        f"{entries_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Data tersimpan di kurs_daily",
        category="gold"
    )


def notify_error(service: str, error_msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    send(
        f"🔴 <b>ERROR — {service}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Waktu : {now}\n"
        f"❌ Error  : <code>{str(error_msg)[:200]}</code>",
        category="error"
    )


def notify_shutdown(service: str, summary: dict):
    now = datetime.now().strftime("%H:%M:%S")
    send(
        f"🛑 <b>Pipeline Dihentikan — {service}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Waktu         : {now}\n"
        f"📦 Total tick    : {summary.get('total_tick', 0)}\n"
        f"🥉 Bronze flush  : {summary.get('bronze', 0)} baris\n"
        f"🥈 Silver flush  : {summary.get('silver', 0)} window\n"
        f"🥇 Gold flush    : {summary.get('gold', 0)} hari\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pipeline berhenti dengan aman ✅",
        category="startup"
    )
