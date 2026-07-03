import time
from telegram_notifier import notify_startup, notify_ingestion, notify_preprocessing, notify_gold

notify_startup("Test Pipeline — IPBD Kelompok 11")
time.sleep(1)

notify_ingestion("EURUSD=X", 1.14157, "11:45:00", 42)
time.sleep(1)

notify_preprocessing(50, 5, [
    {"symbol": "EURUSD=X", "price_change_pct": 0.0120, "volatility": 0.00005, "label": "menguat"},
    {"symbol": "BTC-USD",  "price_change_pct": -0.023, "volatility": 12.5,    "label": "melemah"},
])
time.sleep(1)

notify_gold([
    ("EURUSD=X", "2026-06-30", 1.14157, 0.012, "menguat", 1.13900),
])

time.sleep(2)
print("Semua notifikasi terkirim — cek Telegram kamu!")
