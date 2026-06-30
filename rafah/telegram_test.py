"""
rafah/telegram_test.py — IPBD Kelompok 11 (RAFAH)
Test kirim notifikasi Telegram untuk pipeline komoditas.

Jalankan dari root direktori:
    python3 rafah/telegram_test.py
"""

import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from telegram_notifier import notify_startup, notify_ingestion, notify_gold

notify_startup("Rafah — Commodity Pipeline Test")
time.sleep(1)
notify_ingestion("GLD (Gold)", 231.45, "12:00:00", 1)
time.sleep(1)
notify_gold([("GLD", "2026-06-30", 231.45, 0.012, "naik", 230.90)])
time.sleep(2)
print("✅ Telegram test selesai!")
