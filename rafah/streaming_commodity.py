"""
rafah/streaming_commodity.py — IPBD Kelompok 11 (RAFAH)
Pipeline: yfinance WebSocket → Kafka

Ticker yang dipantau:
  GLD    → SPDR Gold Shares ETF (proxy harga emas)
  BTC-USD → Bitcoin / US Dollar
  SI=F   → Silver Futures (proxy harga perak/silver)

Topik Kafka: commodity_stream
"""

import yfinance as yf
import json
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from telegram_notifier import notify_startup, notify_ingestion, notify_error, notify_shutdown

print("=== PIPELINE DATA INGESTION: TOP 3 COMMODITY (RAFAH) ===")

# Mapping ticker → nama komoditas
TICKER_MAP = {
    "GLD":     "Gold",
    "BTC-USD": "Bitcoin",
    "SI=F":    "Silver",
}

TOPIC_NAME = "commodity_stream"

try:
    producer = KafkaProducer(
        bootstrap_servers=["localhost:29092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
        retries=3,
    )
    print("[INFO] Berhasil terhubung ke Apache Kafka pada localhost:29092.")
    notify_startup("rafah/streaming_commodity.py — Commodity Producer")
except Exception as e:
    print(f"[ERROR] Gagal terhubung ke Kafka: {e}")
    notify_error("streaming_commodity.py", str(e))
    exit(1)

tick_count = 0


def kirim_ke_kafka(message):
    global tick_count

    harga    = message.get("price")
    ticker   = message.get("id")
    waktu_ms = message.get("time")

    if harga and waktu_ms and ticker in TICKER_MAP:
        payload = {
            "symbol":    ticker,
            "commodity": TICKER_MAP[ticker],
            "price":     float(harga),
            "event_time": waktu_ms,
            "source":    "Yahoo Finance",
        }
        try:
            producer.send(TOPIC_NAME, value=payload)
            tick_count += 1
            waktu_lokal = time.strftime("%H:%M:%S", time.localtime(int(waktu_ms) / 1000))
            print(f"[{waktu_lokal}] {TICKER_MAP[ticker]:8s} ({ticker:8s}) | {float(harga):.4f}")

            # Notifikasi Telegram setiap ticker GLD (cooldown 30 detik)
            if ticker == "GLD":
                notify_ingestion(
                    symbol=f"{ticker} ({TICKER_MAP[ticker]})",
                    price=float(harga),
                    event_time=waktu_lokal,
                    tick_count=tick_count,
                )
        except Exception as e:
            print(f"[ERROR] Gagal kirim ke Kafka: {e}")
            notify_error("streaming_commodity.py", str(e))


print(f"[INFO] Membuka WebSocket yfinance untuk: {list(TICKER_MAP.keys())}")
print("[INFO] Menunggu data mengalir. Tekan Ctrl+C untuk berhenti.\n")

try:
    with yf.WebSocket() as ws:
        ws.subscribe(list(TICKER_MAP.keys()))
        ws.listen(kirim_ke_kafka)
except KeyboardInterrupt:
    print("\n[INFO] Streaming dihentikan.")
    notify_shutdown("streaming_commodity.py", {"total_tick": tick_count})
finally:
    print("[INFO] Menutup Kafka Producer.")
    producer.close()
