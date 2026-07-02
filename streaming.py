import yfinance as yf
import json
import time
from kafka import KafkaProducer
from telegram_notifier import notify_startup, notify_ingestion, notify_error, notify_shutdown

print("=== PIPELINE DATA INGESTION: KURS EUR ===")

# 1. Inisialisasi Kafka Producer — dengan retry otomatis
producer = None
for attempt in range(1, 6):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,
            retries=3
        )
        print("[INFO] Berhasil terhubung ke Apache Kafka pada localhost:29092.")
        notify_startup("streaming.py — Ingestion Producer")
        break
    except Exception as e:
        print(f"[WARN] Percobaan {attempt}/5 gagal: {e}")
        if attempt == 5:
            notify_error("streaming.py", str(e))
            print("[TIPS] Pastikan Docker Container Kafka kamu sudah menyala.")
            exit(1)
        print(f"[INFO] Coba lagi dalam 5 detik...")
        time.sleep(5)

TOPIC_NAME   = 'kurs_eur_stream'
tick_count   = 0
POLL_INTERVAL = 30  # detik antara setiap polling
TICKERS      = ["EURUSD=X", "EURIDR=X", "BTC-USD", "GLD"]
last_seen    = {}   # ticker -> harga terakhir yg sudah dikirim

def kirim_ke_kafka(message):
    global tick_count

    harga    = message.get('price')
    ticker   = message.get('id')
    waktu_ms = message.get('time')

    if harga and waktu_ms:
        payload = {
            "symbol":     ticker,
            "price":      float(harga),
            "event_time": waktu_ms,
            "source":     "Yahoo Finance"
        }

        try:
            producer.send(TOPIC_NAME, value=payload)
            tick_count += 1

            waktu_lokal = time.strftime('%H:%M:%S', time.localtime(int(waktu_ms) / 1000))
            print(f"[{waktu_lokal}] Data berhasil di-ingest ke Kafka -> {payload}")

            if ticker == "EURUSD=X":
                notify_ingestion(
                    symbol=ticker,
                    price=float(harga),
                    event_time=waktu_lokal,
                    tick_count=tick_count
                )

        except Exception as e:
            print(f"[ERROR] Gagal mengirim data ke Kafka: {e}")
            notify_error("streaming.py", str(e))


def poll_ticker(ticker: str):
    """Fetch harga terbaru dari yfinance (1-menit candle) dan kirim ke Kafka jika berubah."""
    global last_seen
    try:
        data = yf.Ticker(ticker).history(period="1d", interval="1m")
        if data.empty:
            return

        latest   = data.iloc[-1]
        price    = float(latest["Close"])
        ts_ms    = int(latest.name.timestamp() * 1000)

        last     = last_seen.get(ticker)
        # Kirim jika harga berbeda (minimal 0.0001 dari sebelumnya)
        if last is None or abs(last - price) > 1e-6:
            last_seen[ticker] = price
            msg = {"id": ticker, "price": price, "time": ts_ms}
            kirim_ke_kafka(msg)
    except Exception as e:
        print(f"[WARN] Gagal fetch {ticker}: {e}")


print(f"[INFO] Polling harga dari yfinance untuk ticker: {TICKERS}")
print(f"[INFO] Interval: {POLL_INTERVAL} detik. Tekan Ctrl+C untuk berhenti.\n")

try:
    while True:
        for ticker in TICKERS:
            poll_ticker(ticker)
        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("\n[INFO] Streaming dihentikan oleh pengguna.")
    notify_shutdown("streaming.py", {"total_tick": tick_count})
finally:
    print("[INFO] Menutup koneksi Kafka Producer.")
    producer.close()
