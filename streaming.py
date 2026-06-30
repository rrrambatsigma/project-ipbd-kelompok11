import yfinance as yf
import json
import time
from kafka import KafkaProducer

print("=== PIPELINE DATA INGESTION: KURS EUR ===")

# 1. Inisialisasi Kafka Producer ke port Docker lokal
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[INFO] Berhasil terhubung ke Apache Kafka pada localhost:29092.")
except Exception as e:
    print(f"[ERROR] Gagal terhubung ke Kafka: {e}")
    print("[TIPS] Pastikan Docker Container Kafka kamu sudah menyala.")
    exit(1)

# Nama topik Kafka yang akan digunakan
TOPIC_NAME = 'kurs_eur_stream'

def kirim_ke_kafka(message):
    harga = message.get('price')
    ticker = message.get('id')
    waktu_ms = message.get('time')
    
    if harga and waktu_ms:
        # TAHAP: Validasi Schema (Sesuai dokumen rencana projek Kelompok 11)
        # Memastikan kolom symbol, price, event_time, dan source tersedia
        payload = {
            "symbol": ticker,        # Ticker EUR (misal: EURUSD=X atau EURIDR=X)
            "price": float(harga),   # Nilai kurs harian / real-time
            "event_time": waktu_ms,  # Timestamp mentah (milidetik)
            "source": "Yahoo Finance" # Sumber data origin
        }
        
        try:
            # Kirim data JSON ke topik Kafka
            producer.send(TOPIC_NAME, value=payload)
            
            # Log indikator sukses di terminal kamu
            waktu_lokal = time.strftime('%H:%M:%S', time.localtime(int(waktu_ms) / 1000))
            print(f"[{waktu_lokal}] Data berhasil di-ingest ke Kafka -> {payload}")
        except Exception as e:
            print(f"[ERROR] Gagal mengirim data ke Kafka: {e}")

print(f"[INFO] Membuka WebSocket yfinance untuk ticker EUR...")
print("[INFO] Menunggu data mengalir dari pasar global. Tekan Ctrl+C untuk berhenti.\n")

# 2. Mulai streaming data lewat WebSocket
# Ticker yang dipantau:
#   EURUSD=X  -> EUR/USD (Euro ke Dollar Amerika)
#   EURIDR=X  -> EUR/IDR (Euro ke Rupiah Indonesia)
#   BTC-USD   -> Bitcoin sebagai aset pembanding
#   GLD       -> Gold ETF sebagai aset pembanding
try:
    with yf.WebSocket() as ws:
        ws.subscribe(["EURUSD=X", "EURIDR=X", "BTC-USD", "GLD"])
        ws.listen(kirim_ke_kafka)
except KeyboardInterrupt:
    print("\n[INFO] Streaming dihentikan oleh pengguna.")
finally:
    print("[INFO] Menutup koneksi Kafka Producer.")
    producer.close()
