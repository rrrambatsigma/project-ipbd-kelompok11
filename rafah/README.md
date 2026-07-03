# RAFAH — Top 3 Commodity Stream
## IPBD Kelompok 11

Bagian **Rafah**: streaming harga komoditas **GLD (Gold)**, **BTC-USD (Bitcoin)**, **SI=F (Silver)** secara real-time.

---

## Ticker

| Symbol  | Nama      | Keterangan                  |
|---------|-----------|-----------------------------|
| `GLD`   | Gold      | SPDR Gold Shares ETF        |
| `BTC-USD` | Bitcoin | Bitcoin / US Dollar         |
| `SI=F`  | Silver    | Silver Futures (COMEX)      |

---

## Struktur File

```
rafah/
├── streaming_commodity.py        ← Producer: yfinance WebSocket → Kafka
├── spark_commodity.py            ← Consumer: Kafka → PostgreSQL (Bronze/Silver/Gold)
├── modelling/
│   ├── backfill_commodity.py     ← Backfill data historis yfinance 2021-2026
│   └── model_commodity.py        ← XGBoost baseline per komoditas
├── serving/
│   └── main_commodity.py         ← FastAPI REST API (port 8001)
└── grafana/
    └── push_dashboard_commodity.py ← Push dashboard Grafana
```

---

## Cara Jalankan

### 1. Pastikan Kafka & PostgreSQL running
```bash
docker-compose up -d zookeeper kafka postgres
```

### 2. Backfill data historis (sekali saja)
```bash
python3 rafah/modelling/backfill_commodity.py
```

### 3. Jalankan streaming (2 terminal)
```bash
# Terminal 1 — Producer
python3 rafah/streaming_commodity.py

# Terminal 2 — Consumer/Processor
python3 rafah/spark_commodity.py
```

### 4. Training model baseline
```bash
python3 rafah/modelling/model_commodity.py --ticker all
```

### 5. Jalankan API serving (port 8001)
```bash
uvicorn rafah.serving.main_commodity:app --host 0.0.0.0 --port 8001 --reload
```

### 6. Push Grafana dashboard
```bash
python3 rafah/grafana/push_dashboard_commodity.py
# Buka: http://localhost:3001
```

---

## Database Tables

| Tabel              | Layer  | Isi                                     |
|--------------------|--------|-----------------------------------------|
| `commodity_raw`    | Bronze | Tick mentah (validasi + dedup)          |
| `commodity_silver` | Silver | Aggregasi window 1 menit + fitur        |
| `commodity_daily`  | Gold   | Ringkasan harian + MA5/MA10 + label     |

---

## API Endpoints

| Endpoint                  | Keterangan                              |
|---------------------------|-----------------------------------------|
| `GET /commodity/latest`   | Tick terbaru semua komoditas            |
| `GET /commodity/daily`    | Data harian Gold layer                  |
| `GET /commodity/silver`   | Window aggregasi 1 menit                |
| `GET /predict/{symbol}`   | Prediksi arah harga besok (XGBoost)     |
| `GET /stats/summary`      | Ringkasan pipeline                      |
| `GET /docs`               | Swagger UI                              |

---

## Preprocessing Pipeline

1. Validasi schema (symbol, commodity, price, event_time, source)
2. Konversi timestamp (ms → datetime WIB)
3. Cleaning harga per ticker (batas wajar berbeda tiap komoditas)
4. Deduplication (symbol + event_time)
5. Window aggregation tumbling 1 menit
6. Feature engineering (price_change, volatility, MA)
7. Labeling: **naik** / **turun** / **stabil** (threshold ±0.5%)
8. Gold aggregation harian (UPSERT dengan MA5, MA10)
