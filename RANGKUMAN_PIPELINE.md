# RANGKUMAN PIPELINE — IPBD Kelompok 11

> **EUR/USD Kurs & Sentimen — End-to-End Big Data Pipeline**  
> Jojo (Kurs) | Rafah (Komoditas) | Rambat (Sentimen)

---

## 📌 Arsitektur Umum

```
yfinance ──► streaming.py (Kafka Producer)
                 │
                 ▼  topic: kurs_eur_stream
         spark_stream.py (Kafka Consumer)
                 │
       ┌─────────┼──────────┐
       ▼         ▼          ▼
   Bronze     Silver      Gold
  kurs_raw  kurs_silver kurs_daily
  (Postgres) (Postgres)  (+ MA5, MA10)
     │          │          │
     ▼          ▼          ▼
  MinIO       MinIO    v_market_signals (VIEW)
  Parquet    Parquet    │
                  ┌─────┼──────┐
                  ▼     ▼      ▼
              Jojo   Rafah   Rambat
             (Kurs) (Komodi) (Sentimen)
                  │
                  ▼
      model_offline.py (XGBoost)
      model_kurs.py (RandomForest)
                  │
                  ▼
         serving/main.py (FastAPI)
                  │
            ┌─────┴─────┐
            ▼           ▼
       Grafana      Telegram
     (Dashboard)  (Notifikasi)
```

---

## 1. INGESTION — `streaming.py`

**Sumber:** Yahoo Finance via `yfinance`  
**Broker:** Kafka topic `kurs_eur_stream` (localhost:29092)

**Ticker (4):**
| Symbol | Aset |
|--------|------|
| `EURUSD=X` | EUR/USD |
| `EURIDR=X` | EUR/IDR |
| `BTC-USD` | Bitcoin |
| `GLD` | Gold ETF |

**Alur:**
1. Kafka Producer dengan retry 5×
2. Loop **tiap 30 detik**, fetch 1-menit candle dari yfinance
3. Hanya kirim jika harga berubah (`delta > 1e-6`)
4. Payload JSON `{symbol, price, event_time, source}` → Kafka
5. Notifikasi Telegram tiap tick `EURUSD=X`

**Deduplication:** `last_seen` dict per ticker

---

## 2. PREPROCESSING — `spark_stream.py`

Consumer Kafka dengan **3 layer data (Bronze → Silver → Gold):**

### 🥉 Bronze `kurs_raw`
- Validasi schema & tipe data
- Cleaning: tolak `price <= 0` atau `price >= 1_000_000`
- Konversi timestamp: ms → WIB (UTC+7)
- Dedup: hash MD5 `symbol + price + event_time`
- **Output:** PostgreSQL + Parquet di MinIO `s3://kurs-eur/bronze/`

### 🥈 Silver `kurs_silver`
- **Tumbling window 60 detik**
- Aggregasi: open, close, avg, volatility (stddev), price_change, price_change_pct
- **Labeling:** `menguat` / `melemah` / `stabil` (threshold ±0.05%)
- **Output:** PostgreSQL + Parquet di MinIO `s3://kurs-eur/silver/`

### 🥇 Gold `kurs_daily`
- **UPSERT** per `(trade_date, symbol)`
- Hitung MA5 & MA10 dari histori
- `high_price = GREATEST(...)`, `low_price = LEAST(...)`
- **Output:** PostgreSQL

### 🔗 View `v_market_signals`
Gabungan tabel Jojo (kurs) + Rafah (komoditas) + Rambat (sentimen)

---

## 3. STORAGE

| Komponen | Port | Fungsi |
|----------|------|--------|
| **Zookeeper** | 22181 | Koordinator Kafka |
| **Kafka** (7.5.0) | 29092 | Message broker |
| **PostgreSQL 15** | 5433 | Serving database (`kurs_eur_db`) |
| **MinIO** | 9005 / 9006 | Data Lake (Parquet) |
| **Grafana** | 3001 | Dashboard monitoring |

**Database:** 10 tabel + 1 view (`db_schema.sql`)

---

## 4. MODELLING

Dua model untuk **prediksi arah EUR/USD (t+1):**

### 🅰️ XGBoost (Main) — `model_offline.py`
| Metrik | Nilai |
|--------|-------|
| **Accuracy** | **55.48%** |
| CV 5-fold | 56.34% ± 8.58% |
| **24 fitur** | 15 kurs + 9 sentimen |
| Threshold | ±0.3% |
| SMOTE | Oversampling kelas minoritas |

**Top 3 Feature Importance:**
1. `high_low_range` — 0.1333
2. `volatility` — 0.0663
3. `sentiment_lag2` — 0.0454

### 🅱️ RandomForest (v1) — `model_kurs.py`
- CLI: `--mode train | predict | evaluate`
- 14 fitur (4 kurs + 5 komoditas + 5 sentimen)

### 📊 Analisis Korelasi — `analisis_korelasi.py`
8 visualisasi + laporan teks:
1. Time series overlay EUR/USD + sentimen
2. Korelasi heatmap
3. Scatter sentimen vs kurs (lag-0, -1, -2)
4. Lag analysis (Lag-2 terbaik: r=0.077**)
5. Distribusi label per sentimen
6. Feature importance XGBoost
7. SHAP summary
8. Dashboard summary (top korelasi, rolling accuracy, tren tahunan, pie label)

---

## 5. SERVING — `serving/main.py`

**FastAPI** dengan **10+ endpoint**:

| Method | Endpoint | Fungsi |
|--------|----------|--------|
| GET | `/` | Status + daftar endpoint |
| GET | `/kurs/latest` | Real-time ticks (Bronze) |
| GET | `/kurs/daily` | Data harian (Gold) dengan filter tanggal |
| GET | `/kurs/silver` | Window 1 menit |
| GET | `/market/signals` | Gabungan kurs + komoditas + sentimen |
| GET | `/market/signals/latest` | Sinyal terbaru + ringkasan |
| GET | `/predict/today` | Prediksi RandomForest |
| GET | `/predict/today/xgb` | Prediksi XGBoost (24 fitur) |
| GET | `/stats/summary` | Statistik pipeline |

---

## 6. NOTIFICATION — `telegram_notifier.py`

**Telegram Bot** dengan **cooldown anti-spam:**

| Kategori | Cooldown | Isi Pesan |
|----------|----------|-----------|
| `startup` | 0s | Service started |
| `ingestion` | 30s | Harga + tick count |
| `preprocessing` | 60s | Bronze count + silver window |
| `gold` | 300s | Ringkasan harian + MA5 |
| `error` | 10s | Error message |
| `shutdown` | 0s | Summary sesi |

Semua notifikasi **async thread** (tidak blocking pipeline).

---

## 7. MONITORING — Grafana

**Dashboard 9 panel** (push via API):
1. EUR/USD Close Price time series
2. Pie chart distribusi label
3. Sentimen harian Rambat (bar)
4. Total artikel per hari
5. Tabel korelasi 30 hari
6. Stat cards: total ticks, windows, hari kurs, artikel

**Provisioning:** Auto via YAML (datasource PostgreSQL + dashboard provider)

---

## 8. LOGGING

**Tidak ada logging terpusat** (file log / ELK).  
Output hanya via **stdout (print)** dan **Telegram notifikasi error**.

---

## 9. SCHEDULER

**Tidak ada scheduler otomatis.** Semua proses berjalan **manual/ad-hoc:**
- `streaming.py` / `spark_stream.py` → manual start, stop via `Ctrl+C`
- `model_offline.py` → on-demand
- `serving/main.py` → manual via uvicorn

---

## 10. TECH STACK

| Layer | Tools |
|-------|-------|
| **Streaming** | Kafka, ZooKeeper, yfinance, kafka-python-ng |
| **Database** | PostgreSQL 15, psycopg2 |
| **Data Lake** | MinIO (S3), PyArrow (Parquet), boto3 |
| **Processing** | pandas, numpy |
| **ML** | XGBoost, scikit-learn, imbalanced-learn, SHAP, joblib |
| **Visualisasi** | matplotlib, seaborn |
| **Serving** | FastAPI, Uvicorn |
| **Monitoring** | Grafana |
| **Notifikasi** | Telegram Bot API, requests |
| **Infra** | Docker Compose |

---

## 📂 Struktur File

```
root/
├── streaming.py              # Kafka Producer (ingestion)
├── spark_stream.py           # Kafka Consumer (Bronze/Silver/Gold)
├── telegram_notifier.py      # Shared Telegram notifier
├── docker-compose.yml        # Infra (ZK, Kafka, PG, MinIO, Grafana)
├── db_schema.sql             # Full schema (10 tables + 1 view)
├── requirements.txt          # 38 dependencies

├── modelling/
│   ├── model_offline.py      # XGBoost (main model, 24 fitur)
│   ├── model_kurs.py         # RandomForest (v1, 14 fitur)
│   ├── analisis_korelasi.py  # 8 visualisasi + report
│   ├── backfill_daily.py     # Backfill yfinance 2021-now
│   ├── load_rambat_sentiment.py  # Load sentimen Rambat
│   ├── xgb_baseline.pkl      # Trained XGBoost
│   ├── rf_model.pkl          # Trained RandomForest
│   ├── label_encoder.pkl     # LabelEncoder
│   ├── DOKUMENTASI_MODELLING.md
│   └── visualisasi/          # 8 PNG plots

├── serving/
│   └── main.py               # FastAPI (10+ endpoints)

└── grafana/
    ├── push_dashboard.py     # Push 9-panel dashboard
    └── provisioning/         # Auto-config YAML
```

---

> **Dibuat untuk tugas mata kuliah Infrastruktur dan Platform Big Data**  
> IPBD Kelompok 11 — 2026
