# Market Flow Analysis — EUR/USD, Sentimen Berita & Komoditas
### Kelompok 11 — Infrastruktur dan Platform Big Data

Pipeline **end-to-end paralel 3 peran** untuk menganalisis korelasi antara pergerakan nilai tukar **EUR/USD (Y)** dengan **sentimen berita Euro (X₁)** dan **harga komoditas (X₂)**.

| Peran | Tanggung Jawab | Teknologi |
|-------|----------------|-----------|
| **Rambat** | Sentimen berita Euro | Ingestion 4 sumber → Spark 7-layer preprocessing → LDA + VADER + TF-IDF |
| **Jojo** | Kurs EUR/USD + Analisis Korelasi | yfinance → Kafka → feature engineering → XGBoost |
| **Rafah** | Komoditas (Gold, BTC, Silver) | yfinance → Kafka → window aggregation → XGBoost per komoditas |

Pipeline diorkestrasi oleh **Prefect**, disimpan di **MinIO** (S3-compatible), dimonitor via **Prometheus + Grafana**, dengan notifikasi **Telegram Bot**.

---

## Arsitektur End-to-End

```
                       ┌──────────────────────────────────────────────────────┐
                       │              INFRASTRUKTUR BERSAMA                   │
                       │  PostgreSQL :5433 │ MinIO :9000/9001                 │
                       │  Kafka :29092 │ Grafana :3001                        │
                       └──────────────────────────────────────────────────────┘

 ┌────────── RAMBAT ──────────┐  ┌────────── JOJO ────────────┐  ┌────────── RAFAH ──────────┐
 │                            │  │                            │  │                            │
 │ 4 Sumber Berita            │  │ yfinance WebSocket          │  │ yfinance WebSocket         │
 │ ECB, GDELT, Guardian,      │  │ EURUSD=X, EURIDR=X         │  │ GLD, BTC-USD, SI=F         │
 │ NewsAPI                    │  │       │                    │  │       │                    │
 │       │                    │  │       ▼                    │  │       ▼                    │
 │       ▼                    │  │    Kafka                   │  │    Kafka                   │
 │  MinIO news-raw/           │  │ kurs_eur_stream            │  │ commodity_stream           │
 │       │                    │  │       │                    │  │       │                    │
 │       ▼                    │  │       ▼                    │  │       ▼                    │
 │  Spark 7-Layer Pipeline    │  │ spark_stream.py            │  │ spark_commodity.py         │
 │  LOAD LANGUAGE TOPIC       │  │ Validasi Dedup Window      │  │ Validasi Dedup Window      │
 │  QUALITY DEDUP VADER AGGR  │  │ Feature Eng Label          │  │ Feature Eng Label          │
 │       │                    │  │       │                    │  │       │                    │
 │       ▼                    │  │       ▼                    │  │       ▼                    │
 │  MinIO news-processed/     │  │  PostgreSQL kurs_daily     │  │  PostgreSQL commodity_daily│
 │  articles/ aggregated/     │  │  MA5 MA10 label            │  │  MA5 MA10 label            │
 │       │                    │  │       │                    │  │       │                    │
 │       ▼                    │  │       ▼                    │  │       ▼                    │
 │  LDA Topic Modeling        │  │  XGBoost Classifier        │  │  XGBoost per Symbol        │
 │  TF-IDF/LR Classifier      │  │  24 fitur kurs+sentimen    │  │  21 fitur OHLCV+lag+rolling│
 │       │                    │  │                            │  │                            │
 │       ▼                    │  │  FastAPI Serving :8000     │  │  FastAPI Serving :8001     │
 │  FastAPI Serving :8000     │  │  (main.py - PostgreSQL)    │  │  (main_commodity.py)       │
 │  (app.py - MinIO read)     │  │                            │  │                            │
 └────────────────────────────┘  └────────────┬───────────────┘  └──────────────┬─────────────┘
                                              │                                 │
                                              └──────────┬──────────────────────┘
                                                         │
                                                    ┌────▼────┐
                                                    │  JOIN   │
                                                    │ v_market│
                                                    │ _signals│
                                                    └────┬────┘
                                                         │
                                              ┌──────────▼──────────┐
                                              │  ANALISIS KORELASI   │
                                              │  analisis_korelasi.py│
                                              │                     │
                                              │  Pearson/Spearman    │
                                              │  Lag Analysis       │
                                              │  XGBoost + SHAP     │
                                              │  8 Visualisasi PNG  │
                                              │  Laporan Teks       │
                                              └─────────────────────┘
```

---

## Struktur Folder

```
E:\project-ipbd-kelompok11\
│
├── README.md                         ← Dokumentasi ini
├── rangkuman.md                      ← Dokumentasi lengkap
├── db_schema.sql                     ← Schema PostgreSQL bersama (3 peran)
├── docker-compose.yml                ← Infrastructure: Zookeeper, Kafka, PostgreSQL, MinIO, Grafana
├── requirements.txt                  ← Dependencies gabungan
├── .env.example                      ← Template environment variables
│
├── telegram_notifier.py              ← Utility notifikasi Telegram
├── streaming.py                      ← JOJO: yfinance WebSocket → Kafka producer
├── spark_stream.py                   ← JOJO: Kafka consumer → PostgreSQL Bronze/Silver/Gold
├── check_backend.sh                  ← Script health check API
├── test_telegram.py                  ← Test notifikasi Telegram
│
├── ingestion/                        ← RAMBAT: Ingestion Berita
│   ├── docker-compose.yml            ← 10 services (PostgreSQL, Prefect, MinIO, Selenium, Prometheus, Grafana)
│   ├── prometheus.yml
│   └── collector/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── flows/
│           ├── news_ingestion_flow.py    ← Prefect flow utama (5 batch harian)
│           ├── backfill_flow.py          ← Backfill Guardian 2021-2025
│           ├── prefect.yaml              ← 5 deployment schedules
│           ├── scrapers/
│           │   ├── ecb_scraper.py        ← Tier 1: ECB RSS (4 feeds)
│           │   ├── gdelt_scraper.py      ← Tier 2: GDELT RSS + Guardian API
│           │   ├── newsapi_scraper.py    ← Tier 3: NewsAPI REST
│           │   └── reuters_scraper.py    ← Tier 2: Reuters RSS
│           ├── storage/
│           │   └── minio_client.py       ← MinIO upload + dedup
│           └── utils/
│               ├── config.py
│               ├── telegram_alert.py
│               └── metrics.py            ← Prometheus pushgateway
│
├── preprocessing/                    ← RAMBAT: Preprocessing Sentimen
│   ├── docker-compose.yml            ← 3 services (Spark, Modelling, Serving)
│   └── spark/
│       ├── Dockerfile                ← python:3.11 + JDK17 + fastText + NLTK
│       ├── requirements.txt
│       ├── entrypoint.sh
│       ├── ALUR_PIPELINE.md          ← Dokumentasi detail preprocessing
│       └── jobs/
│           ├── news_sentiment_job.py     ← 7-Layer Pipeline utama
│           ├── news_preprocessing_job.py ← Legacy pipeline (6 step)
│           ├── preprocessing_flow.py     ← Prefect flow wrapper
│           ├── sentiment_udfs.py         ← Semua pandas_udf (VADER, NLP, keyword, session)
│           ├── lang_filter.py            ← fastText English detection
│           ├── minio_utils.py            ← SparkSession + S3A + boto3
│           ├── schema.py                 ← RAW_SCHEMA + PROCESSED_SCHEMA
│           ├── prefect.yaml
│           └── prefect_bootstrap.py
│
├── modelling/                       ← JOJO: Kurs + Analisis Korelasi
│   ├── README.md                    ← Dokumentasi modelling
│   ├── DOKUMENTASI_MODELLING.md     ← Dokumentasi Jojo
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── prefect.yaml
│   │
│   ├── backfill_daily.py            ← Backfill EUR/USD historis dari yfinance (2021-2026)
│   ├── model_offline.py             ← XGBoost (24 fitur: 15 kurs + 9 sentimen) + SMOTE + TimeSeriesSplit
│   ├── model_kurs.py                ← Random Forest alternative
│   ├── analisis_korelasi.py         ← ANALISIS KORELASI (Pearson, Spearman, Lag, XGBoost, SHAP)
│   ├── load_rambat_sentiment.py     ← Load data sentimen Rambat ke PostgreSQL
│   │
│   ├── data_loader.py               ← Baca parquet dari MinIO via pyarrow
│   ├── lda_pipeline.py              ← LDA Topic Modeling
│   ├── sentiment_trainer.py         ← TF-IDF + LogisticRegression
│   ├── evaluator.py                 ← Metrics & confusion matrix
│   ├── model_store.py               ← Save/load .pkl ke MinIO
│   ├── monitor.py                   ← Tracking metrics
│   ├── run_pipeline.py              ← Pipeline modelling Rambat (6 step)
│   │
│   ├── config.py
│   ├── modelling_flow.py
│   ├── check_data.py
│   ├── test_stream_data.py
│   │
│   ├── visualisasi/                 ← Output 8 grafik PNG analisis korelasi
│   ├── baseline_report.txt          ← Laporan baseline accuracy
│   └── analisis_report.txt          ← Laporan analisis korelasi
│
├── rafah/                           ← RAFAH: Komoditas
│   ├── README.md
│   ├── docker_setup.md
│   │
│   ├── streaming_commodity.py       ← yfinance WebSocket → Kafka (GLD, BTC-USD, SI=F)
│   ├── spark_commodity.py           ← Kafka → PostgreSQL Bronze/Silver/Gold + MinIO Parquet
│   │
│   ├── modelling/
│   │   ├── backfill_commodity.py    ← Backfill komoditas historis dari yfinance
│   │   ├── model_commodity.py       ← XGBoost per komoditas
│   │   ├── load_sentiment_daily.py  ← Load sentimen Rambat
│   │   ├── test_commodity_data.py
│   │   ├── market_flow_correlation.py    ← Cross-pipeline modelling
│   │   ├── market_flow_lag_analysis.py   ← Lag analysis gabungan
│   │   └── market_flow_outputs/          ← Output modelling
│   │
│   ├── serving/
│   │   └── main_commodity.py        ← FastAPI port 8001
│   │
│   ├── grafana/
│   │   ├── push_dashboard_commodity.py
│   │   ├── create_kurs_commodity_dashboard.py
│   │   └── load_market_flow_joined_to_postgres.py
│   │
│   ├── monitoring/
│   │   ├── docker-compose.yml       ← Prometheus + Grafana monitoring
│   │   ├── prometheus.yml
│   │   ├── metrics_exporter.py
│   │   └── grafana/provisioning/
│   │
│   └── orchestration/
│       └── prefect_market_flow.py   ← Prefect flow market modelling
│
├── serving/                         ← JOJO: Serving API
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                       ← Sentimen-only API (baca MinIO)
│   └── main.py                      ← Full API (PostgreSQL: kurs + market signals + prediksi)
│
└── grafana/                         ← Dashboard Monitoring
    ├── provisioning/
    │   ├── datasources/
    │   └── dashboards/
    └── push_dashboard.py
```

---

## Cara Menjalankan

### Persiapan Awal

```powershell
# Clone repo
git clone <repo-url>
cd project-ipbd-kelompok11

# Setup environment
copy .env.example .env
# Isi variabel environment yang diperlukan (API key, dll)

# Install dependencies
pip install -r requirements.txt
pip install -r ingestion/collector/requirements.txt
pip install -r modelling/requirements.txt
pip install -r serving/requirements.txt
```

### Mode 1: Infrastructure Dasar

```powershell
# Start shared infrastructure
docker compose up -d postgres minio kafka grafana

# Buat schema database
docker exec -i postgres_kurs_eur psql -U <username> -d <db_name> < db_schema.sql
```

### Mode 2: Data Pipeline (Backfill Historis)

```powershell
# JOJO — Backfill data kurs EUR/USD (2021-2026)
python modelling/backfill_daily.py

# RAFAH — Backfill data komoditas (2021-2026)
python rafah/modelling/backfill_commodity.py

# RAMBAT — Load sentimen ke PostgreSQL
python modelling/load_rambat_sentiment.py
```

### Mode 3: Training Model

```powershell
# JOJO — XGBoost kurs + sentimen
python modelling/model_offline.py

# RAFAH — XGBoost per komoditas
python rafah/modelling/model_commodity.py --ticker all
```

### Mode 4: Analisis Korelasi

```powershell
python modelling/analisis_korelasi.py
```

### Mode 5: Serving API

```powershell
# JOJO + RAMBAT API (port 8000)
uvicorn serving.main:app --host 0.0.0.0 --port 8000

# RAFAH API (port 8001)
uvicorn rafah.serving.main_commodity:app --host 0.0.0.0 --port 8001
```

### Mode 6: Full Pipeline (Rambat — Sentimen)

```powershell
# Start ingestion stack
cd ingestion
docker compose up -d
cd ..

# Start preprocessing stack
cd preprocessing
docker compose up -d
cd ..

# Jalankan Spark preprocessing
docker exec preprocessing-spark spark-submit ^
  --packages org.apache.hadoop:hadoop-aws:3.4.2 ^
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py ^
  jobs/news_sentiment_job.py --raw

# Jalankan modelling Rambat
python modelling/run_pipeline.py

# Serving
uvicorn serving.app:app --host 0.0.0.0 --port 8000
```

### Mode 7: Live Streaming (Jojo — Kurs)

```powershell
# Terminal 1: yfinance → Kafka
python streaming.py

# Terminal 2: Kafka → PostgreSQL
python spark_stream.py
```

### Mode 8: Live Streaming (Rafah — Komoditas)

```powershell
# Terminal 1: yfinance → Kafka
python rafah/streaming_commodity.py

# Terminal 2: Kafka → PostgreSQL
python rafah/spark_commodity.py
```

---

## Pipeline Detail

### Rambat — Sentimen Berita

**Ingestion (5 batch/hari — 06:00-21:00 WIB):**
| Waktu WIB | Sesi | Lookback |
|-----------|------|----------|
| 06:00 | Pre-market | 7 jam |
| 09:00 | Open | 3 jam |
| 13:00 | Mid | 4 jam |
| 17:00 | Pre-close | 4 jam |
| 21:00 | Overlap | 4 jam |

**Sumber data:**
- **Tier 1:** ECB — RSS feed (Economic Bulletin, Press Releases, Speeches, Working Papers)
- **Tier 2:** GDELT — RSS query API | The Guardian — REST API
- **Tier 3:** NewsAPI — REST API

**Preprocessing 7 Layer (PySpark):**
```
LOAD (10.126) → LANGUAGE (9.600) → TOPIC (3.394) → QUALITY (3.090)
→ DEDUP (3.000) → VADER SENTIMENT → AGGR (2.121)
```

| Layer | Fungsi |
|-------|--------|
| LOAD | Baca JSON dari MinIO, parse nested fields |
| LANGUAGE | fastText lid.176 — filter English (p>0.5) |
| TOPIC | Regex EUR/USD + keyword finansial (threshold >=3) |
| QUALITY | Panjang teks 100-15000, non-ASCII ratio, boilerplate |
| DEDUP | Exact title dedup + window function |
| SENTIMENT | VADER scoring (compound, pos, neg, neu) |
| AGGR | Group by date + session tag → 2121 baris agregasi |

**Output ke MinIO:**
```
news-processed/sentiment/
├── articles/                         ← Parquet, partition by year/month
└── aggregated/
    ├── sentiment_by_session/         ← Parquet
    └── sentiment_by_session_csv/     ← CSV
```

### Jojo — Kurs EUR/USD

**Streaming Pipeline:**
```
yfinance WebSocket → Kafka (kurs_eur_stream)
  → spark_stream.py (8-step preprocessing)
    → Bronze: kurs_raw (PostgreSQL + MinIO Parquet)
    → Silver: kurs_silver (PostgreSQL + MinIO Parquet)
    → Gold: kurs_daily (PostgreSQL — MA5, MA10, label)
```

**Modelling — XGBoost:**
- **Target:** Prediksi label EUR/USD hari berikutnya (menguat/melemah/stabil)
- **Threshold:** ±0.3%
- **24 fitur:** 15 fitur teknikal kurs + 9 fitur sentimen Rambat
- **SMOTE:** Oversampling kelas minoritas
- **TimeSeriesSplit:** 5-fold cross-validation
- **Akurasi:** 56.89% (baseline random 33.3%)

**Analisis Korelasi:**
- Pearson & Spearman correlation (sentimen vs price_change_pct)
- Lag analysis (lag-2 sentimen r=0.077**, signifikan)
- Feature importance XGBoost + SHAP
- 8 visualisasi output

### Rafah — Komoditas

**Streaming Pipeline:**
```
yfinance WebSocket (GLD, BTC-USD, SI=F) → Kafka (commodity_stream)
  → spark_commodity.py (8-step preprocessing)
    → Bronze: commodity_raw
    → Silver: commodity_silver (window 1 menit)
    → Gold: commodity_daily (MA5, MA10, label naik/turun/stabil)
```

**Modelling — XGBoost per komoditas:**
- **Threshold:** ±0.5%
- **21 fitur:** OHLCV ranges, lag, rolling, momentum, sentiment ratios
- Target label: naik / turun / stabil

---

## Analisis Korelasi

**Input:** `analisis_korelasi.py` membaca dari PostgreSQL:
- `kurs_daily` — data harian EUR/USD (Jojo)
- `sentiment_daily` — agregasi sentimen harian (Rambat)

**Output:**

| File | Deskripsi |
|------|-----------|
| `visualisasi/1_timeseries_overlay.png` | Time series EUR/USD + sentimen + price change |
| `visualisasi/2_korelasi_heatmap.png` | Heatmap Pearson semua variabel |
| `visualisasi/3_scatter_sentiment_vs_kurs.png` | Scatter lag-0, lag-1, lag-2 + bar chart per label |
| `visualisasi/4_lag_analysis.png` | Bar chart korelasi lag 0-3 |
| `visualisasi/5_label_distribution_by_sentiment.png` | Stacked bar + heatmap sentimen label kurs |
| `visualisasi/6_feature_importance_xgboost.png` | XGBoost feature importance |
| `visualisasi/7_shap_summary.png` | SHAP feature importance |
| `visualisasi/8_dashboard_summary.png` | Dashboard: top korelasi, rolling accuracy, tren tahunan, pie label |
| `analisis_report.txt` | Laporan teks lengkap (Pearson, Spearman, lag analysis, insight) |

**Hasil Korelasi:**
| Fitur | Pearson r | Signifikansi |
|-------|-----------|-------------|
| avg_sentiment (lag-2) | 0.0774 | ** (p=0.0036) |
| has_ecb | 0.0512 | ns (p=0.0538) |
| has_interest_rate | 0.0322 | ns |
| avg_sentiment (lag-0) | 0.0019 | ns |

---

## Hasil Model Baseline

| Metrik | Nilai |
|--------|-------|
| Total data | 1.411 hari (2021-01-18 → 2026-06-30) |
| Train / Test | 1.128 / 283 |
| Test period | 2025-05-27 → 2026-06-29 |
| **Accuracy** | **56.89%** |
| CV 5-fold mean | 55.74% ± 7.93% |
| Top feature | `high_low_range` (0.1500) |
| Sentimen terkuat | `sentiment_lag2` (0.0467) |

---

## Service & Ports

| Service | Port | Deskripsi |
|---------|------|-----------|
| PostgreSQL | 5433 | Database bersama (kurs_daily, sentiment_daily, commodity_daily) |
| MinIO API | 9000 | S3-compatible object storage |
| MinIO Console | 9001 | Web UI MinIO |
| Kafka | 29092 | Message broker untuk streaming |
| Grafana | 3001 | Dashboard monitoring |
| FastAPI (Jojo + Rambat) | 8000 | API serving utama |
| FastAPI (Rafah) | 8001 | API serving komoditas |
| Prefect Server | 4200 | Orchestration UI |
| Prometheus | 9090 | Metrics time-series |
| Pushgateway | 9091 | Metrics receiver batch jobs |

---

## Jadwal Harian (WIB)

| Waktu | Komponen | Durasi | Tools |
|-------|----------|--------|-------|
| 06:00 | Ingestion Pre-market | ~5 menit | Prefect |
| 09:00 | Ingestion Open | ~5 menit | Prefect |
| 13:00 | Ingestion Mid | ~5 menit | Prefect |
| 17:00 | Ingestion Pre-close | ~5 menit | Prefect |
| 21:00 | Ingestion Overlap | ~5 menit | Prefect |
| **22:00** | **Preprocessing Spark** | ~15 menit | PySpark |
| **22:30** | **Modelling retrain** | ~2 menit | scikit-learn |
| 23:00 | Serving update | auto | FastAPI |

---

## Tim Kelompok 11

| Nama | Bagian |
|------|--------|
| Rambat | Orkestrasi, Ingestion, Preprocessing, Modelling Sentimen |
| Jojo | Kurs EUR/USD, Streaming, Modelling XGBoost, Analisis Korelasi |
| Rafah | Komoditas, Streaming, Modelling XGBoost per Symbol |
