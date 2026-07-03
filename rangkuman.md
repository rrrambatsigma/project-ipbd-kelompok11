# 📰 Rangkuman Pipeline Analisis Sentimen Berita Nilai Tukar Euro
### Kelompok 11 — Infrastruktur dan Platform Big Data

---

## 📋 Daftar Isi

1. [Ringkasan Eksekutif](#ringkasan-eksekutif)
2. [Arsitektur End-to-End](#arsitektur-end-to-end)
3. [Komponen 1: Ingestion](#1-ingestion)
4. [Komponen 2: Preprocessing](#2-preprocessing)
5. [Komponen 3: Modelling](#3-modelling)
6. [Komponen 4: Serving](#4-serving)
7. [Komponen 5: Scheduler & Orkestrasi](#5-scheduler--orkestrasi)
8. [Komponen 6: Notification](#6-notification)
9. [Komponen 7: Monitoring](#7-monitoring)
10. [Komponen 8: Storage](#8-storage)
11. [Komponen 9: Logging](#9-logging)
12. [Infrastruktur Docker](#infrastruktur-docker)
13. [Jadwal Eksekusi Harian](#jadwal-eksekusi-harian)
14. [Cara Menjalankan](#cara-menjalankan)
15. [Daftar Teknologi](#daftar-teknologi)
16. [Struktur Proyek](#struktur-proyek)

---

## Ringkasan Eksekutif

Pipeline **end-to-end** ini dirancang untuk mengumpulkan, memproses, menganalisis, dan menyajikan sentimen berita-berita yang berkaitan dengan **nilai tukar Euro (EUR/USD)**. Data dikumpulkan dari 4 sumber berita (ECB, GDELT, The Guardian, NewsAPI) sebanyak **5 kali sehari** mengikuti sesi pasar Eropa, kemudian diproses menggunakan **Apache Spark** untuk filtering dan enrichment sentimen (VADER), dilatih model **LDA Topic Modeling** dan **TF-IDF + Logistic Regression** untuk klasifikasi sentimen, dan disajikan melalui **FastAPI REST API**.

Seluruh pipeline diorkestrasi oleh **Prefect**, disimpan di **MinIO** (S3-compatible object storage), dimonitor via **Prometheus + Grafana**, dan mengirim notifikasi melalui **Telegram Bot**.

---

## Arsitektur End-to-End

```
                         BATCH INGESTION (5x/hari — Prefect)
┌──────────────────┐     06:00  09:00  13:00  17:00  21:00 WIB
│    Ingestion     │ ────────────────────────────────────────────→  MinIO news-raw/
│  ECB · Guardian  │                                                    (JSON)
│  GDELT · NewsAPI │
└──────────────────┘
       │
       │ TRIGGER (22:00 WIB — 1x/hari)
       ▼
┌──────────────────┐
│   Preprocessing  │ ──→  MinIO news-processed/sentiment/
│    (PySpark)     │       ├── articles/ (Parquet, partition year/month)
│                   │       │   ~3000 artikel | VADER scored
│  7-Layer Pipeline │       └── aggregated/sentiment_by_session/
│  → save ke MinIO  │           ~2121 baris | 5 sesi/hari
└──────────────────┘
       │
       │ TRIGGER (22:30 WIB — 1x/hari)
       ▼
┌──────────────────┐
│    Modelling     │ ──→  MinIO news-processed/models/
│  (scikit-learn)  │       ├── lda/ (vectorizer.pkl, lda.pkl, topics...)
│                   │       ├── sentiment/ (vectorizer.pkl, classifier.pkl, report...)
│  LDA Topic Model  │       └── latest/predictions_daily.csv
│  TF-IDF + LR      │
└──────────────────┘
       │
       ▼
┌──────────────────┐
│     Serving      │  FastAPI REST API — port 8000
│  (Dashboard API) │  Baca hasil dari MinIO
└──────────────────┘
```

---

## 1. Ingestion

### 1.1 Tujuan
Mengumpulkan berita-berita terkait nilai tukar Euro dari **4 sumber berita** secara terjadwal, melakukan deduplikasi, dan menyimpannya ke MinIO sebagai **raw JSON**.

### 1.2 Sumber Data (3 Tier)

| Tier | Sumber | Metode | Deskripsi |
|------|--------|--------|-----------|
| **Tier 1** | **ECB** (European Central Bank) | RSS feed via `feedparser` | Economic bulletin, press releases, speeches, working papers. Tidak ada rate limit. |
| **Tier 2** | **GDELT Project** | RSS query API | Global news database. Query: "euro exchange rate", "ECB", "eurozone inflation". Tidak ada rate limit khusus. |
| **Tier 2** | **The Guardian** | REST API (`requests`) | Open API gratis, 500 request/hari. Query: "euro exchange rate", "ECB European Central Bank". |
| **Tier 3** | **NewsAPI.org** | REST API (`requests`) | Free tier: 100 request/hari. Query: "euro exchange rate EUR USD", "ECB monetary policy". |

### 1.3 Jadwal Batch (5x/hari — Senin-Jumat)

Mengikuti **sesi pasar valuta asing Eropa**. Waktu dalam UTC+7 (WIB):

| Waktu WIB | UTC | Sesi | Lookback | Deskripsi |
|-----------|-----|------|----------|-----------|
| 06:00 | 23:00 (H-1) | **Pre-market** | 7 jam | Sebelum pasar Eropa buka |
| 09:00 | 02:00 | **Open** | 3 jam | Pasar Eropa baru buka |
| 13:00 | 06:00 | **Mid** | 4 jam | Tengah sesi Eropa |
| 17:00 | 10:00 | **Pre-close** | 4 jam | London masih aktif |
| 21:00 | 14:00 | **Overlap** | 4 jam | London + New York overlap |

### 1.4 Alur Ingestion per Batch

```
START
  │
  ├── [1] Health Check MinIO ──→ Pastikan storage siap
  │
  ├── [2] Scraping Paralel ─────→ Submit 3 task scraping via PrefectFuture
  │     ├── task_scrape_ecb()       (RSS feedparser)
  │     ├── task_scrape_gdelt()     (GDELT RSS + Guardian API)
  │     └── task_scrape_newsapi()   (NewsAPI REST)
  │
  ├── [3] Deduplikasi Internal ──→ Fingerprint SHA256(url|title|published_at)
  │
  ├── [4] Upload ke MinIO ───────→ news-raw/{source}/{YYYY}/{YYYY-MM-DD}/{file}.json
  │     └── Cek stat_object dulu → skip jika sudah ada (dedup lintas batch)
  │
  ├── [5] Generate Summary ──────→ total input, uploaded, skipped, failed
  │
  ├── [6] Telegram Notification ──→ alert sukses/gagal batch
  │
  └── [7] Push Metrics ──────────→ Prometheus Pushgateway
```

### 1.5 Fitur Deduplikasi

Dua lapis deduplikasi:

1. **Dalam batch (internal):** Fingerprint SHA256 dari `url|title|published_at`, dihapus duplikat dalam satu batch scraping
2. **Lintas batch (MinIO level):** Sebelum upload, cek `stat_object` di MinIO — jika file dengan path yang sama sudah ada, skip (tidak overwrite). Path deterministik: `{source}_{date}_{id_sha256[:12]}.json`

### 1.6 Backfill Historis (2021–2025)

File terpisah (`backfill_flow.py`) untuk mengambil data historis:
- **Sumber:** The Guardian API (500 request/hari gratis)
- **Strategi:** Per bulan dari Januari 2021 sampai Desember 2025
- **Total:** ~60 bulan × 4 query = ~240 request → selesai dalam 1 hari
- **Progress:** Notifikasi Telegram setiap 3 bulan

### 1.7 Komponen Utama Ingestion

| File | Lokasi | Fungsi |
|------|--------|--------|
| `news_ingestion_flow.py` | `ingestion/collector/flows/` | Prefect flow utama — orchestrator 5 batch |
| `backfill_flow.py` | `ingestion/collector/flows/` | Backfill historis Guardian 2021–2025 |
| `ecb_scraper.py` | `ingestion/collector/flows/scrapers/` | Scraper RSS ECB (4 feeds) |
| `gdelt_scraper.py` | `ingestion/collector/flows/scrapers/` | Scraper GDELT RSS + Guardian fallback |
| `newsapi_scraper.py` | `ingestion/collector/flows/scrapers/` | Scraper NewsAPI /v2/everything |
| `minio_client.py` | `ingestion/collector/flows/storage/` | Abstraksi MinIO: upload, dedup, listing, health check |
| `telegram_alert.py` | `ingestion/collector/flows/utils/` | Notifikasi Telegram (artikel, batch, backfill) |
| `metrics.py` | `ingestion/collector/flows/utils/` | Prometheus metrics push |
| `config.py` | `ingestion/collector/flows/utils/` | Konfigurasi terpusat dari environment |
| `prefect.yaml` | `ingestion/collector/flows/` | Definisi 5 deployment + cron schedule |

---

## 2. Preprocessing

### 2.1 Tujuan
Membersihkan, memfilter, dan memperkaya berita mentah (raw JSON dari MinIO) menjadi data terstruktur yang siap untuk pemodelan. Menggunakan **Apache Spark** untuk pemrosesan skala besar.

### 2.2 Teknologi
- **Apache Spark 3.5+** (PySpark) — distributed data processing
- **S3A connector** — Hadoop-AWS untuk baca/tulis MinIO
- **fastText lid.176** — language detection (model 917KB)
- **NLTK VADER** — sentiment scoring
- **Pandas UDF** — fungsi UDF efisien di Spark

### 2.3 7-Layer Pipeline

```
INPUT: ~10.126 artikel
  │
  ├── LAYER 0: LOAD ─────────────→ Baca dari raw JSON atau Parquet sebelumnya
  │     Output: 10.126
  │
  ├── LAYER 1A: Language Filter ──→ Deteksi bahasa (fastText lid.176)
  │     Output: 9.600          (🗑️ ~526 non-English)
  │
  ├── LAYER 1B: Topic Relevance ──→ Skor keyword finansial (threshold ≥3)
  │     Output: 3.394          (🗑️ ~6.206 tidak relevan Euro)
  │
  ├── LAYER 1C: Quality Filter ──→ Panjang teks, boilerplate, tahun
  │     Output: 3.090          (🗑️ ~304 kualitas rendah)
  │
  ├── LAYER 2: Deduplication ────→ Exact dedup + title-key window function
  │     Output: 3.000          (🗑️ ~90 duplikat)
  │
  ├── LAYER 3A: Sentiment Score ──→ VADER compound/pos/neg/neu (tidak ada removal)
  │
  ├── LAYER 3B: NLP Clean ──────→ Tokenize → stopword → lemmatize
  │
  ├── LAYER 3C: Keyword Flags ───→ Boolean flags per keyword finansial
  │
  ├── LAYER 3D: Session Tag ────→ Map UTC hour ke 5 sesi pasar Eropa
  │
  └── AGGREGATE ─────────────────→ Group by (date, session_tag)
        Output: 2.121 baris agregasi
```

### 2.4 Detail Setiap Layer

#### Layer 0: LOAD
- **Sumber:** MinIO `news-raw/` (raw JSON dengan `RAW_SCHEMA`) ATAU `news-processed/articles/` (Parquet)
- **Fallback:** Jika Parquet kosong/gagal, fallback ke raw JSON
- **Schema:** Semua field StringType untuk fleksibilitas parsing

#### Layer 1A: Language Filter
- **Tool:** fastText `lid.176.ftz` model
- **Method:** `pandas_udf` untuk batch prediction per partisi
- **Logika:** Label `__label__en` → keep, lainnya → buang
- **Graceful degradation:** Jika model file tidak ditemukan, skip filtering dengan warning

#### Layer 1B: Topic Relevance Filter
- **Skor berbasis regex:**
  - Core patterns (euro, eur, ecb, eurozone, euro area): **+3 poin**
  - Monetary patterns (interest rate, inflation, cpi, rate decision): **+2 poin**
  - Forex patterns (exchange rate, forex, eur/usd): **+2 poin**
  - Economy patterns (gdp, recession, growth, unemployment): **+1 poin**
- **Threshold:** ≥3 poin di body ATAU title → keep
- **Keyword weights:** ecb=3, inflation=2, interest_rate=2, forex=2, currency=2, dll

#### Layer 1C: Quality Filter
- Text length: 100–15.000 karakter
- Title length: ≥10 karakter
- Boilerplate phrases: "read more", "click here", "subscribe", "sign up", "advertisement"
- Bukan URL-only content
- Year range: 2021–2026

#### Layer 2: Deduplication
1. **Exact dedup:** `dropDuplicates` pada `article_id` dan `url`
2. **Title-key dedup:** Lowercase + regex clean title → window partition → keep first row
3. **Fuzzy dedup (optional):** Jaccard similarity pada title (O(n²), di-skip default)

#### Layer 3A: VADER Sentiment Scoring
- **Tool:** NLTK `SentimentIntensityAnalyzer`
- **Output:** 4 kolom — `vader_compound` (-1 s/d 1), `vader_pos`, `vader_neg`, `vader_neu`
- **Method:** `pandas_udf` dengan schema StructType

#### Layer 3B: NLP Clean
- **Proses:** Lowercase → hapus URL → hapus angka → hapus punctuation → tokenize → hapus stopwords → lemmatize (WordNet)
- **Stopword tambahan:** "said", "also", "would", "could", "report", "according", dll
- **Output:** `tokens_lemma` column — teks bersih siap untuk modeling

#### Layer 3C: Keyword Flags
- **11 keyword finansial:** inflation, interest_rate, ecb, monetary_policy, gdp, recession, unemployment, growth, trade, forex, currency
- **Output:** Boolean columns (`has_inflation`, `has_ecb`, dll)

#### Layer 3D: Session Tagging
- Mapping UTC hour ke 5 sesi pasar EUR/USD:

| Session | Rentang UTC | Deskripsi |
|---------|------------|-----------|
| `pre_market` | 23:00 – 01:59 | Sebelum pasar London buka |
| `open` | 02:00 – 05:59 | Pembukaan pasar London |
| `mid` | 06:00 – 09:59 | Tengah sesi Eropa |
| `pre_close` | 10:00 – 13:59 | Menjelang tutup London |
| `overlap` | 14:00 – 22:59 | London + New York overlap |

#### Aggregate
- **Group by:** `date` + `session_tag`
- **Metrics:** avg compound, avg positive/negative/neutral, stddev compound, article count, positive/negative/neutral counts, keyword mentions per keyword
- **Output:** ~2.121 baris agregasi

### 2.5 Output ke MinIO

```
news-processed/sentiment/
├── articles/                         ← Parquet, partition by year/month
│   └── year=2026/month=6/part-*.snappy.parquet
└── aggregated/
    ├── sentiment_by_session/         ← Parquet
    └── sentiment_by_session_csv/     ← CSV
```

### 2.6 Monitoring Preprocessing

Setiap layer mencatat **filter statistik** (before → after → removed → percentage):
- Visualisasi ASCII bar chart di log
- Prometheus metrics per stage (`preprocessing_filter_before/after/removed`)
- Telegram notification start/end dengan ringkasan filter

### 2.7 Komponen Utama Preprocessing

| File | Lokasi | Fungsi |
|------|--------|--------|
| `news_sentiment_job.py` | `preprocessing/spark/jobs/` | Main pipeline 640 baris — 7 layer + aggr + save |
| `news_preprocessing_job.py` | `preprocessing/spark/jobs/` | Pipeline awal 6 step (legacy) |
| `sentiment_udfs.py` | `preprocessing/spark/jobs/` | Semua pandas_udf (VADER, NLP, keyword, session, dll) |
| `lang_filter.py` | `preprocessing/spark/jobs/` | fastText English detection dengan graceful fallback |
| `minio_utils.py` | `preprocessing/spark/jobs/` | SparkSession builder + S3A + boto3 client |
| `schema.py` | `preprocessing/spark/jobs/` | RAW_SCHEMA + PROCESSED_SCHEMA |
| `preprocessing_flow.py` | `preprocessing/spark/jobs/` | Prefect flow → spark-submit |
| `prefect_bootstrap.py` | `preprocessing/spark/jobs/` | Alternative Python bootstrap |
| `prefect.yaml` | `preprocessing/spark/jobs/` | Deployment manifest (cron 16:00 UTC) |

---

## 3. Modelling

### 3.1 Tujuan
Melatih dua model **text mining** secara offline menggunakan data yang sudah dipreprocessing:
1. **LDA Topic Modeling** — menemukan topik-topik laten dalam berita Euro
2. **Sentiment Classifier** — mengklasifikasikan sentimen berita (positif/negatif)

### 3.2 Teknologi
- **scikit-learn 1.4.2** — LDA, TF-IDF, LogisticRegression
- **pyarrow** — baca Parquet dari MinIO via S3FileSystem
- **boto3** — upload model .pkl ke MinIO
- **joblib** — serialisasi model

### 3.3 Alur Modelling

```
[1/5] LOAD DATA
  │  data_loader.load_articles()
  │  → Baca Parquet dari MinIO news-processed/sentiment/articles/
  │  → Filter: clean_text tidak null, ≥10 kata
  │  → Output: ~3.000 artikel (pandas DataFrame)
  │
[2/5] LDA TOPIC MODELING
  │  lda_pipeline.run_lda(df)
  │  → CountVectorizer (max_df=0.95, min_df=2, stop_words English)
  │  → LatentDirichletAllocation (8 topics, random_state=42, n_jobs=-1)
  │  → Output: topic distribution per artikel + top 10 words per topic
  │  → Coherence score (pairwise Jaccard-based)
  │
[3/5] SENTIMENT CLASSIFIER
  │  sentiment_trainer.run_classifier(df)
  │  → Label dari VADER compound:
  │       compound ≥ 0.30 → positive (label 2)
  │       compound ≤ -0.30 → negative (label 0)
  │       Di antara → neutral (DIBUANG — binary classification)
  │  → TF-IDF Vectorizer (5000 features, unigram+bigram, min_df=2)
  │  → LogisticRegression (multinomial, class_weight=balanced, max_iter=1000)
  │  → Train/Test split: 80/20 stratified
  │  → Output: accuracy, precision, recall, F1, confusion matrix
  │
[4/5] EVALUATION & SUMMARY
  │  evaluator.print_summary()
  │  → Classification report per class
  │  → Confusion matrix
  │  → LDA coherence score
  │
[5/5] SAVE MODELS
  │  model_store.save_all(run_id, lda_result, sentiment_result)
  │  → Upload .pkl, .parquet, .csv ke MinIO
  │  → Update latest.txt symlink
  │
[6/6] PREDICT ALL + DAILY AGGR
  │  → Predict sentiment untuk SEMUA artikel
  │  → Group by date → hitung positif/negatif per hari
  │  → Save predictions_daily.csv
  │
  └── MONITOR: track_run() → metrics history + Pushgateway + Telegram
```

### 3.4 LDA Topic Modeling Detail

| Parameter | Nilai |
|-----------|-------|
| Jumlah topik | 8 |
| Top words per topik | 10 |
| max_df | 0.95 (hapus kata di >95% dokumen) |
| min_df | 2 (hapus kata di <2 dokumen) |
| random_state | 42 |

**Contoh topik yang dihasilkan:**
- Topic 0: rate, ecb, interest, inflation, central, bank, policy, price, monetary, eurozone
- Topic 1: euro, currency, dollar, exchange, market, trade, bank, foreign, reserve, global
- Topic 2: growth, economic, gdp, economy, german, forecast, data, outlook, recovery, expansion
- *(dan seterusnya — 8 topik total)*

### 3.5 Sentiment Classifier Detail

| Parameter | Nilai |
|-----------|-------|
| Label positive | VADER compound ≥ 0.30 |
| Label negative | VADER compound ≤ -0.30 |
| Neutral | Dibuang (binary classification) |
| TF-IDF max_features | 5000 |
| N-gram range | (1, 2) — unigram + bigram |
| Classifier | LogisticRegression (multinomial) |
| Class weight | balanced |
| Train/Test split | 80/20 stratified |
| random_state | 42 |

### 3.6 Output ke MinIO

```
news-processed/models/
├── {run_id}/                          ← run_id = YYYYMMDD_HHMMSS
│   ├── lda/
│   │   ├── vectorizer.pkl             ← CountVectorizer
│   │   ├── lda.pkl                    ← LDA model
│   │   ├── topics.parquet             ← Topic assignment per artikel
│   │   └── top_words.csv              ← Top 10 words per topic
│   └── sentiment/
│       ├── vectorizer.pkl             ← TfidfVectorizer
│       ├── classifier.pkl             ← LogisticRegression
│       ├── report.csv                 ← Classification report
│       ├── confusion_matrix.csv       ← Confusion matrix
│       └── test_predictions.csv       ← Predictions on test set
├── lda/latest.txt                     ← Pointer ke run_id terbaru (LDA)
├── sentiment/latest.txt               ← Pointer ke run_id terbaru (sentiment)
├── latest/predictions_daily.csv       ← Agregasi sentimen harian
└── metrics_history.csv                ← Riwayat metrik antar run
```

### 3.7 Monitoring Modelling

**Metrics history (`metrics_history.csv`):**
- Setiap run mencatat: accuracy, precision_macro, recall_macro, f1_macro, lda_coherence, n_articles, n_train, n_test, timestamp
- **Trend analysis:** Bandingkan dengan run sebelumnya → arrow indicator (↑/↓)

**Prometheus Pushgateway (9 gauges):**
- `modelling_articles_total`
- `modelling_accuracy`
- `modelling_f1_macro`
- `modelling_precision_macro`
- `modelling_recall_macro`
- `modelling_lda_coherence`
- `modelling_duration_seconds`
- `modelling_n_train`
- `modelling_n_test`

**Telegram Notification:**
- Run ID dan durasi
- Accuracy, F1, precision, recall
- Per-class breakdown
- Confusion matrix (formatted)
- LDA coherence score

### 3.8 Komponen Utama Modelling

| File | Lokasi | Fungsi |
|------|--------|--------|
| `run_pipeline.py` | `modelling/` | Orchestrator utama 5+1 langkah |
| `data_loader.py` | `modelling/` | Baca Parquet dari MinIO via pyarrow S3FileSystem |
| `lda_pipeline.py` | `modelling/` | LDA topic modeling: CountVectorizer → LDA → topik |
| `sentiment_trainer.py` | `modelling/` | TF-IDF + LogisticRegression, label dari VADER |
| `evaluator.py` | `modelling/` | Classification report, confusion matrix, print summary |
| `model_store.py` | `modelling/` | Save/load .pkl ke MinIO via boto3 |
| `monitor.py` | `modelling/` | Metrics history, Pushgateway, Telegram notification |
| `config.py` | `modelling/` | Hyperparameter terpusat |
| `modelling_flow.py` | `modelling/` | Prefect flow wrapper |
| `prefect.yaml` | `modelling/` | Deployment config (cron 30 16 * * 1-5 UTC) |

---

## 4. Serving

### 4.1 Tujuan
Menyajikan hasil pipeline (sentimen, model, agregasi) melalui **REST API** yang bisa dikonsumsi oleh dashboard atau aplikasi lain.

### 4.2 Teknologi
- **FastAPI** — framework web Python modern
- **Uvicorn** — ASGI server
- **boto3** — baca CSV dari MinIO
- **pyarrow** — baca Parquet dari MinIO via S3FileSystem

### 4.3 Cara Kerja

```
[Client] ──GET /api/sentiment/daily──→ [FastAPI :8000]
                                            │
                                            ├── models/latest/predictions_daily.csv
                                            │
[Client] ──GET /api/sentiment/report───→ [FastAPI :8000]
                                            │
                                            ├── models/sentiment/latest.txt → run_id
                                            └── models/{run_id}/sentiment/report.csv
```

1. Baca `latest.txt` untuk mendapatkan `run_id` terbaru
2. Baca file hasil dari folder `{run_id}` di MinIO
3. Return sebagai JSON atau CSV download

### 4.4 Endpoints

| Endpoint | Method | Deskripsi | Format |
|----------|--------|-----------|--------|
| `/` | GET | Daftar semua endpoint | JSON |
| `/health` | GET | Health check + status MinIO | JSON |
| `/api/sentiment/daily` | GET | Prediksi sentimen harian | JSON |
| `/api/sentiment/daily.csv` | GET | Prediksi sentimen harian | CSV download |
| `/api/sentiment/predictions` | GET | Test predictions model terbaru | JSON |
| `/api/sentiment/report` | GET | Classification report model terbaru | JSON |
| `/api/sentiment/confusion-matrix` | GET | Confusion matrix model terbaru | JSON |
| `/api/lda/top-words` | GET | Top words per topic | JSON |
| `/api/lda/topics` | GET | Topic assignments (Parquet) | JSON |
| `/api/aggregated/sessions` | GET | Agregasi sentimen per sesi | JSON |
| `/api/aggregated/sessions.csv` | GET | Agregasi sentimen per sesi | CSV download |

### 4.5 Error Handling
- Jika file tidak ditemukan → return empty array `[]` atau empty CSV (graceful degradation)
- CORS middleware: allow all origins (development mode)

### 4.6 Komponen Serving

| File | Lokasi | Fungsi |
|------|--------|--------|
| `app.py` | `serving/` | FastAPI app — 10 endpoints + helpers |
| `Dockerfile` | `serving/` | Build image python:3.11 + FastAPI |
| `requirements.txt` | `serving/` | fastapi, uvicorn, pandas, boto3, pyarrow |

---

## 5. Scheduler & Orkestrasi

### 5.1 Tujuan
Mengorkestrasi dan menjadwalkan seluruh pipeline secara otomatis dan reliabel.

### 5.2 Teknologi
- **Prefect 2.x** — workflow orchestration platform
- **PostgreSQL 15** — backend database (mengganti SQLite default untuk production)
- **Prefect Server** — UI + API (port 4200)
- **Prefect Worker** — process-type worker (3 work pools)

### 5.3 Infrastruktur Prefect

```
┌──────────────────────────────────────────────────────────┐
│                    prefect-server                         │
│  Gambar: prefecthq/prefect:2-latest                       │
│  Port: 4200 (UI + API)                                    │
│  Database: PostgreSQL 15 (postgresql+asyncpg://)           │
│  Health check: /api/health                                │
└────────────────────┬─────────────────────────────────────┘
                     │
      ┌──────────────┼──────────────┐
      │              │              │
      ▼              ▼              ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│ingestion │  │preprocess│  │modelling │
│ worker   │  │ worker   │  │ worker   │
│pool      │  │pool      │  │pool      │
└──────────┘  └──────────┘  └──────────┘
```

### 5.4 Workflow Harian

| Waktu (WIB) | Flow | Deployment | Worker Pool | Durasi |
|-------------|------|------------|-------------|--------|
| 06:00 | Ingestion Pre-market | `euro-news-pre-market` | ingestion-pool | ~5 menit |
| 09:00 | Ingestion Open | `euro-news-open` | ingestion-pool | ~5 menit |
| 13:00 | Ingestion Mid | `euro-news-mid` | ingestion-pool | ~5 menit |
| 17:00 | Ingestion Pre-close | `euro-news-pre-close` | ingestion-pool | ~5 menit |
| 21:00 | Ingestion Overlap | `euro-news-overlap` | ingestion-pool | ~5 menit |
| **22:00** | **Preprocessing** | `euro-news-preprocessing` | preprocessing-pool | ~15 menit |
| **22:30** | **Modelling** | `euro-news-modelling` | modelling-pool | ~2 menit |

### 5.5 Fitur Orkestrasi

1. **Idempotent deployment:** Setiap `entrypoint.sh` cek apakah work pool / deployment sudah ada — jika ya, skip
2. **Graceful shutdown:** Trap SIGTERM → stop worker → exit cleanly
3. **Health check loop:** Monitor worker process tiap 30 detik — restart jika mati
4. **Polling server ready:** Tunggu Prefect Server sampai benar-benar siap (max 5 menit)
5. **PostgreSQL backend:** Mengganti SQLite default yang rawan "database is locked" pada concurrent writes

### 5.6 Konfigurasi Deployment

**Cron schedule di Prefect YAML:**

| File | Schedule (UTC) | Keterangan |
|------|----------------|------------|
| `ingestion/collector/flows/prefect.yaml` | `0 23 * * 0-4` | Pre-market (Minggu–Kamis) |
| | `0 2 * * 1-5` | Open (Senin–Jumat) |
| | `0 6 * * 1-5` | Mid |
| | `0 10 * * 1-5` | Pre-close |
| | `0 14 * * 1-5` | Overlap |
| `preprocessing/spark/jobs/prefect.yaml` | `0 16 * * 1-5` | Preprocessing |
| `modelling/prefect.yaml` | `30 16 * * 1-5` | Modelling |

---

## 6. Notification

### 6.1 Tujuan
Memberikan notifikasi real-time ke Telegram untuk setiap event penting di pipeline.

### 6.2 3 Bot Telegram Terpisah

| Layer | Bot Token | Chat ID | Fungsi |
|-------|-----------|---------|--------|
| **Ingestion** | `8963908474:AAF9...` | `5974165452` | Alert artikel, batch summary, backfill |
| **Preprocessing** | `8218332717:AAHS...` | `5974165452` | Start pipeline, end summary, error |
| **Modelling** | `8883638359:AAF2...` | `5974165452` | Run summary, metrics, confusion matrix |

### 6.3 Jenis Notifikasi

#### Ingestion
- **Per artikel (optional):** Setiap artikel yang diupload ke MinIO — source, title, url, category, raw_text
- **Batch summary:** Total input, uploaded, skipped, failed per source, status SUCCESS/PARTIAL
- **Failed alert:** Error message, session, timestamp
- **Backfill progress:** Setiap 3 bulan — persentase progress
- **Backfill done:** Total artikel, periode

#### Preprocessing
- **Start:** Mode (RAW/PROCESSED), timestamp
- **End:** Duration, input count, passed count, pass rate, filter summary per stage
- **Error:** Error message

#### Modelling
- **Run summary:** Run ID, articles count, train/test split, duration
- **Metrics:** Accuracy, F1 macro, precision, recall
- **Per-class breakdown:** Precision, recall, F1 per class
- **Confusion matrix:** Formatted table
- **LDA coherence score**
- **Trend indicators:** ↑/↓ arrow vs previous run

### 6.4 Format Pesan (HTML)

```html
🤖 <b>Modelling Pipeline Selesai!</b>
────────────────────────────
🆔 Run ID       : 20260608_223000
📥 Input        : 3,004 artikel
📊 Train/Test   : 2,403 / 601
⏱️ Durasi       : 47 detik

📈 <b>Kinerja Model</b>
  Accuracy      : 78.54%
  F1 Macro      : 78.50%
  Precision     : 78.50%
  Recall        : 78.50%

📋 <b>Per Class</b>
  negative → P:76.2% R:81.7% F1:78.9% (n=304)
  positive → P:80.9% R:75.1% F1:77.9% (n=297)

🔄 <b>Confusion Matrix</b>
          negative  positive
negative      248        56
positive       74       223

📚 <b>LDA Topic Coherence</b>
  ✦ Score: 0.7842
```

---

## 7. Monitoring

### 7.1 Tujuan
Memantau kesehatan pipeline, performa model, dan metrik bisnis secara real-time.

### 7.2 Arsitektur Monitoring

```
[Batch Jobs] ──push metrics──→ [Pushgateway :9091]
                                    │
                              [scrape tiap 15s]
                                    │
                                    ▼
                              [Prometheus :9090]
                                    │
                              [query data source]
                                    │
                                    ▼
                              [Grafana :3000]
                          (Dashboard visualisasi)
```

### 7.3 Pushgateway
- **Fungsi:** Menerima metrics dari batch job (yang tidak bisa di-scrape langsung oleh Prometheus)
- **URL:** `http://pushgateway:9091`
- **Job names:** `ingestion`, `preprocessing`, `modelling`

### 7.4 Prometheus
- **Scrape interval:** 15 detik
- **Config:** `ingestion/prometheus.yml`
- **Target:** `pushgateway:9091`

### 7.5 Grafana
- **URL:** `http://localhost:3000`
- **Login:** admin / admin
- **Data source:** Prometheus (http://prometheus:9090)

### 7.6 Metrics Lengkap

#### Ingestion Metrics
| Metric | Type | Labels | Deskripsi |
|--------|------|--------|-----------|
| `ingestion_articles_total` | Counter | source, status | Total artikel per source & status |
| `ingestion_errors_total` | Counter | source | Total error per source |
| `ingestion_run_duration_seconds` | Gauge | session | Durasi batch ingestion |
| `ingestion_articles_unique` | Gauge | session | Artikel unik per sesi |

#### Preprocessing Metrics
| Metric | Type | Labels | Deskripsi |
|--------|------|--------|-----------|
| `preprocessing_articles_total` | Gauge | — | Total artikel input |
| `preprocessing_articles_passed` | Gauge | — | Artikel lolos filter |
| `preprocessing_duration_seconds` | Gauge | — | Durasi pipeline |
| `preprocessing_pass_rate` | Gauge | — | Persentase lolos |
| `preprocessing_filter_before` | Gauge | stage | Sebelum filter |
| `preprocessing_filter_after` | Gauge | stage | Setelah filter |
| `preprocessing_filter_removed` | Gauge | stage | Jumlah dibuang |

#### Modelling Metrics
| Metric | Type | Labels | Deskripsi |
|--------|------|--------|-----------|
| `modelling_articles_total` | Gauge | — | Total artikel input |
| `modelling_accuracy` | Gauge | — | Accuracy classifier |
| `modelling_f1_macro` | Gauge | — | F1 macro average |
| `modelling_precision_macro` | Gauge | — | Precision macro average |
| `modelling_recall_macro` | Gauge | — | Recall macro average |
| `modelling_lda_coherence` | Gauge | — | LDA coherence score |
| `modelling_duration_seconds` | Gauge | — | Durasi training |
| `modelling_n_train` | Gauge | — | Jumlah train set |
| `modelling_n_test` | Gauge | — | Jumlah test set |

---

## 8. Storage

### 8.1 Tujuan
Menyimpan semua data pipeline — dari JSON mentah hingga model terlatih — secara terpusat, scalable, dan S3-compatible.

### 8.2 Teknologi: MinIO
- **Image:** `minio/minio:latest`
- **API:** S3-compatible (boto3, S3A, pyarrow)
- **Port:** 9000 (API), 9001 (Console UI)
- **Login:** minioadmin / minioadmin123

### 8.3 Struktur Bucket

#### Bucket: `news-raw`
Data mentah dari ingestion, disimpan sebagai JSON.

```
news-raw/
├── ecb/
│   ├── 2026/
│   │   ├── 2026-06-08/
│   │   │   ├── ecb_20260608_a1b2c3d4e5f6.json
│   │   │   └── ecb_20260608_6f5e4d3c2b1a.json
│   │   └── ...
│   └── ...
├── guardian/
│   ├── 2021/
│   │   ├── 2021-01-01/
│   │   │   └── guardian_20210101_*.json
│   │   └── ...
│   └── ...
├── gdelt/
│   └── ...
└── newsapi/
    └── ...
```

#### Bucket: `news-processed`
Data olahan, hasil preprocessing, model, dan metrik.

```
news-processed/
├── sentiment/
│   ├── articles/                          ← Parquet partitioned
│   │   ├── year=2026/
│   │   │   └── month=6/
│   │   │       └── part-00001.snappy.parquet
│   │   └── ...
│   └── aggregated/
│       ├── sentiment_by_session/          ← Parquet
│       │   └── part-00001.snappy.parquet
│       └── sentiment_by_session_csv/      ← CSV
│           └── part-00001.csv
└── models/
    ├── 20260608_223000/                   ← run_id
    │   ├── lda/
    │   │   ├── vectorizer.pkl
    │   │   ├── lda.pkl
    │   │   ├── topics.parquet
    │   │   └── top_words.csv
    │   └── sentiment/
    │       ├── vectorizer.pkl
    │       ├── classifier.pkl
    │       ├── report.csv
    │       ├── confusion_matrix.csv
    │       └── test_predictions.csv
    ├── lda/latest.txt                     ← "20260608_223000"
    ├── sentiment/latest.txt               ← "20260608_223000"
    ├── latest/predictions_daily.csv
    └── metrics_history.csv
```

### 8.4 Metode Akses

| Layer | Library | Method | Fungsi |
|-------|---------|--------|--------|
| **Ingestion** | `minio` (Python SDK) | `put_object`, `stat_object` | Upload JSON + cek duplikat |
| **Preprocessing** | `boto3` + S3A (Hadoop) | `list_objects`, `spark.read.json` | Baca raw JSON via Spark |
| **Modelling** | `pyarrow.fs.S3FileSystem` | `pq.read_table` | Baca Parquet dari MinIO |
| **Modelling save** | `boto3` | `upload_fileobj`, `put_object` | Upload .pkl, .csv |
| **Serving** | `boto3` + `pyarrow` | `get_object`, `pq.read_table` | Baca hasil untuk API |

### 8.5 Fitur Storage

1. **Path deterministik:** Nama file berdasarkan hash SHA256 → objek yang sama selalu di path yang sama → dedup otomatis
2. **Partitioning:** Data preprocessing di-partition by `year/month` untuk query efisien
3. **Latest symlink:** File `latest.txt` berisi `run_id` terbaru untuk akses mudah
4. **Auto bucket creation:** `minio-init` service membuat bucket saat startup

---

## 9. Logging

### 9.1 Tujuan
Mencatat seluruh aktivitas pipeline dengan format yang terstruktur, informatif, dan mudah dibaca.

### 9.2 Teknologi: Loguru
- Library logging Python dengan format kaya dan warna otomatis
- Digunakan di semua layer (ingestion, preprocessing, modelling)

### 9.3 Level Logging
| Level | Warna | Penggunaan |
|-------|-------|------------|
| `INFO` | Biru | Informasi umum (mulai/selesai task) |
| `SUCCESS` | Hijau | Keberhasilan (upload selesai, filter OK) |
| `WARNING` | Kuning | Peringatan (fallback ke mode lain) |
| `ERROR` | Merah | Error (task gagal, koneksi putus) |

### 9.4 Contoh Output

```
2026-06-08 22:00:15 | INFO     | [LOAD] Membaca dari raw JSON di news-raw...
2026-06-08 22:00:45 | SUCCESS  | [LOAD] Total dari raw: 10126 artikel
2026-06-08 22:01:20 | INFO     | [1A_LANGUAGE] 10126 → 9600 | dibuang: 526 (5.2%)
2026-06-08 22:02:10 | INFO     | [1B_TOPIC] 9600 → 3394 | dibuang: 6206 (64.6%)
2026-06-08 22:02:30 | INFO     | [1C_QUALITY] 3394 → 3090 | dibuang: 304 (9.0%)
2026-06-08 22:02:45 | INFO     | [2_DEDUP] 3090 → 3000 | dibuang: 90 (2.9%)
2026-06-08 22:03:00 | SUCCESS  | ✅ Total setelah semua layer: 3000 artikel
```

### 9.5 Filtering Report (Visual ASCII)

```
  ═══════════════════════════════════════════
    FILTERING REPORT
  ═══════════════════════════════════════════
    1A_LANGUAGE          │  10126 →   9600 │   526 removed │ ████████████████████ 95%
    1B_TOPIC             │   9600 →   3394 │  6206 removed │ ██████░░░░░░░░░░░░░░ 35%
    1C_QUALITY           │   3394 →   3090 │   304 removed │ ████████████████████ 91%
    2_DEDUP              │   3090 →   3000 │    90 removed │ ████████████████████ 97%
  ═══════════════════════════════════════════
    📥 Input total         : 10126
    ✅ Lolos ke Sentimen   : 3000
    📊 Overall pass rate   : 29.6%
  ═══════════════════════════════════════════
```

---

## Infrastruktur Docker

### Stack 1 — Ingestion (`ingestion/docker-compose.yml`)

| Service | Image | Port | Fungsi |
|---------|-------|------|--------|
| `postgres` | postgres:15-alpine | — | Database Prefect Server |
| `prefect-server` | prefecthq/prefect:2-latest | 4200 | UI + API orkestrasi |
| `prefect-worker` | custom (collector/Dockerfile) | — | Eksekutor flow ingestion |
| `selenium-chrome` | selenium/standalone-chrome:latest | 4444 | Headless browser (cadangan) |
| `minio` | minio/minio:latest | 9000, 9001 | Object storage |
| `minio-init` | minio/mc:latest | — | Inisialisasi bucket |
| `collector-app` | custom (collector/Dockerfile) | — | Register deployment ke Prefect |
| `pushgateway` | prom/pushgateway:latest | 9091 | Penerima metrics batch |
| `prometheus` | prom/prometheus:latest | 9090 | Time-series database |
| `grafana` | grafana/grafana:latest | 3000 | Dashboard monitoring |

### Stack 2 — Preprocessing + Modelling + Serving (`preprocessing/docker-compose.yml`)

| Service | Container Name | Image | Port | Fungsi |
|---------|---------------|-------|------|--------|
| `spark` | preprocessing-spark | custom (spark/Dockerfile) | — | PySpark preprocessing |
| `modelling` | preprocessing-modelling | custom (modelling/Dockerfile) | — | scikit-learn modelling |
| `serving` | serving-api | custom (serving/Dockerfile) | 8000 | FastAPI serving |

### Network
- Semua service terhubung dalam `ingestion_ingestion-net` (bridge network)
- Preprocessing stack menggunakan `external: true` untuk join ke network ingestion

### Volumes
- `postgres-data` — data PostgreSQL
- `minio-data` — data MinIO (persistent)
- `prefect-data` — konfigurasi Prefect
- `spark-logs` — log preprocessing
- `modelling-logs` — log modelling
- `logs` — log collector
- `prometheus-data` — data Prometheus
- `grafana-data` — data Grafana

---

## Jadwal Eksekusi Harian

| Waktu (WIB) | Komponen | Deployment | Durasi | Tools |
|-------------|----------|------------|--------|-------|
| 06:00 | Ingestion Pre-market | `euro-news-pre-market` | ~5 menit | Prefect |
| 09:00 | Ingestion Open | `euro-news-open` | ~5 menit | Prefect |
| 13:00 | Ingestion Mid | `euro-news-mid` | ~5 menit | Prefect |
| 17:00 | Ingestion Pre-close | `euro-news-pre-close` | ~5 menit | Prefect |
| 21:00 | Ingestion Overlap | `euro-news-overlap` | ~5 menit | Prefect |
| **22:00** | **Preprocessing** | `euro-news-preprocessing` | ~15 menit | PySpark |
| **22:30** | **Modelling retrain** | `euro-news-modelling` | ~2 menit | scikit-learn |
| 23:00 | Serving update | — | auto | FastAPI |

Pipeline **tidak aktif di akhir pekan** (Sabtu-Minggu) karena pasar Eropa tutup.

---

## Cara Menjalankan

### 1. Clone dan Setup Environment

```powershell
cd E:\project-ipbd-kelompok11
```

### 2. Start Ingestion Stack

```powershell
cd ingestion
# Isi .env dengan API key
docker compose up -d
```

### 3. Start Preprocessing + Modelling + Serving

```powershell
cd ../preprocessing
docker compose up -d
```

### 4. Build Ulang (setelah ubah dependencies)

```powershell
cd ../preprocessing
docker compose build
```

### 5. Jalankan Preprocessing Manual (Testing)

```powershell
docker exec preprocessing-spark spark-submit ^
  --packages org.apache.hadoop:hadoop-aws:3.4.2 ^
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py ^
  /app/jobs/news_sentiment_job.py --raw
```

### 6. Jalankan Modelling Manual (Testing)

```powershell
docker exec preprocessing-modelling python /app/modelling/run_pipeline.py
```

### 7. Akses Dashboard

| Service | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Prefect UI | http://localhost:4200 |
| Serving API | http://localhost:8000 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

---

## Daftar Teknologi

| Kategori | Teknologi | Versi | Kegunaan |
|----------|-----------|-------|----------|
| **Bahasa** | Python | 3.11 | Bahasa pemrograman utama |
| **Orkestrasi** | Prefect | 2.19.0 / 2.20.25 | Workflow orchestration & scheduling |
| **Database Prefect** | PostgreSQL | 15 | Backend database Prefect |
| **Database Prefect** | asyncpg | 0.29.0 | Async PostgreSQL driver |
| **Scraping** | requests | 2.32.3 | HTTP client untuk REST API |
| **Scraping** | feedparser | 6.0.11 | Parse RSS/Atom feeds |
| **Scraping** | BeautifulSoup4 | 4.12.3 | Parse HTML content |
| **Scraping** | Selenium | 4.15.0 | Headless browser scraping |
| **Big Data** | Apache Spark (PySpark) | 3.5+ | Distributed data processing |
| **Big Data** | Hadoop-AWS (S3A) | 3.4.2 | Spark ↔ MinIO connector |
| **Storage** | MinIO | latest | S3-compatible object storage |
| **Storage (Python)** | boto3 | 1.34+ | AWS SDK untuk MinIO |
| **Storage (Python)** | pyarrow | 14+ | Parquet read/write + S3FileSystem |
| **NLP** | fastText (lid.176) | — | Language identification (917KB model) |
| **NLP** | NLTK | 3.8.1 | VADER sentiment, stopwords, tokenize, lemmatize |
| **ML** | scikit-learn | 1.4.2 | LDA, TF-IDF, LogisticRegression |
| **ML** | joblib | 1.3+ | Model serialization (.pkl) |
| **ML** | numpy | 1.26.4 | Numerical computing |
| **ML** | pandas | 2.1.4 | Data manipulation |
| **API** | FastAPI | 0.100+ | REST API framework |
| **API** | Uvicorn | 0.20+ | ASGI server |
| **Monitoring** | Prometheus | latest | Time-series metric collection |
| **Monitoring** | Pushgateway | latest | Metrics receiver for batch jobs |
| **Monitoring** | Grafana | latest | Dashboard visualization |
| **Monitoring** | prometheus-client | 0.21.1 | Python metrics library |
| **Notification** | Telegram Bot API | — | Real-time notifications |
| **Logging** | Loguru | 0.7.2 | Structured logging |
| **Container** | Docker + Compose | — | Containerization & orchestration |
| **Utilities** | python-dotenv | 1.0.0 | Environment variables |
| **Utilities** | tenacity | 8.3.0 | Retry logic |
| **Utilities** | python-dateutil | 2.9.0 | Date parsing |
| **Utilities** | pytz | 2024.1 | Timezone handling |

---

## Struktur Proyek Lengkap

```
E:\project-ipbd-kelompok11\
│
├── README.md                          ← Dokumentasi utama proyek
├── rangkuman.md                       ← Rangkuman ini
├── .env.example                       ← Template environment variables
├── .gitignore
│
├── ingestion/                         ← 🔵 INGESTION
│   ├── .env                           ← API keys & credentials
│   ├── docker-compose.yml             ← 10 services (MinIO, Prefect, Selenium, dll)
│   ├── prometheus.yml                 ← Prometheus scrape config
│   │
│   └── collector/
│       ├── Dockerfile                 ← python:3.11-slim + dependencies
│       ├── requirements.txt           ← prefect, requests, minio, selenium, dll
│       ├── .prefectignore
│       │
│       └── flows/
│           ├── __init__.py
│           ├── news_ingestion_flow.py  ← ⭐ FLOW UTAMA (5 batch scheduler)
│           ├── backfill_flow.py       ← Backfill Guardian 2021–2025
│           ├── prefect.yaml           ← 5 deployment definitions
│           ├── .prefectignore
│           │
│           ├── scrapers/
│           │   ├── __init__.py
│           │   ├── ecb_scraper.py     ← ECB RSS (4 feeds)
│           │   ├── gdelt_scraper.py   ← GDELT RSS + Guardian API
│           │   └── newsapi_scraper.py ← NewsAPI /v2/everything
│           │
│           ├── storage/
│           │   ├── __init__.py
│           │   └── minio_client.py    ← MinIO upload + dedup + health
│           │
│           └── utils/
│               ├── __init__.py
│               ├── config.py          ← Konfigurasi terpusat
│               ├── telegram_alert.py  ← Telegram notifikasi
│               └── metrics.py         ← Prometheus metrics
│
├── preprocessing/                     ← 🟡 PREPROCESSING
│   ├── .env                           ← MinIO + Telegram credentials
│   ├── docker-compose.yml             ← 3 services (spark, modelling, serving)
│   │
│   └── spark/
│       ├── Dockerfile                 ← python:3.11 + JDK 17 + fastText model
│       ├── entrypoint.sh              ← Startup: Prefect worker + deployment
│       ├── requirements.txt           ← pyspark, nltk, fasttext, prefect, dll
│       ├── .gitignore
│       ├── ALUR_PIPELINE.md           ← Dokumentasi preprocessing
│       │
│       └── jobs/
│           ├── news_sentiment_job.py   ← ⭐ PIPELINE UTAMA (640 baris, 7 layer)
│           ├── news_preprocessing_job.py ← Pipeline awal (legacy, 6 step)
│           ├── preprocessing_flow.py   ← Prefect flow wrapper
│           ├── sentiment_udfs.py      ← Semua pandas_udf (VADER, NLP, keyword, dll)
│           ├── lang_filter.py         ← fastText English detection
│           ├── minio_utils.py         ← SparkSession + S3A + boto3
│           ├── schema.py              ← RAW + PROCESSED schema
│           ├── prefect_bootstrap.py   ← Alternative Python bootstrap
│           ├── prefect.yaml           ← Deployment manifest
│           ├── download_lang_model.sh ← Download fastText model
│           │
│           └── models/
│               └── lid.176.ftz        ← fastText language model (917KB)
│
├── modelling/                         ← 🟠 MODELLING
│   ├── Dockerfile                     ← python:3.11-slim
│   ├── entrypoint.sh                  ← Startup: Prefect worker + deployment
│   ├── requirements.txt               ← scikit-learn, pandas, pyarrow, boto3, dll
│   ├── .dockerignore
│   ├── README.md                      ← Dokumentasi modelling
│   │
│   ├── config.py                      ← Hyperparameter terpusat
│   ├── data_loader.py                 ← Baca Parquet dari MinIO
│   ├── lda_pipeline.py               ← LDA Topic Modeling
│   ├── sentiment_trainer.py           ← TF-IDF + LogisticRegression
│   ├── evaluator.py                   ← Metrics & confusion matrix
│   ├── model_store.py                 ← Save/load .pkl ke MinIO
│   ├── monitor.py                     ← Tracking, Pushgateway, Telegram
│   ├── run_pipeline.py               ← ⭐ ORCHESTRATOR UTAMA (5+1 langkah)
│   ├── modelling_flow.py             ← Prefect flow wrapper
│   └── prefect.yaml                  ← Deployment manifest
│
└── serving/                           ← 🟢 SERVING
    ├── Dockerfile                     ← python:3.11 + FastAPI
    ├── requirements.txt               ← fastapi, uvicorn, pandas, boto3, pyarrow
    ├── app.py                         ← ⭐ FastAPI app (10 endpoints)
    └── venv/                          ← Virtual environment (local dev)
```

---

> **Dibuat oleh Kelompok 11 — Mata Kuliah Infrastruktur dan Platform Big Data**
> 
> **Pipeline end-to-end: Ingestion → Preprocessing → Modelling → Serving**
> **Orkestrasi: Prefect | Storage: MinIO | Monitoring: Prometheus + Grafana**
