# 📰 Analisis Market Flow Nilai Tukar Euro
### Kelompok 11 — IPBD

Pipeline end-to-end: **ingestion berita** → **preprocessing sentimen** → **text mining modelling** → **serving dashboard**.

---

## 🏗️ Arsitektur End-to-End

```
                      BATCH INGESTION (5x/hari — Prefect)
┌──────────────────┐   06:00  09:00  13:00  17:00  21:00 WIB
│    Ingestion     │ ────────────────────────────────────────────→  MinIO news-raw/
│  ECB · Reuters   │                                                    (JSON)
│  NewsAPI · GDELT │
└──────────────────┘
       │
       │ TRIGGER (22:00 WIB — 1x/hari)
       ▼
┌──────────────────┐
│   Preprocessing  │ ──→  MinIO news-processed/sentiment/
│    (PySpark)     │       ├── articles/ (Parquet, partition by year/month)
│                   │       │   3000+ artikel │ VADER scored
│  filter → VADER   │       └── aggregated/sentiment_by_session/
│  → agregasi sesi  │           2121 baris │ 5 sesi/hari
│  → save ke MinIO  │
└──────────────────┘
       │
       │ TRIGGER (22:30 WIB — 1x/hari)
       ▼
┌──────────────────┐
│    Modelling     │ ──→  MinIO models/
│  (scikit-learn)  │       ├── lda_model.pkl
│                   │       ├── tfidf_vectorizer.pkl
│  LDA Topic Model  │       └── sentiment_classifier.pkl
│  TF-IDF + LR      │
│  Simpan .pkl      │
└──────────────────┘
       │                          ┌───────────────────────┐
       │ (baca model + data)      │ Data harga EUR/USD     │
       ▼                          │ (temen — streaming)    │
┌──────────────────┐              └───────────┬───────────┘
│     Serving      │ ◄─────────────────────────┘
│  (Dashboard/API) │     Join sentimen + harga → visualisasi
└──────────────────┘
```

---

## 📋 Alur Data Lengkap (Step-by-Step)

Berikut perjalanan **1 artikel berita** dari awal masuk sampai ke dashboard:

```
            ①
  INTERNET ─────→ Ingestion (Prefect) ──→ ② MinIO news-raw/ (JSON)
                                               │
                                               │ ③ 22:00 WIB
                                               ▼
                                  Preprocessing (PySpark)
                                               │
                                  ┌────────────┼────────────┐
                                  ▼            ▼            ▼
                             ④ LANGUAGE  ⑤ TOPIC     ⑥ QUALITY
                              fastText      regex         filter
                              ↓             ↓             ↓
                              └────────────┼────────────┘
                                           ▼
                                      ⑦ DEDUP
                                        exact title
                                           ↓
                                      ⑧ SENTIMENT
                                        VADER scoring
                                           ↓
                                      ⑨ AGGR SESI
                                   5 sesi trading/hari
                                           │
                                           │ 22:00
                                           ▼
                              MinIO news-processed/sentiment/
                              ├── ⑩ articles/ (Parquet)
                              └── ⑪ aggregated/session (Parquet+CSV)
                                           │
                                           │ 22:30
                                           ▼
                              ⑫ Modelling (scikit-learn)
                                ┌── LDA Topic Model
                                └── TF-IDF + LR Classifier
                                           │
                                           ▼
                              ⑬ MinIO models/ (.pkl files)
                                           │
                              ⑭ Loading ke Serving
                                           │
                              ⑮ Join dengan EUR/USD price ─→ ⑯ Dashboard
```

### Penjelasan Detail Tiap Langkah

| Langkah | Nama | Teknologi | Apa yang Terjadi |
|---------|------|-----------|------------------|
| **①** | **Scrape** | Prefect + Scrapers | ECB (RSS), GDELT (RSS), NewsAPI (REST) ambil artikel dari internet. Terjadwal 5x/hari sesuai sesi pasar (06,09,13,17,21 WIB). |
| **②** | **Simpan Raw** | MinIO | Tiap artikel disimpan sebagai file JSON di bucket `news-raw/{source}/{tanggal}/{file}.json`. |
| **③** | **Trigger Preprocessing** | PySpark | Jam 22:00 WIB, pipeline Spark jalan. Baca semua artikel baru dari `news-raw/` via S3A connector. |
| **④** | **Language Filter** | fastText lid.176 | Deteksi bahasa tiap artikel. Hanya artikel **English** (probabilitas > 0.5) yang lanjut. ~5% artikel non-English dibuang. |
| **⑤** | **Topic Filter** | Regex + NLP | Cek apakah artikel relevan dengan EUR/USD. Keyword: *ECB, inflation, interest rate, forex, Federal Reserve,* dll. ~65% artikel gak relevan dibuang. |
| **⑥** | **Quality Filter** | Rule-based | Buang artikel yang body-nya terlalu pendek (<100 char), terlalu panjang (>15.000 char), atau banyak karakter non-ASCII. ~9% dibuang. |
| **⑦** | **Deduplication** | PySpark | Hapus artikel dengan judul duplikat (exact match). Opsional fuzzy dedup (Jaccard similarity) via `--fuzzy` flag. ~3% dibuang. |
| **⑧** | **Sentiment Scoring** | VADER (NLTK) | Skor sentimen tiap artikel: compound score (−1 s/d +1), positive/negative/neutral ratio, financial keyword booster, market impact label. |
| **⑨** | **Session Aggregation** | PySpark | Kelompokkan artikel ke 5 sesi trading per hari (pre_market, open, mid, pre_close, overlap). Hitung mean compound, positive_ratio, volatility, dll. Output: 2.121 baris agregasi dari 3.000 artikel. |
| **⑩** | **Save Articles** | MinIO (Parquet) | Simpan 3.000 artikel terskor ke `news-processed/sentiment/articles/`, partition by `year/month` untuk query efisien. |
| **⑪** | **Save Aggregation** | MinIO (Parquet+CSV) | Simpan hasil agregasi sesi ke `news-processed/sentiment/aggregated/sentiment_by_session/` (Parquet) dan versi CSV-nya. |
| **⑫** | **Modelling LDA** | scikit-learn | Baca artikel dari MinIO → tokenize → TF-IDF → LDA → tiap artikel dikasih topik (inflation, monetary_policy, forex, geopolitics, trade). |
| **⑬** | **Modelling TF-IDF + LR** | scikit-learn | Train classifier sentimen finansial dari TF-IDF features, label dari `financial_sentiment` yang udah ada. Simpan model `.pkl`. |
| **⑭** | **Save Models** | MinIO | Model LDA + TF-IDF + Classifier disimpan ke bucket `models/` untuk dipakai serving. |
| **⑮** | **Join Price Data** | Serving App | Gabung data sentimen dengan data harga EUR/USD (dari streaming temen) berdasarkan session_date + session_tag. |
| **⑯** | **Visualisasi** | Dashboard | Tampilkan korelasi sentimen vs harga, distribusi topik per sesi, tren harian, sinyal trading. |

---

```
E:\project-ipbd-kelompok11\
│   README.md                        ← Ini
│
├───ingestion\                       ← 🔵 INGESTION
│   │   .env                         ← API keys & credentials
│   │   docker-compose.yml           ← Service: MinIO, Prefect, Selenium
│   │   prefect.yaml                 ← Prefect deployment config
│   │
│   └───collector\
│       ├── Dockerfile
│       ├── requirements.txt
│       └───flows\
│           ├── news_ingestion_flow.py   ← Prefect flow utama
│           ├───scrapers\                ← ECB, Reuters, NewsAPI, GDELT
│           ├───storage\                 ← MinIO client
│           └───utils\                   ← Config, Telegram alert
│
├───preprocessing\                   ← 🟡 PREPROCESSING
│   │   docker-compose.yml           ← Spark service
│   │
│   └───spark\
│       ├── Dockerfile               ← python:3.11-slim-bookworm + JDK 17
│       ├── requirements.txt         ← numpy=1.26.4, nltk, fasttext, pyspark
│       ├── ALUR_PIPELINE.md         ← Dokumentasi detail preprocessing
│       └───jobs\
│           ├── news_sentiment_job.py ← Main pipeline (7 layer)
│           ├── minio_utils.py        ← SparkSession builder + S3A config
│           ├── schema.py             ← RAW_SCHEMA
│           ├── lang_filter.py        ← fastText lid.176
│           └── sentiment_udfs.py     ← Semua UDF
│
├───modelling\                       ← 🟠 MODELLING
│   │   (akan diisi: LDA, TF-IDF, classifier)
│   │
│   └───models\                      ← Hasil model (.pkl) → MinIO
│
└───serving\                         ← 🟢 SERVING
    │   (akan diisi: dashboard/API)
    │
    └───streaming_price\             ← (temen: streaming EUR/USD)
```

---

## 🔄 Komponen & Alur Detail

### 1️⃣ Ingestion (5x/hari, 06:00–21:00 WIB)

| Waktu WIB | UTC | Sesi | Lookback |
|-----------|-----|------|----------|
| 06:00 | 23:00 | Pre-market | 7 jam |
| 09:00 | 02:00 | Open | 3 jam |
| 13:00 | 06:00 | Mid | 4 jam |
| 17:00 | 10:00 | Pre-close ✅ London aktif | 4 jam |
| 21:00 | 14:00 | Overlap ✅✅ London+NY | 4 jam |

**Aktivitas:**
- Scrape ECB (RSS), GDELT (RSS), NewsAPI (REST)
- Dedup dalam batch (by URL+title)
- Upload JSON ke MinIO `news-raw/{source}/{date}/{file}.json`
- Kirim notifikasi Telegram (sukses/gagal)

**Service:** `docker compose up -d` (di folder `ingestion/`)
```
prefect-server   ← UI + scheduler (http://localhost:4200)
prefect-worker   ← Eksekutor flow
selenium-chrome  ← Headless browser
collector-app    ← Register deployments
minio            ← Object storage (http://localhost:9001)
```

---

### 2️⃣ Preprocessing (1x/hari, 22:00 WIB)

**Pipeline 7 Layer (PySpark):**

```
LOAD (10.126) → LANGUAGE (9.600) → TOPIC (3.394) → QUALITY (3.090) → DEDUP (3.000) → SENTIMENT → AGGR (2.121)
```

| Layer | Fungsi | Hasil |
|-------|--------|-------|
| LOAD | Baca JSON/MinIO | Parse nested fields |
| LANGUAGE | fastText lid.176 | Filter English (p>0.5) |
| TOPIC | Regex EUR/USD + NLP keyword | Buang artikel tidak relevan |
| QUALITY | Panjang body, non-ASCII ratio | Buang artikel kualitas rendah |
| DEDUP | Exact title dedup (fuzzy opt-in) | Buang duplikat |
| SENTIMENT | VADER + financial keyword booster | Skor compound, pos, neg, neu |
| AGGR | Group by 5 sesi/hari | 2121 baris agregasi |

**Output ke MinIO:**
```
news-processed/sentiment/
├── articles/                         ← Parquet, partition by year/month
│   └── year=2026/month=6/part-*.snappy.parquet
└── aggregated/
    ├── sentiment_by_session/         ← Parquet
    └── sentiment_by_session_csv/     ← CSV
```

**Jalankan manual:**
```powershell
docker exec preprocessing-spark spark-submit ^
  --jars /opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar ^
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py ^
  jobs/news_sentiment_job.py --raw
```

---

### 3️⃣ Modelling (1x/hari, 22:30 WIB — setelah preprocessing)

**Dua model text mining (offline learning):**

#### A. LDA Topic Modeling
- **Input:** `title` + `body` dari 3000+ artikel (baca dari MinIO)
- **Proses:** Tokenize → TF-IDF → LDA → assign topik per artikel
- **Output topik (contoh):** `inflation`, `monetary_policy`, `forex`, `geopolitics`, `trade`
- **Simpan:** `models/lda_model.pkl`, `models/lda_topics.csv`

#### B. TF-IDF + LogisticRegression
- **Input:** TF-IDF dari `body` artikel
- **Label:** Dari kolom `financial_sentiment` / `market_impact` (sudah ada di preprocessing)
- **Output:** Classifier sentimen finansial — akurasi lebih tinggi dari VADER
- **Simpan:** `models/tfidf_vectorizer.pkl`, `models/sentiment_classifier.pkl`

**Catatan:**
- Retrain 1x/hari sudah cukup (LDA & TF-IDF tidak berubah drastis per batch)
- Model disimpan ke MinIO bucket `models/`
- Inference bisa di serving atau preprocessing

---

### 4️⃣ Serving (Dashboard)

- Baca hasil sentimen dari MinIO
- Load model (.pkl) untuk prediksi/inference
- Join dengan data harga EUR/USD dari streaming temen
- Visualisasi: korelasi sentimen vs harga, tren per sesi, distribusi topik

*(akan diisi lebih detail setelah development)*

---

## ⏰ Jadwal Eksekusi Harian (WIB)

| Waktu | Komponen | Durasi | Tools |
|-------|----------|--------|-------|
| 06:00 | Ingestion Pre-market | ~5 menit | Prefect |
| 09:00 | Ingestion Open | ~5 menit | Prefect |
| 13:00 | Ingestion Mid | ~5 menit | Prefect |
| 17:00 | Ingestion Pre-close | ~5 menit | Prefect |
| 21:00 | Ingestion Overlap | ~5 menit | Prefect |
| **22:00** | **Preprocessing** | ~15 menit | PySpark |
| **22:30** | **Modelling retrain** | ~2 menit | scikit-learn |
| 23:00 | Serving update | auto | Dashboard |

---

## 🚀 Cara Menjalankan Semua Service

### 1. Start ingestion stack
```powershell
cd E:\project-ipbd-kelompok11\ingestion
isi .env (API key)
docker compose up -d
```

### 2. Start preprocessing service
```powershell
cd E:\project-ipbd-kelompok11\preprocessing
docker compose up -d
```

### 3. Build Docker image preprocessing (1x setelah ubah dependencies)
```powershell
cd E:\project-ipbd-kelompok11\preprocessing
docker compose build
```

### 4. Jalankan preprocessing manual (testing)
```powershell
docker exec preprocessing-spark spark-submit ^
  --jars /opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar ^
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py ^
  jobs/news_sentiment_job.py --raw
```

### 5. Jalankan modelling manual (testing)
```powershell
docker exec preprocessing-spark python jobs/run_modelling.py
```

---

## 📊 Dashboard & Port

| Service | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Prefect UI | http://localhost:4200 |
| Selenium | http://localhost:4444 |

Login MinIO: `minioadmin` / `minioadmin123`

---

## 👥 Tim Kelompok 11

| Nama | Bagian |
|------|--------|
| ... | Orkestrasi & Ingestion (Prefect + Scrapers) |
| ... | Preprocessing (Apache Spark) |
| ... | Modelling (LDA, TF-IDF, scikit-learn) |
| ... | Streaming EUR/USD + Serving Dashboard |

---

## 📚 Referensi

- [Prefect Documentation](https://docs.prefect.io/2.16.9/)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/API.html)
- [Apache Spark](https://spark.apache.org/docs/latest/)
- [NLTK VADER](https://www.nltk.org/howto/sentiment.html)
- [fastText lid.176](https://fasttext.cc/docs/en/language-identification.html)
- [scikit-learn LDA](https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.LatentDirichletAllocation.html)
