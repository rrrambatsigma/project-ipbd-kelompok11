# Sentiment Preprocessing Pipeline — Alur & Dokumentasi

## Ringkasan

Pipeline PySpark ini memproses artikel berita Euro (mentah dari Guardian API) menjadi data siap analisis dengan skor sentimen VADER dan agregasi per sesi trading EUR/USD.

## Diagram Alur

```
                        ┌─────────────────────────┐
                        │  MINIO news-raw/ (JSON)  │
                        │  ~10.126 artikel         │
                        └──────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  Layer 1: LOAD              │
                    │  Spark baca JSON → DataFrame │
                    │  Parse nested fields, cast   │
                    │  tipe data                   │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 2: LANGUAGE FILTER   │
                    │  fastText lid.176 model     │
                    │  Filter bahasa Inggris (p>0.5)│
                    │  10126 → 9600 (-5.2%)       │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 3: TOPIC FILTER      │
                    │  Regex keyword EUR/USD      │
                    │  + NLP keyword expansion    │
                    │  9600 → 3394 (-64.6%)       │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 4: QUALITY FILTER    │
                    │  Min length, max length,    │
                    │  non-ASCII ratio            │
                    │  3394 → 3090 (-9%)          │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 5: DEDUPLICATION     │
                    │  Exact title/body dedup     │
                    │  (Fuzzy Jaccard: opt-in     │
                    │   via --fuzzy flag)         │
                    │  3090 → 3000 (-2.9%)        │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 6: SENTIMENT SCORE   │
                    │  VADER + financial keyword  │
                    │  scoring (uptrend/downtrend,│
                    │  market impact, sentimen    │
                    │  kata kunci ekonomi)        │
                    │  3000 artikel               │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  Layer 7: AGGREGATION       │
                    │  Group by session (timestamp │
                    │  → 5 sesi/hari)             │
                    │  Hitung mean, max, min,     │
                    │  positive/negative ratio    │
                    │  2121 baris agregasi        │
                    └──────────┬──────────────────┘
                               │
                               ▼
                    ┌─────────────────────────────┐
                    │  SAVE TO MINIO              │
                    │  news-processed/sentiment/  │
                    │  ├── articles/ (Parquet,    │
                    │  │   partitioned by         │
                    │  │   year/month)            │
                    │  └── aggregated/            │
                    │      ├── sentiment_by_session│
                    │      │   (Parquet)          │
                    │      └── sentiment_by_      │
                    │          session_csv (CSV)  │
                    └─────────────────────────────┘
```

## Detail Per Layer

### Layer 1: LOAD
- **File:** `news_sentiment_job.py` — fungsi `load_raw_data()`
- **Source:** MinIO `news-raw/` — JSON per artikel dari Guardian API
- **Fallback:** Parquet `news-processed/articles/`
- **Flag:** `--raw` untuk baca dari JSON mentah
- **Schema:** `schema.py` — `RAW_SCHEMA`

### Layer 2: LANGUAGE FILTER
- **File:** `lang_filter.py` — fungsi `filter_english()`
- **Model:** fastText `lid.176.ftz` di `/app/jobs/models/`
- **Logika:** Prediksi bahasa per artikel → simpan hanya `__label__en` dengan probabilitas > 0.5
- **Hasil:** ~5% artikel non-Inggris dibuang

### Layer 3: TOPIC FILTER
- **File:** `sentiment_udfs.py` — UDF `is_euro_topic()`
- **Strategi:** Regex keyword (EUR, USD, ECB, Federal Reserve, forex, exchange rate, inflation, interest rate, dsb) + ekspansi NLP (synonym, lemma)
- **Hasil:** ~65% artikel gak relevan dibuang

### Layer 4: QUALITY FILTER
- **File:** `sentiment_udfs.py` — UDF `is_good_quality()`
- **Kriteria:**
  - Panjang body: 100–15.000 karakter
  - Non-ASCII ratio: maks 10%
  - Gak boleh null atau kosong
- **Hasil:** ~9% artikel kualitas rendah dibuang

### Layer 5: DEDUPLICATION
- **File:** `sentiment_udfs.py` + `news_sentiment_job.py`
- **Exact dedup:** `dropDuplicates(subset=["title_clean"])`
- **Fuzzy dedup (opsional):** Jaccard similarity antar judul. **Matikan default** karena O(n²) — terlalu mahal.
  - Aktifkan: `--fuzzy` flag
  - Threshold: 0.85

### Layer 6: SENTIMENT SCORE
- **File:** `sentiment_udfs.py` — UDF `vader_score_udf()`
- **Model:** NLTK VADER + financial keyword booster
- **Output per artikel:**
  - `sentiment_compound` (−1 s/d +1)
  - `sentiment_pos`, `sentiment_neg`, `sentiment_neu`
  - `sentiment_label` (Positive/Neutral/Negative)
  - `financial_sentiment` (skor khusus kata ekonomi)
  - `market_impact` (uptrend/downtrend signal)

### Layer 7: AGGREGATION
- **File:** `news_sentiment_job.py` — fungsi `aggregate_sentiment()`
- **Session window:** timestamp → 5 sesi per hari:
  - `pre_market` (00:00–06:59)
  - `early_session` (07:00–09:29)
  - `main_session` (09:30–11:59)
  - `afternoon` (12:00–15:59)
  - `post_market` (16:00–23:59)
- **Agregasi:** mean/max/min compound, positive/negative ratio, volatility, article count

## Cara Menjalankan

```bash
# Pipeline lengkap dari raw JSON (default: skip fuzzy dedup)
spark-submit \
  --jars /opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py \
  jobs/news_sentiment_job.py --raw

# Dengan fuzzy dedup (lebih lambat)
spark-submit ... jobs/news_sentiment_job.py --raw --fuzzy

# Baca dari hasil sebelumnya (skip LOAD raw)
spark-submit ... jobs/news_sentiment_job.py --processed
```

Di Docker:
```bash
docker exec preprocessing-spark spark-submit \
  --jars /opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
  --py-files /app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py \
  jobs/news_sentiment_job.py --raw
```

## Output di MinIO

```
news-processed/sentiment/
├── articles/                   # Semua artikel terskor (Parquet)
│   ├── year=2021/
│   │   ├── month=1/ ... month=12/
│   ├── year=2022/ ...
│   ├── ...
│   └── year=2026/
│       └── month=1/ ... month=6/
├── aggregated/
│   ├── sentiment_by_session/   # Agregasi per sesi (Parquet)
│   └── sentiment_by_session_csv/  # Agregasi per sesi (CSV)
```

**Kolom penting di `articles/`:**
- `article_id`, `title`, `body`, `web_url`, `section_id`
- `sentiment_compound`, `sentiment_pos`, `sentiment_neg`, `sentiment_neu`, `sentiment_label`
- `financial_sentiment`, `market_impact`
- `session_tag`, `session_date`, `sentence_count`
- `year`, `month` (partition columns)

**Kolom penting di `sentiment_by_session/`:**
- `session_date`, `session_tag`
- `article_count`, `avg_compound`, `max_compound`, `min_compound`
- `positive_ratio`, `negative_ratio`, `neutral_ratio`
- `avg_financial_sentiment`
- `volatility` (stddev compound)

## Catatan Penting

| Issue | Fix |
|-------|-----|
| JDK 21 + Arrow error | Pindah ke `python:3.11-slim-bookworm` + `openjdk-17-jdk-headless` |
| NumPy 2.x + fasttext | Pinning `numpy==1.26.4` di `requirements.txt` |
| MODEL_PATH relatif | Hardcode `/app/jobs/models/lid.176.ftz` (gak pake `__file__`) |
| OOM Docker (4G→2G) | Driver & executor `2g→1g`, shuffle `4→2`, S3A fast upload |
| Fuzzy dedup lambat | Opt-in via `--fuzzy`, default mati |
| SparkContext S3A jar | Wajib lewat `spark-submit --jars`, gak bisa config di session |
| `--py-files` | Wajib biar worker bisa import `lang_filter`, `sentiment_udfs`, `schema` |

## File dalam Repo

```
preprocessing/spark/
├── Dockerfile                    # Image: python:3.11-slim-bookworm + JDK 17 + NLTK data
├── requirements.txt              # Dependencies (numpy=1.26.4, nltk, fasttext-wheel, pyspark)
├── docker-compose.yml            # Spark service config (1g memory, bind mount jobs/)
├── ALUR_PIPELINE.md              # ← Ini
└── jobs/
    ├── news_sentiment_job.py     # Main pipeline entry point
    ├── minio_utils.py            # SparkSession builder, S3A config
    ├── schema.py                 # RAW_SCHEMA untuk parse JSON
    ├── lang_filter.py            # fastText language detection
    └── sentiment_udfs.py         # Semua UDF: topic, quality, dedup, VADER, session
```
