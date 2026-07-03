# 🟠 MODELLING — Text Mining Pipeline

Container terpisah untuk **LDA Topic Modeling** + **TF-IDF LogisticRegression Classifier**.
Jalan setiap hari **22:30 WIB** (30 menit setelah preprocessing selesai).

---

## 📐 Arsitektur Modelling Pipeline

```
                          ┌─────────────────────────────────────┐
                          │     MinIO Object Storage             │
                          │  (ingestion_ingestion-net)           │
                          │                                      │
                          │  news-processed/sentiment/articles/  │
                          │    ├── year=2021/month=1/part-*.parquet
                          │    ├── year=2022/month=3/part-*.parquet
                          │    ├── ...                           │
                          │    └── year=2026/month=6/part-*.parquet
                          │                                      │
                          │  news-processed/models/              │
                          │    ├── lda/vectorizer_*.pkl          │
                          │    ├── lda/lda_*.pkl                 │
                          │    ├── lda/topics_*.parquet          │
                          │    ├── sentiment/vectorizer_*.pkl    │
                          │    ├── sentiment/classifier_*.pkl    │
                          │    ├── sentiment/report_*.csv        │
                          │    └── metrics_history.csv           │
                          └──────────┬──────────────────────────┘
                                     │
                           (boto3 + pyarrow)
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         run_pipeline.py                                 │
│                                                                        │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────────┐  │
│  │  data_loader.py  │   │  model_store.py  │   │    monitor.py       │  │
│  │                  │   │                  │   │                     │  │
│  │ Baca parquet     │   │ Simpan .pkl      │   │ Metrics history     │  │
│  │ dari MinIO via   │   │ ke MinIO         │   │ antar-run tracking  │  │
│  │ pyarrow S3 FS    │   │ Download model   │   │ Trend analysis      │  │
│  └────────┬─────────┘   └────────┬─────────┘   └─────────────────────┘  │
│           │                      ▲                                       │
│           ▼                      │                                       │
│  ┌───────────────────┐          │                                       │
│  │   Pandas DataFrame │          │                                       │
│  │   (3.004 baris)   │──────────┘                                       │
│  └───────────────────┘                                                  │
│           │                                                              │
│           │                                                              │
│    ┌──────┴──────────────┐                                              │
│    ▼                     ▼                                               │
│  ┌──────────────┐   ┌──────────────────┐                                │
│  │ lda_pipeline │   │ sentiment_trainer│                                │
│  │              │   │                  │                                │
│  │ CountVector  │   │ TfidfVectorizer  │                                │
│  │ → LDA (8)    │   │ → LogisticReg    │                                │
│  │ → topik      │   │ → label VADER    │                                │
│  │ → coherence  │   │ → eval metrics   │                                │
│  └──────┬───────┘   └────────┬─────────┘                                │
│         │                    │                                           │
│         ▼                    ▼                                           │
│  ┌──────────────┐   ┌──────────────────┐                                │
│  │ topics_df    │   │ classification   │                                │
│  │ + lda model  │   │ report + clf     │                                │
│  └──────────────┘   └──────────────────┘                                │
└────────────────────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────────────────────────────────────────────┐
│           Output ke MinIO (model_store.py)           │
│                                                      │
│  news-processed/models/lda/                          │
│    ├── vectorizer_{run_id}.pkl                       │
│    ├── lda_{run_id}.pkl                              │
│    ├── topics_{run_id}.parquet                       │
│    └── top_words_{run_id}.csv                        │
│                                                      │
│  news-processed/models/sentiment/                    │
│    ├── vectorizer_{run_id}.pkl                       │
│    ├── classifier_{run_id}.pkl                       │
│    └── report_{run_id}.csv                           │
│                                                      │
│  news-processed/models/metrics_history.csv           │
│    (append: run_id, accuracy, f1, coherence, ...)   │
└─────────────────────────────────────────────────────┘
```

---

## 🔄 Alur Lengkap (Step-by-Step)

### Step 0: Sebelum Mulai — Pahami Datanya

Dari preprocessing, kamu punya **3.004 artikel** dengan schema:

| Kolom | Contoh | Untuk Modelling? |
|-------|--------|:---:|
| `article_id` | MD5 hash URL | ✅ Join key |
| `clean_text` | "ECB raises interest rates..." | ✅ **Fitur utama** |
| `vader_compound` | 0.78 / -0.32 / 0.01 | ✅ **Label classifier** |
| `vader_pos/neg/neu` | 0.45 / 0.10 / 0.45 | 📊 Distribusi |
| `has_inflation` | True / False | ✅ Fitur tambahan |
| `has_interest_rate` | True / False | ✅ Fitur tambahan |
| `has_ecb` | True / False | ✅ Fitur tambahan |
| `has_forex` | True / False | ✅ Fitur tambahan |
| `published_at` | 2026-06-23 | 📊 Filter waktu |
| `source` | gdelt / guardian / ecb | 📊 Analisis source |
| `category` | business / politics | 📊 Analisis kategori |

---

### Step 1: Load Data — `data_loader.py`

**Apa yang terjadi:**
1. Konek ke MinIO pakai `pyarrow.fs.S3FileSystem`
2. Baca semua file Parquet dari `news-processed/sentiment/articles/`
3. Load ke Pandas DataFrame
4. Filter kolom yang relevan

**Kode:**
```python
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq

fs = S3FileSystem(
    access_key="minioadmin",
    secret_key="minioadmin123",
    endpoint_override="minio:9000",
    scheme="http",
)
table = pq.read_table("news-processed/sentiment/articles", filesystem=fs)
df = table.to_pandas()
```

**Output:** DataFrame ~3.004 baris × 25+ kolom.

---

### Step 2: LDA Topic Modeling — `lda_pipeline.py`

**Teori singkat:** LDA (Latent Dirichlet Allocation) adalah model probabilistik yang mengelompokkan dokumen ke topik-topik berdasarkan pola kemunculan kata. Setiap dokumen adalah campuran dari beberapa topik.

**Apa yang terjadi:**

```
clean_text (3.004 artikel)
        │
        ▼
┌───────────────────┐
│ CountVectorizer    │ ← ubah teks jadi matrix jumlah kata
│ max_df=0.95        │   buang kata yang muncul di >95% artikel
│ min_df=2           │   buang kata yang muncul di <2 artikel
│ stop_words='english'│   buang stop words (the, a, an, ...)
└────────┬──────────┘
         ▼
  shape: (3004, ~5000)  ← matrix sparse
         │
         ▼
┌───────────────────┐
│ LDA                │ ← cari pola kemunculan bersama (co-occurrence)
│ n_components=8     │   8 topik
│ random_state=42    │   reproducible
└────────┬──────────┘
         ▼
┌───────────────────┐
│ Transform          │ ← setiap artikel → distribusi prob per topik
└────────┬──────────┘
         ▼
  shape: (3004, 8)  ← misal artikel-1: [0.7, 0.1, 0.0, 0.1, 0.0, 0.0, 0.05, 0.05]
                               → dominant_topic = topic_0 (prob 0.7)
         │
         ▼
┌───────────────────┐
│ Top words per topic│
│                     │
│ Topic 0: rate, ecb, interest, inflation, central, bank, ...
│ Topic 1: gdp, growth, economy, quarter, forecast, ...
│ Topic 2: trade, tariff, china, export, import, ...
│ Topic 3: oil, price, energy, supply, demand, ...
│ Topic 4: brexit, eu, uk, deal, trade, ...
│ Topic 5: stock, market, index, investor, ...
│ Topic 6: unemployment, job, labor, wage, ...
│ Topic 7: currency, euro, usd, dollar, exchange, forex, ...
└───────────────────┘
```

**Evaluasi:** Topic coherence = seberapa sering kata dalam satu topik muncul bersama di dokumen yang sama. Nilai >0.4 umumnya bagus.

**Output LDA:**
| article_id | dominant_topic | topic_0_prob | topic_1_prob | ... |
|------------|:---:|:---:|:---:|:---:|
| abc123 | topic_7 (forex) | 0.02 | 0.05 | ... |
| def456 | topic_0 (monetary) | 0.78 | 0.01 | ... |

---

### Step 3: Sentiment Classifier — `sentiment_trainer.py`

**Apa yang terjadi:**

```
clean_text (3.004) + vader_compound (label)
        │
        ▼
┌───────────────────────────┐
│ Labeling                  │
│                           │
│ vader_compound ≥ 0.05   → positive  (label=2)
│ |vader_compound| < 0.05 → neutral   (label=1)
│ vader_compound ≤ -0.05  → negative  (label=0)
└───────────┬───────────────┘
            ▼
     Distribusi label:
     positive: ~1.200 artikel
     neutral:  ~1.000 artikel
     negative: ~800 artikel
            │
            ▼
┌───────────────────────────┐
│ Train/Test Split 80/20    │
│                           │
│ Train: 2.403 artikel      │
│ Test:  601 artikel        │
│ (stratify → proporsi label│
│  sama di train & test)    │
└───────────┬───────────────┘
            │
            ▼
┌───────────────────────────┐
│ TfidfVectorizer           │
│                           │
│ max_features=5000         │ ← ambil 5000 kata paling penting
│ ngram_range=(1,2)         │ ← unigram + bigram ("ecb rate", "fed hike")
│ min_df=2                  │ ← buang kata langka
└───────────┬───────────────┘
            ▼
     shape train: (2403, 5000)  ← matrix sparse TF-IDF
            │
            ▼
┌───────────────────────────┐
│ LogisticRegression        │
│                           │
│ max_iter=1000             │ ← konvergensi terjamin
│ class_weight='balanced'   │ ← handle class imbalance
│ multi_class='multinomial' │ ← 3 kelas
└───────────┬───────────────┘
            ▼
     Train model → dapat coefficient per kelas
            │
            ▼
┌───────────────────────────┐
│ Predict test set (601)    │
└───────────┬───────────────┘
            ▼
┌───────────────────────────┐
│ Evaluasi                  │
│                           │
│ ┌──────────┬──────┬──────┬──────┐
│ │ Class    │ Prec │ Rec  │ F1   │
│ ├──────────┼──────┼──────┼──────┤
│ │ negative │ 0.80 │ 0.83 │ 0.81 │
│ │ neutral  │ 0.72 │ 0.69 │ 0.70 │
│ │ positive │ 0.85 │ 0.78 │ 0.81 │
│ ├──────────┼──────┼──────┼──────┤
│ │ macro avg│ 0.79 │ 0.77 │ 0.77 │
│ └──────────┴──────┴──────┴──────┘
│                           │
│ Accuracy: 0.77            │
│                           │
│ Confusion Matrix:         │
│              Predicted    │
│              neg  neu  pos│
│ Actual neg  [120   15   5]│
│        neu  [ 20  140  30]│
│        pos  [ 10   25  236]│
└───────────────────────────┘
```

**Arti metrik:**
- **Precision:** Dari yang diprediksi sebagai X, berapa % benar-benar X?
- **Recall:** Dari yang benar-benar X, berapa % yang terdeteksi?
- **F1:** Rata-rata harmonik precision & recall — metrik utama
- **Macro avg:** Rata-rata semua kelas (tanpa peduli jumlah sampel)

---

### Step 4: Simpan Model — `model_store.py`

Semua model + evaluasi diupload ke MinIO:

```
news-processed/models/
│
├── lda/
│   ├── vectorizer_20260623_223000.pkl    ← CountVectorizer (bisa untuk prediksi artikel baru)
│   ├── lda_20260623_223000.pkl           ← Model LDA (bisa untuk transform artikel baru)
│   ├── topics_20260623_223000.parquet    ← Topic assignment per artikel
│   └── top_words_20260623_223000.csv     ← Top-10 kata per topik
│
├── sentiment/
│   ├── vectorizer_20260623_223000.pkl    ← TfidfVectorizer
│   ├── classifier_20260623_223000.pkl    ← LogisticRegression model
│   └── report_20260623_223000.csv        ← Classification report + confusion matrix
│
└── metrics_history.csv                    ← Riwayat semua run (append)
```

---

### Step 5: Monitoring — `monitor.py`

Setiap selesai training, satu baris ditambahkan ke `metrics_history.csv`:

```csv
run_id,timestamp,accuracy,precision_macro,recall_macro,f1_macro,lda_coherence,n_train,n_test
20260622_223000,2026-06-22 22:30:00,0.75,0.74,0.73,0.73,0.38,2400,600
20260623_223000,2026-06-23 22:30:00,0.77,0.79,0.77,0.77,0.41,2403,601
```

Kalau di-plot, kamu bisa lihat **trend performa** dari hari ke hari. Turun? Ada masalah. Naik? Model makin baik.

---

## 📊 Penjelasan Output

### Output LDA — `top_words_{run_id}.csv`

```
topic_id,word_1,word_2,word_3,word_4,word_5,word_6,word_7,word_8,word_9,word_10
0,rate,ecb,interest,inflation,central,bank,cut,hike,lending,policy
1,gdp,growth,economy,quarter,forecast,expansion,recovery,annual,slower,outlook
2,trade,tariff,china,export,import,barrier,war,negotiation,deal,restriction
3,oil,price,energy,supply,demand,crisis,shortage,fuel,gas,production
4,brexit,eu,uk,deal,trade,transition,custom,regulation,standard,market
5,stock,market,index,investor,equity,volatility,rally,decline,sector,benchmark
6,unemployment,job,labor,wage,employment,hiring,claim,payroll,worker,jobless
7,currency,euro,usd,dollar,exchange,forex,strength,weakness,appreciation,depreciation
```

**Interpretasi:** Kalau artikel punya topik dominan `0`, ia bicara tentang suku bunga & inflasi. Topik `7` bicara tentang forex. Ini berguna untuk:
- Segmentasi artikel berdasarkan isi
- Analisis sentimen per topik (topik mana yang paling negatif?)
- Dashboard filter by topic

### Output Classifier — `report_{run_id}.csv`

```
class,precision,recall,f1,support
negative,0.80,0.83,0.81,140
neutral,0.72,0.69,0.70,190
positive,0.85,0.78,0.81,271
macro avg,0.79,0.77,0.77,601
weighted avg,0.79,0.77,0.77,601
accuracy,0.77,0.77,0.77,0.77
```

**Interpretasi:**
- Kelas **negative** paling bagus (F1=0.81) — model bagus mendeteksi sentimen negatif
- Kelas **neutral** paling rendah (F1=0.70) — ini wajar karena batas antara netral & positif/negatif tipis
- Kalau F1 turun drastis antar run, ada yang salah dengan data atau preprocessing

---

## 🚀 Cara Menjalankan

### Manual (testing)

```powershell
docker exec preprocessing-modelling python run_pipeline.py
```

### Via Prefect (scheduled)

```powershell
docker exec preprocessing-modelling prefect deployment run 'euro-news-modelling/euro-news-modelling'
```

### Dari container build

```powershell
cd E:\project-ipbd-kelompok11\modelling
docker build -t modelling .
docker run --rm --network ingestion_ingestion-net modelling
```

---

## ✅ Checklist Sebelum Run Pertama

- [ ] Container preprocessing aktif & data sudah di MinIO
- [ ] MinIO credentials di `config.py` sesuai (`minioadmin` / `minioadmin123`)
- [ ] Bisa `docker exec preprocessing-spark python -c "import pyarrow; print('OK')"` — pastikan pyarrow ada
- [ ] Kalau pyarrow belum ada: tambahkan ke `requirements.txt` preprocessing atau pakai container terpisah

---

## 🧪 Skenario Jika Performa Kurang

| Masalah | Kemungkinan Penyebab | Solusi |
|---------|---------------------|--------|
| F1 rendah di semua kelas | VADER label tidak akurat | Coba label threshold berbeda (±0.1 / ±0.2) |
| | Teks artikel terlalu pendek | Filter artikel dengan clean_text > 100 kata |
| | Stop words tidak dibuang | Pastikan TfidfVectorizer pakai stop_words |
| F1 kelas neutral rendah | Batas netral terlalu tipis | Naikkan threshold ±0.1 jadi ±0.2 |
| LDA coherence < 0.3 | Terlalu banyak/sedikit topik | GridSearch coba 5-15 topik |
| | Banyak noise di teks | Tambah preprocessing (lowercase, hapus angka) |
| Akurasi turun antar run | Distribusi label berubah | Cek distribusi vader_compound tiap run |
| | Data baru berbeda pola | Retrain lebih sering atau tambah fitur |

---

## 📁 Struktur Folder

```
E:\project-ipbd-kelompok11\modelling\
│
├── README.md                 ← (ini) panduan lengkap
├── requirements.txt          ← Python dependencies
├── Dockerfile                ← Build container
├── .dockerignore
│
├── config.py                 ← Settings terpusat (MinIO, path, hyperparams)
├── data_loader.py            ← Baca artikel dari MinIO via pyarrow
├── lda_pipeline.py           ← CountVectorizer → LDA → topic assignment
├── sentiment_trainer.py      ← TfidfVectorizer → LogisticRegression → eval
├── evaluator.py              ← Metrics, confusion matrix, coherence score
├── model_store.py            ← Simpan/load .pkl + CSV ke MinIO
├── monitor.py                ← Metrics history & trend tracking
├── run_pipeline.py           ← Orchestrator: urutkan semua step
│
├── modelling_flow.py         ← Prefect flow wrapper untuk scheduler
├── prefect.yaml              ← Deployment config (cron 22:30 WIB)
├── entrypoint.sh             ← Container startup script
│
└─── models/                  ← (local cache, .gitignored)
```

---

## 🔗 Integrasi dengan Komponen Lain

| Komponen | Koneksi | Arah |
|----------|---------|:----:|
| Preprocessing | Baca `news-processed/sentiment/articles/` dari MinIO | ← Input |
| Preprocessing | Tulis `news-processed/models/` ke MinIO | → Output |
| Dashboard | Baca model .pkl + topic .parquet dari MinIO | → Serving |
| Prefect | Trigger via cron 22:30 WIB | Orkestrasi |

---

## 🎯 Ringkasan untuk Kamu (Yang Perlu Kamu Pahami)

1. **Kamu punya** 3.004 artikel berita Euro yang sudah di-VADER (skor sentiment)
2. **Step 1:** Baca artikel dari MinIO → Pandas DataFrame
3. **Step 2:** LDA → cari 8 topik yang muncul di artikel (inflation, forex, gdp, ...)
4. **Step 3:** TF-IDF + LogisticRegression → latih classifier sentimen yang lebih akurat dari VADER
5. **Step 4:** Simpan semuanya ke MinIO (model + hasil)
6. **Step 5:** Catat metrik performa ke file riwayat
7. **Hasil akhir:** Dashboard bisa baca model + topik + sentimen → visualisasi korelasi dengan EUR/USD
