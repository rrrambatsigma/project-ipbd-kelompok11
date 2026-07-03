# Dokumentasi Modelling — IPBD Kelompok 11 (JOJO)

> **Tanggal:** 2026-06-30  
> **Role:** JOJO (kursadmin) — Analisis Korelasi & Prediksi Arah EUR/USD

---

## Peran dalam Pipeline

Jojo bertanggung jawab atas:
1. **Ingestion harga EUR/USD** via yfinance → Kafka → PostgreSQL
2. **Analisis korelasi** antara sentimen berita (Rambat) dan pergerakan kurs
3. **Prediksi arah EUR/USD** menggunakan XGBoost (menguat / melemah / stabil)
4. **Serving API** via FastAPI untuk endpoint prediksi real-time

---

## Arsitektur Pipeline

```
                    ┌─────────────────────────────────────────────┐
                    │               DATA SOURCES                   │
                    ├──────────────────┬──────────────────────────┤
                    │  Rambat (News)   │   Jojo (EUR/USD)          │
                    │  MinIO Parquet   │   yfinance WebSocket      │
                    │  VADER Sentiment │   Kafka Streaming         │
                    └────────┬─────────┴──────────┬───────────────┘
                             │                    │
                    ┌────────▼─────────┐  ┌───────▼───────────────┐
                    │  sentiment_daily │  │ kurs_raw → kurs_silver│
                    │  (PostgreSQL)    │  │ → kurs_daily           │
                    └────────┬─────────┘  └───────┬───────────────┘
                             │                    │
                    ┌────────▼────────────────────▼───────────────┐
                    │         JOJO — Analisis Korelasi             │
                    │  • Pearson/Spearman correlation              │
                    │  • Lag analysis (Lag-2 r=0.077**)           │
                    │  • XGBoost + SMOTE + TimeSeriesSplit         │
                    └────────────────────┬────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │          SERVING & MONITORING               │
                    │  FastAPI (/predict/today)                   │
                    │  Grafana Dashboard (port 3001)              │
                    │  Telegram Bot Notifikasi                    │
                    └─────────────────────────────────────────────┘
```

---

## Tools & Library yang Digunakan

### Ingestion & Streaming
| Tool | Versi | Fungsi |
|------|-------|--------|
| yfinance | latest | WebSocket streaming harga EUR/USD real-time |
| kafka-python-ng | 2.2.3 | Kafka producer/consumer |
| Apache Kafka | 7.5.0 (Docker) | Message broker untuk streaming data |
| Apache Zookeeper | 7.5.0 (Docker) | Koordinator Kafka |

### Preprocessing & Storage
| Tool | Versi | Fungsi |
|------|-------|--------|
| psycopg2-binary | 2.9.12 | Koneksi PostgreSQL dari Python |
| pandas | 2.3.3 | Manipulasi data tabular (DataFrame) |
| numpy | 2.4.6 | Operasi numerik dan array |
| boto3 | 1.43.37 | Koneksi MinIO (S3-compatible) |
| pyarrow | 23.0.0 | Baca/tulis file Parquet |

### Storage Infrastruktur
| Tool | Versi | Fungsi |
|------|-------|--------|
| PostgreSQL | 15 (Docker) | Serving database (kurs_raw, kurs_silver, kurs_daily, sentiment_daily) |
| MinIO Rambat | latest | Object storage parquet files (Bronze → Silver layer) |

### Modelling
| Tool | Versi | Fungsi |
|------|-------|--------|
| xgboost | 3.2.0 | XGBoost Classifier (model utama) |
| scikit-learn | 1.8.0 | LabelEncoder, TimeSeriesSplit, cross_val_score, metrics |
| imbalanced-learn | 0.14.2 | SMOTE — oversampling kelas minoritas |
| joblib | 1.5.2 | Simpan/load model (.pkl) |
| matplotlib | 3.11.0 | Plotting (confusion matrix, feature importance, prediksi vs aktual) |
| seaborn | 0.13.2 | Heatmap confusion matrix |
| shap | 0.51.0 | Interpretasi model (SHAP values) |
| scipy | latest | Pearson & Spearman correlation analysis |

### Monitoring & Serving
| Tool | Versi | Fungsi |
|------|-------|--------|
| Grafana | latest (Docker) | Dashboard monitoring real-time (port 3001) |
| FastAPI | latest | REST API endpoint prediksi |
| uvicorn | latest | ASGI server untuk FastAPI |
| Telegram Bot API | latest | Notifikasi otomatis pipeline |
| requests | 2.34.2 | HTTP client |

---

## Parameter Terpilih (Jojo + Rambat)

**Total: 24 fitur**

### Kurs Jojo (15 fitur)

| # | Fitur | Deskripsi |
|---|-------|-----------|
| 1 | `price_change_pct` | Persentase perubahan harga close hari ini vs kemarin |
| 2 | `volatility` | Rentang harian (high − low) ternormalisasi |
| 3 | `high_low_range` | Selisih harga tertinggi dan terendah hari ini |
| 4 | `close_vs_open` | Selisih harga close dan open (candle direction) |
| 5 | `close_vs_ma5` | Deviasi close terhadap Moving Average 5 hari |
| 6 | `close_vs_ma10` | Deviasi close terhadap Moving Average 10 hari |
| 7 | `ma5_vs_ma10` | Spread MA5 vs MA10 (trend crossover signal) |
| 8 | `lag1_change_pct` | price_change_pct 1 hari sebelumnya |
| 9 | `lag2_change_pct` | price_change_pct 2 hari sebelumnya |
| 10 | `lag3_change_pct` | price_change_pct 3 hari sebelumnya |
| 11 | `lag1_volatility` | Volatilitas 1 hari sebelumnya |
| 12 | `rolling3_avg_change` | Rata-rata price_change_pct 3 hari terakhir |
| 13 | `rolling5_avg_change` | Rata-rata price_change_pct 5 hari terakhir |
| 14 | `rolling3_volatility` | Rata-rata volatilitas 3 hari terakhir |
| 15 | `momentum_5d` | (close_hari_ini / close_5hari_lalu − 1) × 100% |

### Rambat Sentimen (9 fitur)

| # | Fitur | Deskripsi | Korelasi |
|---|-------|-----------|----------|
| 16 | `avg_sentiment` | Rata-rata VADER compound score per hari | r=0.0019 |
| 17 | `sentiment_volatility` | Std dev VADER compound per hari | r=−0.0138 |
| 18 | `sentiment_lag1` | avg_sentiment 1 hari sebelumnya | r=0.0229 |
| 19 | `sentiment_lag2` | avg_sentiment 2 hari sebelumnya | **r=0.077**\*\* (terbaik) |
| 20 | `has_ecb` | Proporsi artikel menyebut ECB per hari | r=0.0512\* |
| 21 | `has_interest_rate` | Proporsi artikel menyebut suku bunga | r=0.0322 |
| 22 | `has_monetary_policy` | Proporsi artikel menyebut kebijakan moneter | r=−0.0336 |
| 23 | `positive_ratio` | positive_count / total_news (engineered) | — |
| 24 | `negative_ratio` | negative_count / total_news (engineered) | — |

---

## Algoritma: XGBoost (Extreme Gradient Boosting)

**Cara Kerja:**  
XGBoost membangun ensemble pohon keputusan (decision tree) secara bertahap (boosting). Setiap pohon baru dilatih untuk memperbaiki kesalahan residu dari ensemble sebelumnya, menggunakan gradient descent pada fungsi loss multi-class (softmax). Regularisasi L1 (`reg_alpha`) dan L2 (`reg_lambda`) mencegah overfitting.

**Hyperparameter yang Digunakan:**

```python
XGBClassifier(
    n_estimators     = 300,
    max_depth        = 5,
    learning_rate    = 0.05,
    subsample        = 0.8,
    colsample_bytree = 0.8,
    min_child_weight = 5,
    gamma            = 0.1,
    reg_alpha        = 0.1,
    reg_lambda       = 1.0,
    eval_metric      = "mlogloss",
    random_state     = 42,
    n_jobs           = -1
)
```

**Alasan Memilih XGBoost:**
1. Data tabular harian (bukan sekuensial murni) — XGBoost cocok karena menggunakan lag features dan rolling features
2. Handle imbalanced class lebih mudah (dikombinasikan dengan SMOTE)
3. Feature importance mudah diinterpretasikan (F-score, SHAP values)
4. Training cepat, scalable, dan stabil untuk ~1400 sampel harian
5. Toleran terhadap missing values dan outlier

**SMOTE (Synthetic Minority Over-sampling Technique):**  
Digunakan untuk menangani ketidakseimbangan kelas (stabil ~55%, melemah/menguat ~22% masing-masing). Kelas minoritas di-oversample secara sintetis dengan menginterpolasi sampel tetangga terdekat (k-NN = 5), sehingga model tidak bias ke kelas mayoritas.

**TimeSeriesSplit Cross-Validation (5-fold):**  
Tidak menggunakan random split karena data memiliki dependensi waktu. TimeSeriesSplit memastikan data masa depan tidak bocor ke training set. Setiap fold memperluas window training secara berurutan ke depan.

**Target Label:**
- `menguat` : price_change_pct > +0.3%
- `melemah` : price_change_pct < −0.3%
- `stabil`  : −0.3% ≤ price_change_pct ≤ +0.3%

---

## Hasil Baseline

**Data:**
- Symbol: EURUSD=X
- Periode: 2021-01-18 → 2026-06-30
- Total hari: 1,411 (setelah feature engineering)
- Train: 1,128 hari | Test: 283 hari (80/20 time-based split)
- Test period: 2025-05-27 → 2026-06-29
- Sentimen Rambat tersedia: 903/1417 hari (63.7%)

**Hasil Model:**

| Metrik | Nilai |
|--------|-------|
| **Accuracy** | **55.48%** |
| CV 5-fold mean | 56.34% |
| CV 5-fold std | ±8.58% |
| CV fold scores | [43.4%, 53.2%, 68.9%, 54.5%, 61.7%] |

**Classification Report:**

| Kelas | Precision | Recall | F1-Score | Support |
|-------|-----------|--------|----------|---------|
| melemah | 0.29 | 0.31 | 0.30 | 59 |
| menguat | 0.26 | 0.19 | 0.22 | 59 |
| stabil | 0.72 | 0.78 | 0.74 | 165 |
| **weighted avg** | **0.53** | **0.55** | **0.54** | **283** |

**Top 10 Feature Importance (XGBoost Gain):**

| Rank | Fitur | Importance |
|------|-------|-----------|
| 1 | high_low_range | 0.1333 |
| 2 | volatility | 0.0663 |
| 3 | **sentiment_lag2** | **0.0454** |
| 4 | lag3_change_pct | 0.0427 |
| 5 | **has_monetary_policy** | **0.0419** |
| 6 | lag1_change_pct | 0.0393 |
| 7 | lag2_change_pct | 0.0390 |
| 8 | rolling3_avg_change | 0.0378 |
| 9 | ma5_vs_ma10 | 0.0373 |
| 10 | **sentiment_lag1** | **0.0370** |

> Catatan: `sentiment_lag2` (rank 3) dan `sentiment_lag1` (rank 10) membuktikan temuan analisis korelasi bahwa lag-2 sentimen (r=0.077**) adalah fitur sentimen terkuat.

**Interpretasi:**
- Model paling baik memprediksi kelas **stabil** (F1=0.74) karena dominan dalam data (~55%)
- Prediksi `menguat` dan `melemah` masih lemah (F1 0.22–0.30), wajar untuk prediksi forex harian
- Accuracy 55.48% lebih baik dari random baseline (33.3% untuk 3 kelas) dan dari model kurs saja
- Volatilitas harga (high_low_range, volatility) adalah fitur terpenting, diikuti sentimen lag-2

---

## File Output

| File | Deskripsi |
|------|-----------|
| `xgb_baseline.pkl` | Model XGBoost tersimpan (joblib) |
| `rf_model.pkl` | Copy model untuk kompatibilitas serving |
| `label_encoder.pkl` | LabelEncoder (melemah=0, menguat=1, stabil=2) |
| `baseline_report.txt` | Laporan akurasi lengkap |
| `confusion_matrix.png` | Visualisasi confusion matrix |
| `feature_importance.png` | Top 15 feature importance chart |
| `prediction_vs_actual.png` | Prediksi vs aktual di test set |
| `analisis_report.txt` | Laporan analisis korelasi lengkap |

---

## Cara Menjalankan

```bash
# 1. Load/reload data sentimen Rambat
python3 modelling/load_rambat_sentiment.py

# 2. Retrain model
python3 modelling/model_offline.py

# 3. Jalankan API serving
uvicorn serving.main:app --reload --port 8000

# 4. Start Grafana monitoring
docker compose up -d grafana
# Akses: http://localhost:3001 (admin / admin123)
```

---

## Catatan Teknis

- Data sentimen Rambat: 3,015 total artikel dari 132 parquet files (MinIO)
- Rata-rata artikel per hari: 2.6 (periode 2021–2026)
- Session tags digabungkan: pre_market (21%), open (5%), mid (7%), pre_close (9%), overlap (58%)
- Threshold label ±0.3% dipilih karena pergerakan EUR/USD harian yang kecil (biasanya 0.1–0.5%)
- Time-based split digunakan (bukan random) untuk menghormati struktur time series
