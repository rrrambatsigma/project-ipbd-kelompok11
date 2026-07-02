"""
model_offline.py — IPBD Kelompok 11 (JOJO)
Offline Baseline Training dari data harian REAL (kurs_daily + sentiment_daily)

Sumber data :
  - kurs_daily      — backfill yfinance 2021–2026
  - sentiment_daily — Rambat MinIO parquet (vader_compound, keyword flags)

Granularity : Harian
Target Y    : label hari BERIKUTNYA (menguat / melemah / stabil)

Threshold label:
  menguat = price_change_pct >  +0.3%
  melemah = price_change_pct <  -0.3%
  stabil  = antara -0.3% dan +0.3%

FITUR TERPILIH (24 total):
  Jojo kurs (15): price_change_pct, volatility, high_low_range, close_vs_open,
                   close_vs_ma5, close_vs_ma10, ma5_vs_ma10,
                   lag1_change_pct, lag2_change_pct, lag3_change_pct,
                   lag1_volatility, rolling3_avg_change, rolling5_avg_change,
                   rolling3_volatility, momentum_5d
  Rambat sentiment (9): avg_sentiment, sentiment_volatility,
                         sentiment_lag1, sentiment_lag2,
                         has_ecb, has_interest_rate, has_monetary_policy,
                         positive_ratio, negative_ratio
"""

import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import psycopg2
import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from xgboost import XGBClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from imblearn.over_sampling import SMOTE

# ── Path ──────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH    = os.path.join(BASE_DIR, "xgb_baseline.pkl")
RF_MODEL_PATH = os.path.join(BASE_DIR, "rf_model.pkl")
ENCODER_PATH  = os.path.join(BASE_DIR, "label_encoder.pkl")
REPORT_PATH   = os.path.join(BASE_DIR, "baseline_report.txt")

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin"
}

STABLE_THRESHOLD = 0.3

# ── 24 Selected Features ─────────────────────────────────────────────────
# Jojo kurs features (15)
KURS_FEATURES = [
    "price_change_pct",
    "volatility",
    "high_low_range",
    "close_vs_open",
    "close_vs_ma5",
    "close_vs_ma10",
    "ma5_vs_ma10",
    "lag1_change_pct",
    "lag2_change_pct",
    "lag3_change_pct",
    "lag1_volatility",
    "rolling3_avg_change",
    "rolling5_avg_change",
    "rolling3_volatility",
    "momentum_5d",
]

# Rambat sentiment features (9)
SENTIMENT_FEATURES = [
    "avg_sentiment",
    "sentiment_volatility",
    "sentiment_lag1",       # lag-1 sentiment (dari analisis korelasi)
    "sentiment_lag2",       # lag-2 sentiment, r=0.077 ** (terbaik)
    "has_ecb",              # r=0.051, paling signifikan
    "has_interest_rate",    # r=0.032
    "has_monetary_policy",  # kebijakan moneter
    "positive_ratio",       # engineered: positive_count / total_news
    "negative_ratio",       # engineered: negative_count / total_news
]

FEATURE_COLS = KURS_FEATURES + SENTIMENT_FEATURES  # 24 total


# ── 0. Ensure schema — tambah kolom keyword kalau belum ada ──────────────

def ensure_schema():
    """Tambah kolom keyword flags ke sentiment_daily jika belum ada (self-healing)."""
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    keyword_cols = [
        ("has_inflation",        "DOUBLE PRECISION DEFAULT 0"),
        ("has_interest_rate",    "DOUBLE PRECISION DEFAULT 0"),
        ("has_ecb",              "DOUBLE PRECISION DEFAULT 0"),
        ("has_monetary_policy",  "DOUBLE PRECISION DEFAULT 0"),
        ("has_gdp",              "DOUBLE PRECISION DEFAULT 0"),
        ("has_recession",        "DOUBLE PRECISION DEFAULT 0"),
        ("has_growth",           "DOUBLE PRECISION DEFAULT 0"),
        ("has_trade",            "DOUBLE PRECISION DEFAULT 0"),
        ("has_forex",            "DOUBLE PRECISION DEFAULT 0"),
        ("has_currency",         "DOUBLE PRECISION DEFAULT 0"),
    ]
    for col_name, col_def in keyword_cols:
        try:
            cur.execute(f"ALTER TABLE sentiment_daily ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
        except Exception as e:
            conn.rollback()
            print(f"[WARN] Schema {col_name}: {e}")
    conn.commit()
    cur.close()
    conn.close()
    print("[INFO] Schema sentiment_daily: OK (keyword columns ensured)")


# ── 1. Load data dari PostgreSQL ──────────────────────────────────────────

def load_daily_data(symbol: str = "EURUSD=X") -> pd.DataFrame:
    ensure_schema()
    conn = psycopg2.connect(**PG_CONFIG)
    df = pd.read_sql(f"""
        SELECT
            k.trade_date,
            k.open_price, k.high_price, k.low_price, k.close_price,
            k.volatility,
            k.price_change, k.price_change_pct,
            k.ma5, k.ma10,
            -- Sentimen Rambat (COALESCE 0 kalau belum ada)
            COALESCE(s.avg_sentiment,        0) AS avg_sentiment,
            COALESCE(s.sentiment_volatility, 0) AS sentiment_volatility,
            COALESCE(s.positive_count,       0) AS positive_count,
            COALESCE(s.negative_count,       0) AS negative_count,
            COALESCE(s.neutral_count,        0) AS neutral_count,
            COALESCE(s.total_news,           0) AS total_news,
            -- Keyword flags terpilih berdasarkan korelasi
            COALESCE(s.has_ecb,             0) AS has_ecb,
            COALESCE(s.has_interest_rate,   0) AS has_interest_rate,
            COALESCE(s.has_monetary_policy, 0) AS has_monetary_policy
        FROM kurs_daily k
        LEFT JOIN sentiment_daily s ON s.trade_date = k.trade_date
        WHERE k.symbol = %(symbol)s
          AND k.open_price  IS NOT NULL
          AND k.close_price IS NOT NULL
        ORDER BY k.trade_date ASC
    """, conn, params={"symbol": symbol})
    conn.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"])

    matched = (df["avg_sentiment"] != 0).sum()
    print(f"[INFO] Data harian ({symbol}): {len(df)} hari")
    print(f"[INFO] Periode: {df['trade_date'].min().date()} → {df['trade_date'].max().date()}")
    print(f"[INFO] Hari dengan data sentimen Rambat: {matched}/{len(df)} "
          f"({matched/len(df)*100:.1f}%)")
    return df


# ── 2. Feature engineering ────────────────────────────────────────────────

def compute_label(pct: float) -> str:
    if pct > STABLE_THRESHOLD:
        return "menguat"
    elif pct < -STABLE_THRESHOLD:
        return "melemah"
    return "stabil"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("trade_date").reset_index(drop=True)

    # Label dari price_change_pct
    df["label"] = df["price_change_pct"].apply(compute_label)

    # ── Jojo kurs features ──
    df["high_low_range"]  = df["high_price"] - df["low_price"]
    df["close_vs_open"]   = df["close_price"] - df["open_price"]
    df["close_vs_ma5"]    = df["close_price"] - df["ma5"]
    df["close_vs_ma10"]   = df["close_price"] - df["ma10"]
    df["ma5_vs_ma10"]     = df["ma5"] - df["ma10"]

    # Lag kurs
    df["lag1_change_pct"] = df["price_change_pct"].shift(1)
    df["lag2_change_pct"] = df["price_change_pct"].shift(2)
    df["lag3_change_pct"] = df["price_change_pct"].shift(3)
    df["lag1_volatility"] = df["volatility"].shift(1)

    # Rolling kurs
    df["rolling3_avg_change"] = df["price_change_pct"].rolling(3).mean()
    df["rolling5_avg_change"] = df["price_change_pct"].rolling(5).mean()
    df["rolling3_volatility"] = df["volatility"].rolling(3).mean()

    # Momentum 5 hari
    df["momentum_5d"] = (df["close_price"] / df["close_price"].shift(5) - 1) * 100

    # ── Rambat sentiment features ──
    # Lag sentiment (penting: lag-2 r=0.077**)
    df["sentiment_lag1"] = df["avg_sentiment"].shift(1)
    df["sentiment_lag2"] = df["avg_sentiment"].shift(2)

    # Engineered ratios
    df["positive_ratio"] = np.where(
        df["total_news"] > 0,
        df["positive_count"] / df["total_news"],
        0.0
    )
    df["negative_ratio"] = np.where(
        df["total_news"] > 0,
        df["negative_count"] / df["total_news"],
        0.0
    )

    # Target = label hari BERIKUTNYA
    df["target"] = df["label"].shift(-1)

    df = df.dropna(subset=FEATURE_COLS + ["target"])
    return df


# ── 3. Training ───────────────────────────────────────────────────────────

def train():
    print("\n" + "=" * 65)
    print("  OFFLINE BASELINE — XGBoost + Rambat Sentiment")
    print("  Data Harian EUR/USD | 24 Selected Features")
    print(f"  Threshold label: ±{STABLE_THRESHOLD}%")
    print("=" * 65)

    df = load_daily_data("EURUSD=X")
    df = build_features(df)

    X = df[FEATURE_COLS]
    y = df["target"]

    print(f"\n[INFO] Total sampel setelah feature engineering: {len(X)}")
    print(f"[INFO] Features: {len(FEATURE_COLS)}")
    print(f"  Kurs (Jojo)     : {len(KURS_FEATURES)} fitur")
    print(f"  Sentimen (Rambat): {len(SENTIMENT_FEATURES)} fitur")

    print(f"\n[INFO] Distribusi label target (hari berikutnya):")
    vc = y.value_counts()
    for label, cnt in vc.items():
        pct = cnt / len(y) * 100
        bar = "█" * int(pct / 3)
        print(f"  {label:10s}: {cnt:4d} ({pct:.1f}%) {bar}")

    # Encode label
    le    = LabelEncoder()
    y_enc = le.fit_transform(y)
    print(f"\n[INFO] Kelas: {list(le.classes_)}")

    # Time-based split 80/20 (no random — time series!)
    split_idx       = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y_enc[:split_idx], y_enc[split_idx:]
    dates_test      = df["trade_date"].iloc[split_idx:].values

    print(f"\n[INFO] Train: {len(X_train)} hari | Test: {len(X_test)} hari")
    print(f"[INFO] Test period: "
          f"{df['trade_date'].iloc[split_idx].date()} → "
          f"{df['trade_date'].iloc[-1].date()}")

    # SMOTE untuk imbalanced class
    min_class = min(np.bincount(y_train))
    if min_class >= 6:
        k = min(5, min_class - 1)
        smote        = SMOTE(random_state=42, k_neighbors=k)
        X_res, y_res = smote.fit_resample(X_train, y_train)
        unique, counts = np.unique(y_res, return_counts=True)
        print(f"\n[INFO] Setelah SMOTE: {len(X_res)} sampel")
        for u, c in zip(le.inverse_transform(unique), counts):
            print(f"  {u}: {c}")
    else:
        X_res, y_res = X_train, y_train
        print(f"[WARN] SMOTE dilewati (min kelas={min_class})")

    # XGBoost Classifier
    model = XGBClassifier(
        n_estimators      = 300,
        max_depth         = 5,
        learning_rate     = 0.05,
        subsample         = 0.8,
        colsample_bytree  = 0.8,
        min_child_weight  = 5,
        gamma             = 0.1,
        reg_alpha         = 0.1,
        reg_lambda        = 1.0,
        eval_metric       = "mlogloss",
        random_state      = 42,
        n_jobs            = -1
    )
    model.fit(X_res, y_res, eval_set=[(X_test, y_test)], verbose=False)

    # ── Evaluasi ──
    y_pred      = model.predict(X_test)
    acc         = accuracy_score(y_test, y_pred)
    labels_seen = sorted(set(y_test) | set(y_pred))
    names_seen  = le.inverse_transform(labels_seen)

    print(f"\n{'=' * 65}")
    print(f"  BASELINE ACCURACY : {acc * 100:.2f}%")
    print(f"{'=' * 65}")
    report = classification_report(
        y_test, y_pred,
        labels=labels_seen,
        target_names=names_seen,
        zero_division=0
    )
    print(report)

    # ── TimeSeriesSplit Cross-Validation (5-fold) ──
    print("[INFO] Menjalankan TimeSeriesSplit 5-fold cross-validation...")
    tscv = TimeSeriesSplit(n_splits=5)
    cv_model = XGBClassifier(
        n_estimators     = 200,
        max_depth        = 5,
        learning_rate    = 0.05,
        eval_metric      = "mlogloss",
        random_state     = 42,
        n_jobs           = -1
    )
    cv_scores = cross_val_score(cv_model, X, y_enc, cv=tscv, scoring="accuracy")
    cv_mean   = float(np.nanmean(cv_scores))
    cv_std    = float(np.nanstd(cv_scores))
    print(f"[CV] TimeSeriesSplit (5-fold): {cv_mean:.4f} ± {cv_std:.4f}")
    print(f"[CV] Scores per fold: {[round(float(s), 4) for s in cv_scores]}")

    # ── Simpan model ──
    joblib.dump(model, MODEL_PATH)
    joblib.dump(le,    ENCODER_PATH)
    print(f"\n[INFO] Model     → {MODEL_PATH}")
    print(f"[INFO] Encoder   → {ENCODER_PATH}")

    # ── Feature importance ──
    importances = pd.Series(
        model.feature_importances_, index=FEATURE_COLS
    ).sort_values(ascending=False)

    print(f"\n[INFO] Top 10 Feature Importance:")
    for feat, imp in importances.head(10).items():
        bar = "█" * int(imp * 200)
        print(f"  {feat:<30s}: {imp:.4f} {bar}")

    # ── Tulis report ──
    _write_report(df, X_train, X_test, acc, cv_mean, cv_std, cv_scores,
                  report, importances)

    # ── Plot ──
    _plot_confusion_matrix(y_test, y_pred, names_seen)
    _plot_feature_importance(model, importances)
    _plot_prediction_vs_actual(dates_test, y_test, y_pred, le)

    return model, le, acc


# ── Report writer ─────────────────────────────────────────────────────────

def _write_report(df, X_train, X_test, acc, cv_mean, cv_std, cv_scores,
                  report, importances):
    lines = [
        "BASELINE MODEL REPORT — IPBD Kelompok 11 (JOJO)",
        "=" * 65,
        "Model        : XGBoost Classifier",
        "Sumber       : kurs_daily + sentiment_daily",
        "Symbol       : EURUSD=X",
        "Granularity  : Harian",
        f"Threshold    : ±{STABLE_THRESHOLD}%",
        f"Total data   : {len(df)} hari",
        f"Train / Test : {len(X_train)} / {len(X_test)}",
        f"Accuracy     : {acc * 100:.2f}%",
        f"CV (5-fold)  : {cv_mean:.4f} ± {cv_std:.4f}",
        f"CV scores    : {[round(float(s), 4) for s in cv_scores]}",
        "=" * 65,
        "",
        "CLASSIFICATION REPORT:",
        report,
        "",
        f"FITUR TERPILIH ({len(FEATURE_COLS)} total):",
        f"  Kurs Jojo ({len(KURS_FEATURES)}): " + ", ".join(KURS_FEATURES),
        f"  Rambat Sentimen ({len(SENTIMENT_FEATURES)}): " + ", ".join(SENTIMENT_FEATURES),
        "",
        "TOP 10 FEATURE IMPORTANCE:",
    ]
    for feat, imp in importances.head(10).items():
        lines.append(f"  {feat:<30s}: {imp:.4f}")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"[INFO] Report    → {REPORT_PATH}")


# ── Plot helpers ──────────────────────────────────────────────────────────

def _plot_confusion_matrix(y_test, y_pred, classes):
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=classes, yticklabels=classes)
    plt.title(
        f"Confusion Matrix — XGBoost (24 Fitur)\n"
        f"Prediksi Arah EUR/USD Harian (threshold ±{STABLE_THRESHOLD}%)"
    )
    plt.ylabel("Aktual")
    plt.xlabel("Prediksi")
    plt.tight_layout()
    out = os.path.join(BASE_DIR, "confusion_matrix.png")
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"[INFO] confusion_matrix.png saved")


def _plot_feature_importance(model, importances):
    top15 = importances.sort_values(ascending=True).tail(15)
    colors = []
    for feat in top15.index:
        if feat in SENTIMENT_FEATURES:
            colors.append("#FF9800")   # orange = sentiment
        else:
            colors.append("#2196F3")   # blue = kurs

    plt.figure(figsize=(10, 7))
    plt.barh(top15.index, top15.values, color=colors, edgecolor="white")
    plt.xlabel("XGBoost Feature Importance (Gain)")
    plt.title(
        "Top 15 Fitur — XGBoost EUR/USD (24 Features)\n"
        "🔵 Kurs (Jojo)  🟠 Sentimen (Rambat)"
    )
    plt.tight_layout()
    out = os.path.join(BASE_DIR, "feature_importance.png")
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"[INFO] feature_importance.png saved")


def _plot_prediction_vs_actual(dates, y_test, y_pred, le):
    num_map = {"menguat": 1, "stabil": 0, "melemah": -1}
    actual  = [num_map.get(le.inverse_transform([y])[0], 0) for y in y_test]
    pred    = [num_map.get(le.inverse_transform([y])[0], 0) for y in y_pred]

    plt.figure(figsize=(14, 4))
    plt.plot(dates, actual, label="Aktual",   alpha=0.7, linewidth=1.5)
    plt.plot(dates, pred,   label="Prediksi", alpha=0.7, linewidth=1.5, linestyle="--")
    plt.yticks([-1, 0, 1], ["Melemah", "Stabil", "Menguat"])
    plt.title("Prediksi vs Aktual — Arah EUR/USD Harian (Test Set)")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    out = os.path.join(BASE_DIR, "prediction_vs_actual.png")
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"[INFO] prediction_vs_actual.png saved")


# ── TOOLS YANG DIGUNAKAN ──────────────────────────────────────────────────

def print_tools_used():
    print("""
╔══════════════════════════════════════════════════════════════╗
║         TOOLS YANG DIGUNAKAN — IPBD Kelompok 11 (JOJO)      ║
╠══════════════════════════════════════════════════════════════╣
║  DATA SOURCE                                                 ║
║  • PostgreSQL 15 (Docker, port 5433)                         ║
║    - kurs_daily   : harga EUR/USD harian (yfinance backfill) ║
║    - sentiment_daily : agregasi sentimen Rambat              ║
║  • MinIO (Rambat, 100.118.244.91:9000)                       ║
║    - 132 parquet files (VADER sentiment + keyword flags)     ║
║                                                              ║
║  PYTHON LIBRARIES                                            ║
║  • pandas          : manipulasi DataFrame tabular            ║
║  • numpy           : operasi numerik & array                 ║
║  • psycopg2-binary : koneksi PostgreSQL                      ║
║  • boto3 + pyarrow : baca parquet dari MinIO                 ║
║  • xgboost         : XGBoost Classifier (model utama)        ║
║  • scikit-learn    : LabelEncoder, TimeSeriesSplit,          ║
║                      cross_val_score, classification_report  ║
║  • imbalanced-learn: SMOTE (oversample kelas minoritas)      ║
║  • matplotlib      : plotting confusion matrix, importance   ║
║  • seaborn         : heatmap confusion matrix                ║
║  • joblib          : simpan/load model pickle                ║
║                                                              ║
║  ALGORITMA                                                   ║
║  • XGBoost (Extreme Gradient Boosting)                       ║
║  • SMOTE (Synthetic Minority Over-sampling Technique)        ║
║  • TimeSeriesSplit Cross-Validation (5-fold)                 ║
║                                                              ║
║  FEATURES (24 total)                                         ║
║  • Kurs Jojo (15): price_change_pct, volatility,             ║
║    high_low_range, close_vs_open, close_vs_ma5/ma10,        ║
║    ma5_vs_ma10, lag1-3_change_pct, lag1_volatility,          ║
║    rolling3/5_avg_change, rolling3_volatility, momentum_5d   ║
║  • Rambat Sentimen (9): avg_sentiment, sentiment_volatility, ║
║    sentiment_lag1/lag2, has_ecb, has_interest_rate,          ║
║    has_monetary_policy, positive_ratio, negative_ratio       ║
╚══════════════════════════════════════════════════════════════╝
""")


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print_tools_used()

    model, le, acc = train()

    if model is not None:
        print(f"\n{'=' * 65}")
        print(f"  ✅ BASELINE SELESAI — IPBD Kelompok 11 (JOJO)")
        print(f"  Accuracy    : {acc * 100:.2f}%")
        print(f"  Features    : {len(FEATURE_COLS)} (15 kurs + 9 sentimen)")
        print(f"  Model saved : xgb_baseline.pkl + rf_model.pkl")
        print(f"  Encoder     : label_encoder.pkl")
        print(f"  Plots       : confusion_matrix.png, feature_importance.png,")
        print(f"                prediction_vs_actual.png")
        print(f"{'=' * 65}")
        print(f"\n  Serving API  : uvicorn serving.main:app --reload")
        print(f"  Retrain      : python3 modelling/model_offline.py")
