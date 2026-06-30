"""
model_kurs.py — IPBD Kelompok 11
Modelling: Prediksi Arah Pergerakan EUR/USD

Input  : v_market_signals (JOIN kurs + komoditas + sentimen)
Output : label prediksi → menguat / melemah / stabil
Model  : Random Forest Classifier

Cara pakai:
    python3 modelling/model_kurs.py --mode train    # training + simpan model
    python3 modelling/model_kurs.py --mode predict  # prediksi hari ini
    python3 modelling/model_kurs.py --mode evaluate # evaluasi + feature importance
"""

import argparse
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

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    classification_report, confusion_matrix, accuracy_score
)
from sklearn.preprocessing import LabelEncoder

# ── Konfigurasi ───────────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin"
}

MODEL_PATH  = os.path.join(os.path.dirname(__file__), "rf_model.pkl")
ENCODER_PATH = os.path.join(os.path.dirname(__file__), "label_encoder.pkl")

# Fitur yang dipakai dari v_market_signals
# Kolom dari Rafah & Rambat akan di-fillna(0) kalau belum ada datanya
KURS_FEATURES = [
    "kurs_change_pct",
    "kurs_volatility",
    "kurs_ma5",
    "kurs_ma10",
]

COMMODITY_FEATURES = [
    "wti_change_pct",
    "brent_change_pct",
    "gold_change_pct",
    "natgas_change_pct",
    "copper_change_pct",
]

SENTIMENT_FEATURES = [
    "avg_sentiment",
    "positive_count",
    "negative_count",
    "total_news",
    "sentiment_volatility",
]

ALL_FEATURES = KURS_FEATURES + COMMODITY_FEATURES + SENTIMENT_FEATURES

# ── Load data dari PostgreSQL ─────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    """
    Ambil data dari v_market_signals.
    Kalau kolom komoditas/sentimen belum ada (NULL), isi dengan 0.
    """
    conn = psycopg2.connect(**PG_CONFIG)

    query = """
        SELECT
            trade_date,
            kurs_open,
            kurs_close,
            kurs_change_pct,
            kurs_volatility,
            kurs_ma5,
            kurs_ma10,
            kurs_label,
            -- komoditas (bisa NULL kalau Rafah belum isi)
            COALESCE(wti_change_pct,    0) AS wti_change_pct,
            COALESCE(brent_change_pct,  0) AS brent_change_pct,
            COALESCE(gold_change_pct,   0) AS gold_change_pct,
            COALESCE(natgas_change_pct, 0) AS natgas_change_pct,
            COALESCE(copper_change_pct, 0) AS copper_change_pct,
            -- sentimen (bisa NULL kalau Rambat belum isi)
            COALESCE(avg_sentiment,        0) AS avg_sentiment,
            COALESCE(positive_count,       0) AS positive_count,
            COALESCE(negative_count,       0) AS negative_count,
            COALESCE(total_news,           0) AS total_news,
            COALESCE(sentiment_volatility, 0) AS sentiment_volatility
        FROM v_market_signals
        WHERE kurs_label IS NOT NULL
        ORDER BY trade_date ASC
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"[INFO] Data loaded: {len(df)} baris dari {df['trade_date'].min()} s/d {df['trade_date'].max()}")
    return df


def prepare_features(df: pd.DataFrame):
    """
    Buat fitur X dan target Y.
    Target: label BESOK (shift -1) — prediksi hari berikutnya.
    """
    df = df.copy()

    # Target = label kurs hari berikutnya
    df["target"] = df["kurs_label"].shift(-1)
    df = df.dropna(subset=["target"])

    # Tambah fitur lag (kemarin dan 2 hari lalu)
    for col in ["kurs_change_pct", "kurs_volatility"]:
        df[f"{col}_lag1"] = df[col].shift(1)
        df[f"{col}_lag2"] = df[col].shift(2)

    df = df.dropna()

    feature_cols = ALL_FEATURES + [
        "kurs_change_pct_lag1", "kurs_change_pct_lag2",
        "kurs_volatility_lag1", "kurs_volatility_lag2",
    ]

    X = df[feature_cols]
    y = df["target"]

    return X, y, df


# ── Training ──────────────────────────────────────────────────────────────

def train():
    print("\n" + "="*55)
    print("  TRAINING: Random Forest — Prediksi Arah EUR/USD")
    print("="*55)

    df = load_data()
    X, y, _ = prepare_features(df)

    print(f"[INFO] Total sampel    : {len(X)}")
    print(f"[INFO] Distribusi label:\n{y.value_counts().to_string()}")

    # Encode label
    le = LabelEncoder()
    y_enc = le.fit_transform(y)

    # Split train/test — pakai time-based split (jangan random untuk time series)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y_enc[:split_idx], y_enc[split_idx:]

    print(f"[INFO] Train: {len(X_train)} | Test: {len(X_test)}")

    # Model
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        min_samples_split=10,
        min_samples_leaf=5,
        class_weight="balanced",   # handle imbalanced label
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluasi
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"\n[RESULT] Accuracy: {acc:.4f} ({acc*100:.2f}%)")
    print("\n[RESULT] Classification Report:")
    print(classification_report(y_test, y_pred, target_names=le.classes_))

    # Cross-validation
    cv_scores = cross_val_score(model, X, y_enc, cv=5, scoring="accuracy")
    print(f"[RESULT] Cross-val (5-fold): {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    # Simpan model
    joblib.dump(model, MODEL_PATH)
    joblib.dump(le, ENCODER_PATH)
    print(f"\n[INFO] Model disimpan → {MODEL_PATH}")

    # Feature importance
    plot_feature_importance(model, X.columns.tolist(), le)

    return model, le


# ── Evaluasi ──────────────────────────────────────────────────────────────

def evaluate():
    if not os.path.exists(MODEL_PATH):
        print("[ERROR] Model belum ada. Jalankan --mode train dulu.")
        return

    model = joblib.load(MODEL_PATH)
    le    = joblib.load(ENCODER_PATH)

    df = load_data()
    X, y, df_feat = prepare_features(df)

    y_enc  = le.transform(y)
    y_pred = model.predict(X)

    print("\n" + "="*55)
    print("  EVALUASI MODEL")
    print("="*55)
    print(f"Accuracy: {accuracy_score(y_enc, y_pred)*100:.2f}%\n")
    print(classification_report(y_enc, y_pred, target_names=le.classes_))

    # Confusion matrix
    cm = confusion_matrix(y_enc, y_pred)
    plt.figure(figsize=(6, 5))
    sns.heatmap(
        cm, annot=True, fmt="d", cmap="Blues",
        xticklabels=le.classes_, yticklabels=le.classes_
    )
    plt.title("Confusion Matrix — Prediksi Arah EUR/USD")
    plt.ylabel("Aktual")
    plt.xlabel("Prediksi")
    plt.tight_layout()
    out = os.path.join(os.path.dirname(__file__), "confusion_matrix.png")
    plt.savefig(out, dpi=150)
    print(f"[INFO] Confusion matrix disimpan → {out}")

    plot_feature_importance(model, X.columns.tolist(), le)


# ── Prediksi hari ini ─────────────────────────────────────────────────────

def predict():
    if not os.path.exists(MODEL_PATH):
        print("[ERROR] Model belum ada. Jalankan --mode train dulu.")
        return

    model = joblib.load(MODEL_PATH)
    le    = joblib.load(ENCODER_PATH)

    df = load_data()
    X, y, df_feat = prepare_features(df)

    # Ambil baris terakhir = kondisi hari ini
    X_latest = X.iloc[[-1]]
    date_latest = df_feat["trade_date"].iloc[-1]

    pred_enc  = model.predict(X_latest)[0]
    pred_proba = model.predict_proba(X_latest)[0]
    pred_label = le.inverse_transform([pred_enc])[0]

    print("\n" + "="*55)
    print("  PREDIKSI ARAH EUR/USD — HARI BERIKUTNYA")
    print("="*55)
    print(f"  Berdasarkan data   : {date_latest}")
    print(f"  Prediksi besok     : {pred_label.upper()}")
    print(f"\n  Probabilitas:")
    for cls, prob in zip(le.classes_, pred_proba):
        bar = "█" * int(prob * 30)
        print(f"    {cls:10s} {bar:30s} {prob*100:.1f}%")

    # Konteks data hari ini
    print(f"\n  Konteks hari ini:")
    print(f"    EUR/USD change : {df_feat['kurs_change_pct'].iloc[-1]:+.4f}%")
    print(f"    Volatility     : {df_feat['kurs_volatility'].iloc[-1]:.5f}")
    print(f"    MA5            : {df_feat['kurs_ma5'].iloc[-1]:.5f}")
    print(f"    MA10           : {df_feat['kurs_ma10'].iloc[-1]:.5f}")
    if df_feat["avg_sentiment"].iloc[-1] != 0:
        print(f"    Avg Sentiment  : {df_feat['avg_sentiment'].iloc[-1]:+.4f}")
    if df_feat["gold_change_pct"].iloc[-1] != 0:
        print(f"    Gold change    : {df_feat['gold_change_pct'].iloc[-1]:+.4f}%")

    return pred_label


# ── Utility ───────────────────────────────────────────────────────────────

def plot_feature_importance(model, feature_names: list, le):
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1][:15]  # top 15

    plt.figure(figsize=(10, 6))
    colors = []
    for i in indices:
        name = feature_names[i]
        if name in KURS_FEATURES or "lag" in name:
            colors.append("#2196F3")   # biru = kurs (Jojo)
        elif name in COMMODITY_FEATURES:
            colors.append("#FF9800")   # oranye = komoditas (Rafah)
        else:
            colors.append("#4CAF50")   # hijau = sentimen (Rambat)

    plt.barh(
        range(len(indices)),
        importances[indices],
        color=colors,
        edgecolor="white"
    )
    plt.yticks(range(len(indices)), [feature_names[i] for i in indices])
    plt.xlabel("Feature Importance")
    plt.title("Top 15 Fitur — Prediksi Arah EUR/USD\n🔵 Kurs (Jojo)  🟠 Komoditas (Rafah)  🟢 Sentimen (Rambat)")
    plt.tight_layout()
    plt.gca().invert_yaxis()

    out = os.path.join(os.path.dirname(__file__), "feature_importance.png")
    plt.savefig(out, dpi=150)
    print(f"[INFO] Feature importance disimpan → {out}")
    plt.close()


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model Prediksi Arah EUR/USD")
    parser.add_argument(
        "--mode",
        choices=["train", "predict", "evaluate"],
        default="train",
        help="Mode: train | predict | evaluate"
    )
    args = parser.parse_args()

    if args.mode == "train":
        train()
    elif args.mode == "predict":
        predict()
    elif args.mode == "evaluate":
        evaluate()
