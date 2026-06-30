"""
rafah/modelling/model_commodity.py — IPBD Kelompok 11 (RAFAH)
XGBoost Baseline untuk prediksi arah harga komoditas harian.

Ticker: GLD (Gold), BTC-USD (Bitcoin), SI=F (Silver)
Target : label hari BERIKUTNYA → naik / turun / stabil
Fitur  : 20 fitur teknikal per komoditas (OHLCV + MA + lag + rolling)

Cara pakai:
    python3 rafah/modelling/model_commodity.py --ticker GLD
    python3 rafah/modelling/model_commodity.py --ticker BTC-USD
    python3 rafah/modelling/model_commodity.py --ticker SI=F
    python3 rafah/modelling/model_commodity.py --ticker all
"""

import os
import argparse
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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin",
}

TICKER_MAP = {
    "GLD":     "Gold",
    "BTC-USD": "Bitcoin",
    "SI=F":    "Silver",
}

STABLE_THRESHOLD = 0.5

FEATURE_COLS = [
    "price_change_pct", "volatility",
    "high_low_range", "close_vs_open",
    "close_vs_ma5", "close_vs_ma10", "ma5_vs_ma10",
    "lag1_change_pct", "lag2_change_pct", "lag3_change_pct",
    "lag1_volatility", "lag2_volatility",
    "lag1_hl_range",
    "rolling3_avg_change", "rolling5_avg_change",
    "rolling3_volatility", "rolling5_volatility",
    "momentum_5d",
    "positive_ratio", "negative_ratio",   # dari Rambat
]


# ── Load data dari commodity_daily ────────────────────────────────────────

def load_data(symbol: str) -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONFIG)
    df = pd.read_sql(f"""
        SELECT
            c.trade_date,
            c.open_price, c.high_price, c.low_price, c.close_price,
            c.volatility, c.price_change, c.price_change_pct,
            c.ma5, c.ma10, c.commodity,
            COALESCE(s.positive_count::float / NULLIF(s.total_news,0), 0) AS positive_ratio,
            COALESCE(s.negative_count::float / NULLIF(s.total_news,0), 0) AS negative_ratio
        FROM commodity_daily c
        LEFT JOIN sentiment_daily s ON s.trade_date = c.trade_date
        WHERE c.symbol = '{symbol}'
          AND c.open_price IS NOT NULL
          AND c.close_price IS NOT NULL
        ORDER BY c.trade_date ASC
    """, conn)
    conn.close()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    print(f"[INFO] Data {symbol}: {len(df)} hari | "
          f"{df['trade_date'].min().date()} → {df['trade_date'].max().date()}")
    return df


def compute_label(pct: float) -> str:
    if abs(pct) < STABLE_THRESHOLD:
        return "stabil"
    return "naik" if pct > 0 else "turun"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    df["label"] = df["price_change_pct"].apply(compute_label)

    df["high_low_range"]  = df["high_price"] - df["low_price"]
    df["close_vs_open"]   = df["close_price"] - df["open_price"]
    df["close_vs_ma5"]    = df["close_price"] - df["ma5"]
    df["close_vs_ma10"]   = df["close_price"] - df["ma10"]
    df["ma5_vs_ma10"]     = df["ma5"] - df["ma10"]

    for lag in [1, 2, 3]:
        df[f"lag{lag}_change_pct"] = df["price_change_pct"].shift(lag)
        df[f"lag{lag}_volatility"] = df["volatility"].shift(lag)
        df[f"lag{lag}_hl_range"]   = df["high_low_range"].shift(lag)

    df["rolling3_avg_change"] = df["price_change_pct"].rolling(3).mean()
    df["rolling5_avg_change"] = df["price_change_pct"].rolling(5).mean()
    df["rolling3_volatility"] = df["volatility"].rolling(3).mean()
    df["rolling5_volatility"] = df["volatility"].rolling(5).mean()
    df["momentum_5d"]         = (df["close_price"] / df["close_price"].shift(5) - 1) * 100

    df["target"] = df["label"].shift(-1)
    return df.dropna(subset=FEATURE_COLS + ["target"])


# ── Training ──────────────────────────────────────────────────────────────

def train(symbol: str):
    commodity = TICKER_MAP.get(symbol, symbol)
    print(f"\n{'='*60}")
    print(f"  TRAINING: {commodity} ({symbol})")
    print(f"  Threshold label: ±{STABLE_THRESHOLD}%")
    print(f"{'='*60}")

    df = load_data(symbol)
    df = build_features(df)
    X, y = df[FEATURE_COLS], df["target"]

    print(f"[INFO] Sampel: {len(X)} | Distribusi:")
    for lbl, cnt in y.value_counts().items():
        print(f"  {lbl:8s}: {cnt} ({cnt/len(y)*100:.1f}%)")

    le    = LabelEncoder()
    y_enc = le.fit_transform(y)

    split_idx       = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y_enc[:split_idx], y_enc[split_idx:]

    # SMOTE
    min_class = min(np.bincount(y_train))
    if min_class >= 6:
        smote = SMOTE(random_state=42, k_neighbors=min(5, min_class - 1))
        X_res, y_res = smote.fit_resample(X_train, y_train)
        print(f"[INFO] Setelah SMOTE: {len(X_res)} sampel")
    else:
        X_res, y_res = X_train, y_train

    model = XGBClassifier(
        n_estimators=300, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, gamma=0.1,
        reg_alpha=0.1, reg_lambda=1.0,
        eval_metric="mlogloss", random_state=42, n_jobs=-1,
    )
    model.fit(X_res, y_res, eval_set=[(X_test, y_test)], verbose=False)

    y_pred      = model.predict(X_test)
    acc         = accuracy_score(y_test, y_pred)
    labels_seen = sorted(set(y_test) | set(y_pred))
    names_seen  = le.inverse_transform(labels_seen)

    print(f"\n{'='*60}")
    print(f"  ACCURACY {commodity}: {acc*100:.2f}%")
    print(f"{'='*60}")
    print(classification_report(y_test, y_pred,
                                 labels=labels_seen, target_names=names_seen,
                                 zero_division=0))

    # Cross-val
    tscv = TimeSeriesSplit(n_splits=5)
    cv = cross_val_score(
        XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                      eval_metric="mlogloss", random_state=42, n_jobs=-1),
        X, y_enc, cv=tscv, scoring="accuracy"
    )
    print(f"[CV] 5-fold: {np.nanmean(cv):.4f} ± {np.nanstd(cv):.4f}")

    # Simpan model
    safe = symbol.replace("/", "-").replace("=", "")
    model_path   = os.path.join(BASE_DIR, f"xgb_{safe}.pkl")
    encoder_path = os.path.join(BASE_DIR, f"encoder_{safe}.pkl")
    report_path  = os.path.join(BASE_DIR, f"report_{safe}.txt")

    joblib.dump(model, model_path)
    joblib.dump(le, encoder_path)

    # Feature importance plot
    importances = pd.Series(model.feature_importances_, index=FEATURE_COLS)
    plt.figure(figsize=(10, 7))
    importances.sort_values(ascending=True).tail(15).plot.barh(color="#FF9800")
    plt.title(f"Feature Importance — {commodity} ({symbol})\nIPBD Kelompok 11 (Rafah)")
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, f"feature_importance_{safe}.png"), dpi=150)
    plt.close()

    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(5, 4))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Oranges",
                xticklabels=names_seen, yticklabels=names_seen)
    plt.title(f"Confusion Matrix — {commodity}")
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, f"confusion_matrix_{safe}.png"), dpi=150)
    plt.close()

    with open(report_path, "w") as f:
        f.write(f"COMMODITY MODEL REPORT — {commodity} ({symbol})\n")
        f.write("=" * 50 + "\n")
        f.write(f"Accuracy : {acc*100:.2f}%\n")
        f.write(f"CV 5-fold: {np.nanmean(cv):.4f} ± {np.nanstd(cv):.4f}\n\n")
        f.write(classification_report(y_test, y_pred,
                                       labels=labels_seen, target_names=names_seen,
                                       zero_division=0))
        f.write("\nTop 10 Feature Importance:\n")
        for feat, imp in importances.sort_values(ascending=False).head(10).items():
            f.write(f"  {feat:<30s}: {imp:.4f}\n")

    print(f"[OK] Model → {model_path}")
    print(f"[OK] Plot  → feature_importance_{safe}.png")
    return acc


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="all",
                        help="GLD | BTC-USD | SI=F | all")
    args = parser.parse_args()

    tickers = list(TICKER_MAP.keys()) if args.ticker == "all" else [args.ticker]

    results = {}
    for sym in tickers:
        acc = train(sym)
        results[sym] = acc

    print(f"\n{'='*60}")
    print("  RINGKASAN ACCURACY KOMODITAS")
    print(f"{'='*60}")
    for sym, acc in results.items():
        print(f"  {TICKER_MAP[sym]:10s} ({sym:8s}): {acc*100:.2f}%")
    print(f"{'='*60}")
    print("\n  Retrain: python3 rafah/modelling/model_commodity.py --ticker all")
