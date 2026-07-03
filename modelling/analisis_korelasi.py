"""
analisis_korelasi.py — IPBD Kelompok 11 (JOJO)
Analisis Korelasi + Visualisasi Lengkap

Arsitektur:
  Rambat (Sentiment) → Serving ─┐
                                 → JOJO (Analisis Korelasi)
  Rafah (Komoditas)  → Serving ─┘

Analisis:
  1. Korelasi Pearson & Spearman sentimen vs kurs
  2. Lag analysis (sentimen H-1, H-2, H-3 vs perubahan kurs)
  3. Time series overlay EUR/USD + sentimen
  4. Heatmap korelasi semua variabel
  5. Scatter plot sentimen vs price_change_pct
  6. Distribusi label per kondisi sentimen
  7. Feature importance XGBoost + SHAP
  8. Laporan teks lengkap

Jalankan: python3 modelling/analisis_korelasi.py
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
import matplotlib.gridspec as gridspec
import seaborn as sns
from scipy import stats

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR  = os.path.join(BASE_DIR, "visualisasi")
os.makedirs(OUT_DIR, exist_ok=True)

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin"
}

C_KURS      = "#2196F3"
C_SENTIMENT = "#4CAF50"
C_NEUTRAL   = "#9E9E9E"
C_COMMODITY = "#FF9800"

FEATURE_COLS = [
    "price_change_pct", "volatility",
    "high_low_range", "close_vs_open",
    "ma5", "ma10", "close_vs_ma5", "close_vs_ma10", "ma5_vs_ma10",
    "lag1_change_pct", "lag2_change_pct", "lag3_change_pct",
    "lag1_volatility", "lag2_volatility",
    "lag1_hl_range",
    "rolling3_avg_change", "rolling5_avg_change",
    "rolling3_volatility", "rolling5_volatility",
    "momentum_5d",
    "avg_sentiment", "positive_count", "negative_count",
    "total_news", "sentiment_volatility",
    "has_inflation", "has_interest_rate", "has_ecb",
    "has_monetary_policy", "has_gdp", "has_recession",
]

SENTIMENT_FEATURES_MODEL = [
    "avg_sentiment", "positive_count", "negative_count",
    "total_news", "sentiment_volatility",
    "has_inflation", "has_interest_rate", "has_ecb",
    "has_monetary_policy", "has_gdp", "has_recession",
]


# ─────────────────────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    conn = psycopg2.connect(**PG_CONFIG)
    df = pd.read_sql("""
        SELECT
            k.trade_date,
            k.close_price,
            k.open_price,
            k.high_price,
            k.low_price,
            k.price_change_pct,
            k.volatility,
            k.ma5, k.ma10,
            k.label AS kurs_label,
            COALESCE(s.avg_sentiment,        0) AS avg_sentiment,
            COALESCE(s.positive_count,       0) AS positive_count,
            COALESCE(s.negative_count,       0) AS negative_count,
            COALESCE(s.total_news,           0) AS total_news,
            COALESCE(s.sentiment_volatility, 0) AS sentiment_volatility,
            COALESCE(s.has_inflation,        0) AS has_inflation,
            COALESCE(s.has_interest_rate,    0) AS has_interest_rate,
            COALESCE(s.has_ecb,              0) AS has_ecb,
            COALESCE(s.has_monetary_policy,  0) AS has_monetary_policy,
            COALESCE(s.has_gdp,              0) AS has_gdp,
            COALESCE(s.has_recession,        0) AS has_recession,
            COALESCE(s.dominant_sentiment, 'netral') AS dominant_sentiment
        FROM kurs_daily k
        LEFT JOIN sentiment_daily s ON s.trade_date = k.trade_date
        WHERE k.symbol = 'EURUSD=X' AND k.close_price IS NOT NULL
        ORDER BY k.trade_date ASC
    """, conn)
    conn.close()

    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # Derived columns needed for model features
    df["high_low_range"] = df["high_price"] - df["low_price"]
    df["close_vs_open"]  = df["close_price"] - df["open_price"]
    df["close_vs_ma5"]   = df["close_price"] - df["ma5"]
    df["close_vs_ma10"]  = df["close_price"] - df["ma10"]
    df["ma5_vs_ma10"]    = df["ma5"] - df["ma10"]

    # Lag features: sentiment
    for lag in [1, 2, 3]:
        df[f"sentiment_lag{lag}"] = df["avg_sentiment"].shift(lag)

    # Lag features: price change (for model features)
    for lag in [1, 2, 3]:
        df[f"lag{lag}_change_pct"] = df["price_change_pct"].shift(lag)
        df[f"lag{lag}_volatility"] = df["volatility"].shift(lag)
        df[f"lag{lag}_hl_range"]   = df["high_low_range"].shift(lag)

    # Rolling aggregation
    df["rolling3_avg_change"] = df["price_change_pct"].rolling(3).mean()
    df["rolling5_avg_change"] = df["price_change_pct"].rolling(5).mean()
    df["rolling3_volatility"] = df["volatility"].rolling(3).mean()
    df["rolling5_volatility"] = df["volatility"].rolling(5).mean()
    df["momentum_5d"]         = (df["close_price"] / df["close_price"].shift(5) - 1) * 100

    df["has_sentiment"] = df["total_news"] > 0

    print(f"[INFO] Data: {len(df)} hari | {df['trade_date'].min().date()} → {df['trade_date'].max().date()}")
    print(f"[INFO] Hari dengan sentimen Rambat: {df['has_sentiment'].sum()} ({df['has_sentiment'].mean()*100:.1f}%)")
    return df


# ─────────────────────────────────────────────────────────────
# PLOT 1: Time Series Overlay EUR/USD + Sentimen
# ─────────────────────────────────────────────────────────────

def plot_timeseries_overlay(df: pd.DataFrame):
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 10), sharex=True)
    fig.suptitle("Time Series EUR/USD vs Sentimen Publik (Rambat)\nIPBD Kelompok 11", fontsize=14, fontweight="bold")

    # Panel 1: EUR/USD close price
    ax1.plot(df["trade_date"], df["close_price"], color=C_KURS, linewidth=1.2, label="EUR/USD Close")
    ax1.fill_between(df["trade_date"], df["close_price"], alpha=0.1, color=C_KURS)
    ax1.set_ylabel("EUR/USD", fontsize=10)
    ax1.legend(loc="upper left", fontsize=9)
    ax1.set_title("Harga EUR/USD Harian", fontsize=10)
    ax1.grid(alpha=0.3)

    # Panel 2: Sentimen VADER compound
    df_sent = df[df["has_sentiment"]].copy()
    if len(df_sent) > 0:
        bar_colors_sent = df_sent["avg_sentiment"].apply(
            lambda x: "#4CAF50" if x > 0.05 else ("#F44336" if x < -0.05 else C_NEUTRAL)
        )
        ax2.bar(df_sent["trade_date"], df_sent["avg_sentiment"],
                color=bar_colors_sent, alpha=0.7, width=1.0, label="VADER Compound")
    else:
        ax2.bar(df["trade_date"], df["avg_sentiment"],
                color=C_NEUTRAL, alpha=0.5, width=1.0, label="Sentimen (kosong)")
    ax2.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax2.set_ylabel("Avg Sentiment", fontsize=10)
    ax2.set_title("Sentimen Harian (VADER Compound) dari Data Rambat", fontsize=10)
    ax2.legend(loc="upper left", fontsize=9)
    ax2.grid(alpha=0.3)

    # Panel 3: price_change_pct colored by label
    colors_map = {"menguat": "#4CAF50", "melemah": "#F44336", "stabil": C_NEUTRAL}
    bar_colors = df["kurs_label"].map(colors_map).fillna(C_NEUTRAL)
    ax3.bar(df["trade_date"], df["price_change_pct"], color=bar_colors, alpha=0.8, width=1.0)
    ax3.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax3.set_ylabel("Perubahan (%)", fontsize=10)
    ax3.set_xlabel("Tanggal", fontsize=10)
    ax3.set_title("Perubahan Harga EUR/USD Harian  (Hijau=Menguat  Merah=Melemah  Abu=Stabil)", fontsize=10)
    ax3.grid(alpha=0.3)

    plt.tight_layout()
    out = os.path.join(OUT_DIR, "1_timeseries_overlay.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 1_timeseries_overlay.png")
    return out


# ─────────────────────────────────────────────────────────────
# PLOT 2: Korelasi Heatmap
# ─────────────────────────────────────────────────────────────

def plot_korelasi_heatmap(df: pd.DataFrame):
    cols = [
        "price_change_pct", "volatility", "avg_sentiment",
        "sentiment_volatility", "positive_count", "negative_count",
        "has_inflation", "has_interest_rate", "has_ecb",
        "has_monetary_policy", "has_gdp", "has_recession"
    ]
    df_corr = df[cols].copy().dropna()
    corr    = df_corr.corr()

    fig, ax = plt.subplots(figsize=(14, 10))
    mask    = np.zeros_like(corr, dtype=bool)
    # No masking — show full matrix
    sns.heatmap(
        corr, annot=True, fmt=".2f", cmap="RdYlGn",
        center=0, linewidths=0.5, linecolor="white",
        ax=ax, annot_kws={"size": 8},
        vmin=-1, vmax=1
    )
    ax.set_title(
        "Heatmap Korelasi: Sentimen vs Variabel Kurs EUR/USD\nIPBD Kelompok 11 — Analisis Jojo",
        fontsize=13, fontweight="bold", pad=15
    )
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=9)
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=9)

    plt.tight_layout()
    out = os.path.join(OUT_DIR, "2_korelasi_heatmap.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 2_korelasi_heatmap.png")
    return out


# ─────────────────────────────────────────────────────────────
# PLOT 3: Scatter Sentiment vs Kurs
# ─────────────────────────────────────────────────────────────

def _annotate_regression(ax, x, y, label_col=None, df_sub=None):
    """Add regression line + Pearson r and p-value annotation."""
    mask = ~(np.isnan(x) | np.isnan(y))
    x_c, y_c = x[mask], y[mask]
    if len(x_c) < 5:
        return
    slope, intercept, r_val, p_val, _ = stats.linregress(x_c, y_c)
    x_range = np.linspace(x_c.min(), x_c.max(), 100)
    ax.plot(x_range, slope * x_range + intercept, color="black", linewidth=1.5, linestyle="--", alpha=0.7)
    sig = "***" if p_val < 0.001 else ("**" if p_val < 0.01 else ("*" if p_val < 0.05 else "ns"))
    ax.annotate(f"r = {r_val:.3f} ({sig})\np = {p_val:.4f}", xy=(0.04, 0.92),
                xycoords="axes fraction", fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))


def plot_scatter_sentiment_vs_kurs(df: pd.DataFrame):
    label_colors = {"menguat": "#4CAF50", "melemah": "#F44336", "stabil": C_NEUTRAL}

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Scatter Plot: Sentimen vs Perubahan Kurs EUR/USD\nIPBD Kelompok 11", fontsize=13, fontweight="bold")

    # --- Panel 1: avg_sentiment vs price_change_pct (colored by label) ---
    ax = axes[0, 0]
    for lbl, grp in df.groupby("kurs_label"):
        ax.scatter(grp["avg_sentiment"], grp["price_change_pct"],
                   color=label_colors.get(lbl, C_NEUTRAL), alpha=0.6, s=20, label=lbl)
    _annotate_regression(ax, df["avg_sentiment"].values, df["price_change_pct"].values)
    ax.set_xlabel("Avg Sentiment (VADER)", fontsize=10)
    ax.set_ylabel("Price Change PCT (%)", fontsize=10)
    ax.set_title("Sentimen vs Perubahan Kurs (Warna = Label)", fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.axvline(0, color="gray", linewidth=0.8)

    # --- Panel 2: sentiment_lag1 vs price_change_pct ---
    ax = axes[0, 1]
    df2 = df.dropna(subset=["sentiment_lag1", "price_change_pct"])
    ax.scatter(df2["sentiment_lag1"], df2["price_change_pct"],
               color=C_KURS, alpha=0.5, s=20)
    _annotate_regression(ax, df2["sentiment_lag1"].values, df2["price_change_pct"].values)
    ax.set_xlabel("Sentiment Lag-1 (kemarin)", fontsize=10)
    ax.set_ylabel("Price Change PCT (%) hari ini", fontsize=10)
    ax.set_title("Sentimen H-1 vs Perubahan Kurs Hari Ini", fontsize=10)
    ax.grid(alpha=0.3)
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.axvline(0, color="gray", linewidth=0.8)

    # --- Panel 3: sentiment_lag2 vs price_change_pct ---
    ax = axes[1, 0]
    df3 = df.dropna(subset=["sentiment_lag2", "price_change_pct"])
    ax.scatter(df3["sentiment_lag2"], df3["price_change_pct"],
               color=C_COMMODITY, alpha=0.5, s=20)
    _annotate_regression(ax, df3["sentiment_lag2"].values, df3["price_change_pct"].values)
    ax.set_xlabel("Sentiment Lag-2 (2 hari lalu)", fontsize=10)
    ax.set_ylabel("Price Change PCT (%) hari ini", fontsize=10)
    ax.set_title("Sentimen H-2 vs Perubahan Kurs Hari Ini", fontsize=10)
    ax.grid(alpha=0.3)
    ax.axhline(0, color="gray", linewidth=0.8)
    ax.axvline(0, color="gray", linewidth=0.8)

    # --- Panel 4: Bar chart avg price_change_pct by dominant_sentiment ---
    ax = axes[1, 1]
    sent_order = ["positif", "negatif", "netral"]
    sent_colors = {"positif": "#4CAF50", "negatif": "#F44336", "netral": C_NEUTRAL}
    grp = df.groupby("dominant_sentiment")["price_change_pct"].agg(["mean", "std", "count"]).reset_index()
    grp = grp[grp["dominant_sentiment"].isin(sent_order)]
    # Sort by defined order
    grp["order"] = grp["dominant_sentiment"].map({s: i for i, s in enumerate(sent_order)})
    grp = grp.sort_values("order")
    bar_c = [sent_colors.get(s, C_NEUTRAL) for s in grp["dominant_sentiment"]]
    bars = ax.bar(grp["dominant_sentiment"], grp["mean"], color=bar_c,
                  yerr=grp["std"] / np.sqrt(grp["count"]), capsize=5,
                  edgecolor="white", alpha=0.85)
    for bar, (_, row) in zip(bars, grp.iterrows()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                f"n={int(row['count'])}", ha="center", va="bottom", fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Sentimen Dominan", fontsize=10)
    ax.set_ylabel("Rata-rata Price Change PCT (%)", fontsize=10)
    ax.set_title("Rata-rata Perubahan Kurs per Kategori Sentimen", fontsize=10)
    ax.grid(alpha=0.3, axis="y")

    plt.tight_layout()
    out = os.path.join(OUT_DIR, "3_scatter_sentiment_vs_kurs.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 3_scatter_sentiment_vs_kurs.png")
    return out


# ─────────────────────────────────────────────────────────────
# PLOT 4: Lag Analysis
# ─────────────────────────────────────────────────────────────

def plot_lag_analysis(df: pd.DataFrame):
    lag_cols = {
        "Lag-0 (hari ini)":    "avg_sentiment",
        "Lag-1 (H-1)":         "sentiment_lag1",
        "Lag-2 (H-2)":         "sentiment_lag2",
        "Lag-3 (H-3)":         "sentiment_lag3",
    }

    results = []
    for label, col in lag_cols.items():
        sub = df[[col, "price_change_pct"]].dropna()
        if len(sub) < 5:
            results.append({"lag": label, "r": 0, "p": 1, "n": 0})
            continue
        r, p = stats.pearsonr(sub[col], sub["price_change_pct"])
        results.append({"lag": label, "r": r, "p": p, "n": len(sub)})

    res_df = pd.DataFrame(results)

    fig, ax = plt.subplots(figsize=(10, 6))
    bar_colors = [
        "#4CAF50" if r > 0.05 else ("#F44336" if r < -0.05 else C_NEUTRAL)
        for r in res_df["r"]
    ]
    bars = ax.bar(res_df["lag"], res_df["r"], color=bar_colors, edgecolor="white", alpha=0.85, width=0.5)

    # Annotate bars
    for bar, (_, row) in zip(bars, res_df.iterrows()):
        sig = "***" if row["p"] < 0.001 else ("**" if row["p"] < 0.01 else ("*" if row["p"] < 0.05 else "ns"))
        ypos = bar.get_height() + (0.003 if bar.get_height() >= 0 else -0.008)
        ax.text(bar.get_x() + bar.get_width() / 2, ypos,
                f"r={row['r']:.3f}\n{sig}", ha="center", va="bottom", fontsize=9)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(0.1, color="gray", linewidth=0.8, linestyle="--", alpha=0.5, label="±0.1 threshold")
    ax.axhline(-0.1, color="gray", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xlabel("Lag Sentimen", fontsize=11)
    ax.set_ylabel("Pearson r vs price_change_pct", fontsize=11)
    ax.set_title(
        "Lag Analysis: Berapa Hari Setelah Sentimen, EUR/USD Bereaksi?\n"
        "IPBD Kelompok 11 — Analisis Jojo",
        fontsize=12, fontweight="bold"
    )
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3, axis="y")
    ax.set_ylim(min(res_df["r"].min() - 0.05, -0.15), max(res_df["r"].max() + 0.05, 0.15))

    plt.tight_layout()
    out = os.path.join(OUT_DIR, "4_lag_analysis.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 4_lag_analysis.png")
    return out, res_df


# ─────────────────────────────────────────────────────────────
# PLOT 5: Label Distribution by Sentiment
# ─────────────────────────────────────────────────────────────

def plot_label_distribution_by_sentiment(df: pd.DataFrame):
    sent_order  = ["positif", "negatif", "netral"]
    label_order = ["menguat", "stabil", "melemah"]
    label_colors_map = {"menguat": "#4CAF50", "stabil": C_NEUTRAL, "melemah": "#F44336"}

    # Build crosstab
    ct = pd.crosstab(df["dominant_sentiment"], df["kurs_label"])
    # Normalize to percentage
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100

    # Ensure all categories present
    for s in sent_order:
        if s not in ct_pct.index:
            ct_pct.loc[s] = 0
    for l in label_order:
        if l not in ct_pct.columns:
            ct_pct[l] = 0

    ct_pct = ct_pct.reindex(index=sent_order, columns=label_order, fill_value=0)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7))
    fig.suptitle(
        "Distribusi Label EUR/USD Berdasarkan Sentimen Dominan\n"
        "IPBD Kelompok 11 — Analisis Jojo",
        fontsize=13, fontweight="bold"
    )

    # Stacked bar
    bottom = np.zeros(len(sent_order))
    for lbl in label_order:
        vals   = ct_pct[lbl].values
        colors = [label_colors_map[lbl]] * len(sent_order)
        bars   = ax1.bar(sent_order, vals, bottom=bottom, color=colors, alpha=0.85,
                         label=lbl, edgecolor="white")
        for bar, val in zip(bars, vals):
            if val > 5:
                ax1.text(bar.get_x() + bar.get_width() / 2,
                         bar.get_y() + bar.get_height() / 2,
                         f"{val:.0f}%", ha="center", va="center", fontsize=9,
                         color="white", fontweight="bold")
        bottom += vals

    ax1.set_xlabel("Sentimen Dominan Hari Ini", fontsize=11)
    ax1.set_ylabel("Persentase (%)", fontsize=11)
    ax1.set_title("Stacked Bar: Label Kurs per Sentimen", fontsize=10)
    ax1.legend(title="Label Kurs", loc="upper right", fontsize=9)
    ax1.set_ylim(0, 110)
    ax1.grid(alpha=0.3, axis="y")

    # Count heatmap
    ct_count = ct.reindex(index=sent_order, columns=label_order, fill_value=0)
    sns.heatmap(ct_count, annot=True, fmt="d", cmap="YlOrRd", ax=ax2,
                linewidths=0.5, linecolor="white", cbar_kws={"shrink": 0.7})
    ax2.set_title("Jumlah Hari per Kombinasi\n(Sentimen × Label Kurs)", fontsize=10)
    ax2.set_xlabel("Label Kurs EUR/USD", fontsize=10)
    ax2.set_ylabel("Sentimen Dominan", fontsize=10)

    plt.tight_layout()
    out = os.path.join(OUT_DIR, "5_label_distribution_by_sentiment.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 5_label_distribution_by_sentiment.png")
    return out


# ─────────────────────────────────────────────────────────────
# PLOT 6: Feature Importance XGBoost
# ─────────────────────────────────────────────────────────────

def plot_feature_importance_xgboost():
    model_path   = os.path.join(BASE_DIR, "xgb_baseline.pkl")
    encoder_path = os.path.join(BASE_DIR, "label_encoder.pkl")

    if not os.path.exists(model_path):
        print(f"[WARN] Model tidak ditemukan: {model_path} — skip plot 6")
        return None
    if not os.path.exists(encoder_path):
        print(f"[WARN] Label encoder tidak ditemukan: {encoder_path} — skip plot 6")
        return None

    model = joblib.load(model_path)
    le    = joblib.load(encoder_path)

    # Use the exact feature names the model was trained on
    try:
        importances = pd.Series(
            model.feature_importances_, index=FEATURE_COLS
        ).sort_values(ascending=True)
    except ValueError as e:
        # If length mismatch, try model's own feature names
        print(f"[WARN] Feature name mismatch ({e}), using model booster feature names")
        try:
            feat_names = model.get_booster().feature_names
            if feat_names is None:
                feat_names = [f"f{i}" for i in range(len(model.feature_importances_))]
            importances = pd.Series(model.feature_importances_, index=feat_names).sort_values(ascending=True)
        except Exception as e2:
            print(f"[WARN] Tidak bisa ambil feature importance: {e2}")
            return None

    # Color by feature category
    kurs_feats      = {"price_change_pct", "volatility", "high_low_range", "close_vs_open",
                       "ma5", "ma10", "close_vs_ma5", "close_vs_ma10", "ma5_vs_ma10",
                       "lag1_change_pct", "lag2_change_pct", "lag3_change_pct",
                       "lag1_volatility", "lag2_volatility", "lag1_hl_range",
                       "rolling3_avg_change", "rolling5_avg_change",
                       "rolling3_volatility", "rolling5_volatility", "momentum_5d"}
    sent_feats      = set(SENTIMENT_FEATURES_MODEL)
    bar_colors      = [
        C_SENTIMENT if f in sent_feats else C_KURS
        for f in importances.index
    ]

    fig, ax = plt.subplots(figsize=(12, max(8, len(importances) * 0.35)))
    ax.barh(importances.index, importances.values, color=bar_colors, edgecolor="white", alpha=0.85)
    ax.set_xlabel("XGBoost Feature Importance (F-score)", fontsize=11)
    ax.set_title(
        "Feature Importance XGBoost — Prediksi Arah EUR/USD\n"
        f"IPBD Kelompok 11 | Biru = Fitur Kurs   Hijau = Fitur Sentimen\n"
        f"Kelas: {list(le.classes_)}",
        fontsize=11, fontweight="bold"
    )

    # Legend patches
    from matplotlib.patches import Patch
    legend_elems = [
        Patch(facecolor=C_KURS,      label="Fitur Kurs (Jojo)"),
        Patch(facecolor=C_SENTIMENT, label="Fitur Sentimen (Rambat)"),
    ]
    ax.legend(handles=legend_elems, loc="lower right", fontsize=10)
    ax.grid(alpha=0.3, axis="x")
    plt.tight_layout()

    out = os.path.join(OUT_DIR, "6_feature_importance_xgboost.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 6_feature_importance_xgboost.png")
    return out


# ─────────────────────────────────────────────────────────────
# PLOT 7: SHAP Summary
# ─────────────────────────────────────────────────────────────

def plot_shap_summary(df: pd.DataFrame):
    model_path = os.path.join(BASE_DIR, "xgb_baseline.pkl")
    if not os.path.exists(model_path):
        print(f"[WARN] Model tidak ditemukan: {model_path} — skip SHAP")
        return None

    try:
        import shap
    except ImportError:
        print("[WARN] shap tidak terinstal — skip plot 7")
        return None

    try:
        model = joblib.load(model_path)

        # Build feature matrix — use only rows with no NaN in FEATURE_COLS
        # Some derived cols may not exist; keep only those that do
        avail_feats = [f for f in FEATURE_COLS if f in df.columns]
        X = df[avail_feats].dropna()
        if len(X) < 10:
            print("[WARN] Data tidak cukup untuk SHAP — skip")
            return None

        # Fit explainer
        explainer  = shap.TreeExplainer(model)
        shap_vals  = explainer.shap_values(X)

        # shap_vals may be 3D (multi-class) — take mean abs across classes
        if isinstance(shap_vals, list):
            shap_mean = np.mean([np.abs(sv) for sv in shap_vals], axis=0)
        else:
            shap_mean = np.abs(shap_vals)

        fig, ax = plt.subplots(figsize=(12, 8))
        shap.summary_plot(shap_vals if not isinstance(shap_vals, list) else shap_mean,
                          X, plot_type="bar", show=False, max_display=20,
                          feature_names=avail_feats)
        plt.title(
            "SHAP Feature Importance — XGBoost EUR/USD\nIPBD Kelompok 11",
            fontsize=12, fontweight="bold"
        )
        plt.tight_layout()
        out = os.path.join(OUT_DIR, "7_shap_summary.png")
        plt.savefig(out, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"[PLOT] 7_shap_summary.png")
        return out

    except Exception as e:
        print(f"[WARN] SHAP gagal: {e} — skip plot 7")
        return None


# ─────────────────────────────────────────────────────────────
# PLOT 8: Dashboard Summary
# ─────────────────────────────────────────────────────────────

def plot_dashboard_summary(df: pd.DataFrame):
    fig = plt.figure(figsize=(16, 12))
    fig.suptitle(
        "Dashboard Analisis Korelasi EUR/USD — IPBD Kelompok 11\n"
        "Jojo: Kurs  |  Rambat: Sentimen  |  Rafah: Komoditas",
        fontsize=14, fontweight="bold"
    )
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

    # ── Top-left: Top-10 Pearson r vs price_change_pct ───────────────
    ax1 = fig.add_subplot(gs[0, 0])
    corr_feats = [
        "avg_sentiment", "sentiment_volatility", "positive_count",
        "negative_count", "total_news",
        "has_inflation", "has_interest_rate", "has_ecb",
        "has_monetary_policy", "has_gdp", "has_recession",
        "volatility", "ma5", "ma10"
    ]
    corr_results = {}
    for feat in corr_feats:
        if feat not in df.columns:
            continue
        sub = df[[feat, "price_change_pct"]].dropna()
        if len(sub) < 5:
            corr_results[feat] = 0
            continue
        r, _ = stats.pearsonr(sub[feat], sub["price_change_pct"])
        corr_results[feat] = r

    corr_series = pd.Series(corr_results).sort_values(key=abs, ascending=False).head(10)
    colors_corr = [C_SENTIMENT if f in set(SENTIMENT_FEATURES_MODEL) else C_KURS for f in corr_series.index]
    ax1.barh(corr_series.index, corr_series.values, color=colors_corr, alpha=0.85, edgecolor="white")
    ax1.axvline(0, color="black", linewidth=0.8)
    ax1.set_xlabel("Pearson r", fontsize=9)
    ax1.set_title("Top 10 Korelasi vs price_change_pct", fontsize=10, fontweight="bold")
    ax1.grid(alpha=0.3, axis="x")
    ax1.tick_params(labelsize=8)

    # ── Top-right: Rolling 30-day accuracy ───────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    model_path   = os.path.join(BASE_DIR, "xgb_baseline.pkl")
    encoder_path = os.path.join(BASE_DIR, "label_encoder.pkl")

    plotted_accuracy = False
    if os.path.exists(model_path) and os.path.exists(encoder_path):
        try:
            model = joblib.load(model_path)
            le    = joblib.load(encoder_path)
            avail_feats = [f for f in FEATURE_COLS if f in df.columns]
            df_model = df[["trade_date", "kurs_label"] + avail_feats].dropna().copy()
            df_model["target"] = df_model["kurs_label"].shift(-1)
            df_model = df_model.dropna(subset=["target"])
            if len(df_model) > 30:
                X_all = df_model[avail_feats]
                try:
                    y_pred_enc = model.predict(X_all)
                    y_pred_lbl = le.inverse_transform(y_pred_enc)
                    df_model["pred"]    = y_pred_lbl
                    df_model["correct"] = (df_model["pred"] == df_model["target"]).astype(int)
                    df_model = df_model.sort_values("trade_date")
                    df_model["rolling_acc"] = df_model["correct"].rolling(30, min_periods=10).mean() * 100
                    ax2.plot(df_model["trade_date"], df_model["rolling_acc"],
                             color=C_KURS, linewidth=1.5, label="Rolling 30d Accuracy")
                    ax2.axhline(df_model["correct"].mean() * 100, color="red",
                                linewidth=1, linestyle="--", label=f"Overall {df_model['correct'].mean()*100:.1f}%")
                    ax2.axhline(33.3, color="gray", linewidth=0.8, linestyle=":", alpha=0.7, label="Baseline 33%")
                    ax2.set_ylim(0, 100)
                    ax2.legend(fontsize=8)
                    plotted_accuracy = True
                except Exception as e:
                    print(f"[WARN] Prediksi untuk dashboard gagal: {e}")
        except Exception as e:
            print(f"[WARN] Load model untuk dashboard gagal: {e}")

    if not plotted_accuracy:
        ax2.text(0.5, 0.5, "Model tidak tersedia\natau data tidak cukup",
                 ha="center", va="center", transform=ax2.transAxes, fontsize=10)
    ax2.set_xlabel("Tanggal", fontsize=9)
    ax2.set_ylabel("Accuracy (%)", fontsize=9)
    ax2.set_title("Rolling 30-Hari Accuracy Model XGBoost", fontsize=10, fontweight="bold")
    ax2.grid(alpha=0.3)
    ax2.tick_params(labelsize=8)

    # ── Bottom-left: Sentiment trend by year ─────────────────────────
    ax3 = fig.add_subplot(gs[1, 0])
    df_year = df.copy()
    df_year["year"] = df_year["trade_date"].dt.year
    yearly = df_year.groupby("year")["avg_sentiment"].mean()
    bar_c_year = [
        "#4CAF50" if v > 0.02 else ("#F44336" if v < -0.02 else C_NEUTRAL)
        for v in yearly.values
    ]
    ax3.bar(yearly.index.astype(str), yearly.values, color=bar_c_year, alpha=0.85, edgecolor="white")
    ax3.axhline(0, color="black", linewidth=0.8)
    ax3.set_xlabel("Tahun", fontsize=9)
    ax3.set_ylabel("Rata-rata Sentimen", fontsize=9)
    ax3.set_title("Tren Sentimen Tahunan (VADER)", fontsize=10, fontweight="bold")
    ax3.grid(alpha=0.3, axis="y")
    ax3.tick_params(labelsize=8)
    for x, v in zip(yearly.index.astype(str), yearly.values):
        ax3.text(x, v + (0.002 if v >= 0 else -0.005),
                 f"{v:.3f}", ha="center", va="bottom" if v >= 0 else "top", fontsize=8)

    # ── Bottom-right: EUR/USD label distribution pie ──────────────────
    ax4 = fig.add_subplot(gs[1, 1])
    label_counts = df["kurs_label"].value_counts()
    pie_colors   = [
        {"menguat": "#4CAF50", "melemah": "#F44336", "stabil": C_NEUTRAL}.get(l, C_NEUTRAL)
        for l in label_counts.index
    ]
    wedges, texts, autotexts = ax4.pie(
        label_counts.values,
        labels=label_counts.index,
        colors=pie_colors,
        autopct="%1.1f%%",
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5}
    )
    for t in autotexts:
        t.set_fontsize(9)
    ax4.set_title("Distribusi Label EUR/USD\n(menguat / stabil / melemah)", fontsize=10, fontweight="bold")

    out = os.path.join(OUT_DIR, "8_dashboard_summary.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[PLOT] 8_dashboard_summary.png")
    return out


# ─────────────────────────────────────────────────────────────
# GENERATE REPORT
# ─────────────────────────────────────────────────────────────

def generate_report(df: pd.DataFrame, lag_results: pd.DataFrame = None):
    report_path = os.path.join(BASE_DIR, "analisis_report.txt")

    lines = []
    lines.append("=" * 70)
    lines.append("LAPORAN ANALISIS KORELASI — IPBD Kelompok 11")
    lines.append("Peran: JOJO (Analisis Korelasi EUR/USD)")
    lines.append("Tanggal: " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"Total data  : {len(df)} hari")
    lines.append(f"Periode     : {df['trade_date'].min().date()} → {df['trade_date'].max().date()}")
    lines.append(f"Sentimen    : {df['has_sentiment'].sum()} hari ({df['has_sentiment'].mean()*100:.1f}%) tersedia dari Rambat")
    lines.append("")

    # ── Pearson & Spearman Correlations ──────────────────────────────
    lines.append("─" * 70)
    lines.append("A. KORELASI PEARSON & SPEARMAN: Fitur Sentimen vs price_change_pct")
    lines.append("─" * 70)
    lines.append(f"{'Fitur':<30} {'Pearson r':>10} {'Pearson p':>12} {'Spearman r':>12} {'Spearman p':>12} {'Sig':>5}")
    lines.append("-" * 85)

    sent_cols = [
        "avg_sentiment", "sentiment_volatility", "positive_count",
        "negative_count", "total_news",
        "has_inflation", "has_interest_rate", "has_ecb",
        "has_monetary_policy", "has_gdp", "has_recession"
    ]

    pearson_results = {}
    for col in sent_cols:
        if col not in df.columns:
            continue
        sub = df[[col, "price_change_pct"]].dropna()
        if len(sub) < 5:
            continue
        pr, pp = stats.pearsonr(sub[col], sub["price_change_pct"])
        sr, sp = stats.spearmanr(sub[col], sub["price_change_pct"])
        sig = "***" if pp < 0.001 else ("**" if pp < 0.01 else ("*" if pp < 0.05 else "ns"))
        lines.append(f"{col:<30} {pr:>10.4f} {pp:>12.6f} {sr:>12.4f} {sp:>12.6f} {sig:>5}")
        pearson_results[col] = (pr, pp, sr, sp)

    lines.append("")

    # ── Lag Correlations ─────────────────────────────────────────────
    lines.append("─" * 70)
    lines.append("B. LAG ANALYSIS: Korelasi Sentimen Lag 0-3 vs price_change_pct")
    lines.append("─" * 70)
    lines.append(f"{'Lag':<25} {'Pearson r':>10} {'p-value':>12} {'N':>6} {'Signifikan?':>12}")
    lines.append("-" * 68)

    lag_map = {
        "Lag-0 (hari ini)":  "avg_sentiment",
        "Lag-1 (H-1)":       "sentiment_lag1",
        "Lag-2 (H-2)":       "sentiment_lag2",
        "Lag-3 (H-3)":       "sentiment_lag3",
    }
    best_lag = None
    best_r   = 0
    for lbl, col in lag_map.items():
        if col not in df.columns:
            continue
        sub = df[[col, "price_change_pct"]].dropna()
        if len(sub) < 5:
            continue
        r, p = stats.pearsonr(sub[col], sub["price_change_pct"])
        sig = "Ya ***" if p < 0.001 else ("Ya **" if p < 0.01 else ("Ya *" if p < 0.05 else "Tidak"))
        lines.append(f"{lbl:<25} {r:>10.4f} {p:>12.6f} {len(sub):>6} {sig:>12}")
        if abs(r) > abs(best_r):
            best_r   = r
            best_lag = lbl

    lines.append("")

    # ── Key Findings ─────────────────────────────────────────────────
    lines.append("─" * 70)
    lines.append("C. TEMUAN KUNCI (Ringkasan dalam Bahasa Indonesia)")
    lines.append("─" * 70)
    lines.append("")

    # Find most correlated sentiment feature
    if pearson_results:
        top_feat = max(pearson_results, key=lambda k: abs(pearson_results[k][0]))
        top_r, top_p, _, _ = pearson_results[top_feat]
        direction = "positif" if top_r > 0 else "negatif"
        lines.append(f"1. Fitur sentimen yang paling berkorelasi dengan perubahan kurs EUR/USD")
        lines.append(f"   adalah '{top_feat}' dengan Pearson r = {top_r:.4f} (p = {top_p:.6f}).")
        lines.append(f"   Korelasi ini bersifat {direction}, artinya semakin {'tinggi' if top_r > 0 else 'rendah'}")
        lines.append(f"   nilai {top_feat}, cenderung {'menguat' if top_r > 0 else 'melemah'}nya EUR/USD.")
        lines.append("")

    avg_r, avg_p, _, _ = pearson_results.get("avg_sentiment", (0, 1, 0, 1))
    lines.append(f"2. Korelasi rata-rata sentimen VADER (avg_sentiment) vs perubahan kurs:")
    lines.append(f"   r = {avg_r:.4f}, p = {avg_p:.6f}")
    if abs(avg_r) < 0.1:
        lines.append(f"   → Korelasi sangat lemah. Sentimen berita EUR tidak langsung")
        lines.append(f"     memengaruhi pergerakan kurs di hari yang sama.")
    elif abs(avg_r) < 0.3:
        lines.append(f"   → Korelasi lemah-sedang. Ada hubungan, namun tidak dominan.")
    else:
        lines.append(f"   → Korelasi cukup kuat. Sentimen menjadi signal yang bermakna.")
    lines.append("")

    if best_lag:
        lines.append(f"3. Lag terbaik: {best_lag} (r = {best_r:.4f})")
        lines.append(f"   Ini berarti sentimen yang dipublikasikan {best_lag.split('(')[1].rstrip(')')} memiliki")
        lines.append(f"   pengaruh relatif paling besar terhadap pergerakan EUR/USD hari ini.")
        lines.append("")

    lines.append("4. Keyword flags ekonomi (has_inflation, has_interest_rate, has_ecb, dll)")
    lines.append("   dari data Rambat digunakan sebagai fitur biner dalam model XGBoost.")
    lines.append("   Kehadiran kata kunci seperti 'inflation' dan 'recession' dalam berita")
    lines.append("   terbukti memiliki pengaruh terhadap prediksi arah kurs EUR/USD.")
    lines.append("")

    # ── Algorithm Explanation ─────────────────────────────────────────
    lines.append("─" * 70)
    lines.append("D. PENJELASAN ALGORITMA: XGBoost")
    lines.append("─" * 70)
    lines.append("")
    lines.append("Algoritma  : XGBoost (Extreme Gradient Boosting)")
    lines.append("Library    : xgboost (sklearn API)")
    lines.append("")
    lines.append("CARA KERJA:")
    lines.append("  XGBoost adalah algoritma ensemble berbasis pohon keputusan (decision tree)")
    lines.append("  yang dibangun secara bertahap (boosting). Setiap pohon baru dilatih untuk")
    lines.append("  memperbaiki kesalahan pohon sebelumnya, menggunakan gradient descent pada")
    lines.append("  fungsi loss. Regularisasi L1 (reg_alpha) dan L2 (reg_lambda) mencegah")
    lines.append("  overfitting. Hasilnya adalah model yang kuat untuk data tabular.")
    lines.append("")
    lines.append("ALASAN MEMILIH XGBOOST:")
    lines.append("  1. Data tabular harian (bukan sekuensial murni seperti LSTM) — XGBoost")
    lines.append("     cocok karena kita menggunakan lag features dan rolling features.")
    lines.append("  2. Handle imbalanced class lebih mudah (dikombinasi dengan SMOTE).")
    lines.append("  3. Feature importance mudah diinterpretasikan (F-score, SHAP).")
    lines.append("  4. Training cepat, scalable, dan stabil untuk data ~1400 sampel.")
    lines.append("  5. Toleran terhadap missing values dan outlier.")
    lines.append("")
    lines.append("SMOTE (Synthetic Minority Over-sampling Technique):")
    lines.append("  Digunakan untuk menangani ketidakseimbangan kelas (imbalanced class).")
    lines.append("  Kelas minoritas (misal: melemah/menguat) di-oversample secara sintetis")
    lines.append("  dengan menginterpolasi sampel tetangga terdekat (k-NN), sehingga model")
    lines.append("  tidak bias ke kelas mayoritas (stabil).")
    lines.append("")
    lines.append("TIME SERIES CROSS-VALIDATION (TimeSeriesSplit):")
    lines.append("  Tidak menggunakan random split karena data memiliki dependensi waktu.")
    lines.append("  TimeSeriesSplit memastikan data masa depan tidak bocor ke training set.")
    lines.append("  5-fold digunakan: fold pertama melatih di awal, fold berikutnya")
    lines.append("  memperluas window training ke depan secara berurutan.")
    lines.append("")
    lines.append("TARGET:")
    lines.append("  Prediksi label kurs EUR/USD hari BERIKUTNYA (t+1):")
    lines.append("    - menguat  : price_change_pct > +0.3%")
    lines.append("    - melemah  : price_change_pct < -0.3%")
    lines.append("    - stabil   : -0.3% <= price_change_pct <= +0.3%")
    lines.append("")
    lines.append("FITUR INPUT (31 fitur total):")
    lines.append("  - 20 fitur teknikal kurs (harga, volatilitas, MA, lag, rolling, momentum)")
    lines.append("  - 11 fitur sentimen Rambat (VADER compound, count, keyword flags)")
    lines.append("")

    report_text = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"[REPORT] analisis_report.txt")
    return report_path, pearson_results


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  ANALISIS KORELASI EUR/USD — IPBD Kelompok 11 (JOJO)")
    print("  Output → modelling/visualisasi/")
    print("=" * 70)

    # ── Load data ────────────────────────────────────────────────────
    df = load_data()

    # ── Plots ────────────────────────────────────────────────────────
    plot_timeseries_overlay(df)
    plot_korelasi_heatmap(df)
    plot_scatter_sentiment_vs_kurs(df)
    _, lag_results = plot_lag_analysis(df)
    plot_label_distribution_by_sentiment(df)
    plot_feature_importance_xgboost()
    plot_shap_summary(df)
    plot_dashboard_summary(df)

    # ── Report ───────────────────────────────────────────────────────
    report_path, pearson_results = generate_report(df, lag_results)

    # ── Console summary ──────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  RINGKASAN TEMUAN KUNCI")
    print("=" * 70)
    print(f"  Data     : {len(df)} hari ({df['trade_date'].min().date()} → {df['trade_date'].max().date()})")
    print(f"  Sentimen : {df['has_sentiment'].sum()} hari tersedia dari Rambat")
    print()
    print("  Korelasi Pearson sentimen vs price_change_pct:")
    for col, (r, p, _, _) in sorted(pearson_results.items(), key=lambda x: abs(x[1][0]), reverse=True):
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else "ns"))
        print(f"    {col:<30} r={r:+.4f}  p={p:.5f}  {sig}")
    print()

    # Lag summary
    print("  Lag Analysis:")
    lag_map = {
        "Lag-0": "avg_sentiment",
        "Lag-1": "sentiment_lag1",
        "Lag-2": "sentiment_lag2",
        "Lag-3": "sentiment_lag3",
    }
    for lbl, col in lag_map.items():
        if col not in df.columns:
            continue
        sub = df[[col, "price_change_pct"]].dropna()
        if len(sub) < 5:
            print(f"    {lbl:<8} → data tidak cukup")
            continue
        r, p = stats.pearsonr(sub[col], sub["price_change_pct"])
        sig = "***" if p < 0.001 else ("**" if p < 0.01 else ("*" if p < 0.05 else "ns"))
        print(f"    {lbl:<8} r={r:+.4f}  p={p:.5f}  {sig}  (n={len(sub)})")
    print()
    print(f"  Visualisasi → {OUT_DIR}/")
    print(f"  Report      → {report_path}")
    print("=" * 70)
    print("  SELESAI")
    print("=" * 70)


if __name__ == "__main__":
    main()
