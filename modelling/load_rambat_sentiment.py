"""
load_rambat_sentiment.py — IPBD Kelompok 11 (JOJO)
Load data sentimen Rambat dari API (model classifier terbaru, bukan VADER).

API Rambat: http://100.118.244.91:8000/api/sentiment/daily
Kolom yang tersedia:
  - tanggal       : date (format YYYY-MM-DD)
  - total_artikel : jumlah artikel per hari
  - negatif       : jumlah artikel negatif
  - positif       : jumlah artikel positif
  - avg_neg_prob  : rata-rata probabilitas negatif (dari model LN/classifier)
  - avg_pos_prob  : rata-rata probabilitas positif (dari model LN/classifier)

Mapping ke sentiment_daily:
  - avg_sentiment  = avg_pos_prob - avg_neg_prob  (net sentiment score, -1 s/d +1)
  - positive_count = positif
  - negative_count = negatif
  - neutral_count  = total_artikel - positif - negatif
  - total_news     = total_artikel
  - positive_ratio = positif / total_artikel
  - negative_ratio = negatif / total_artikel
  - dominant_sentiment = positif / negatif / netral berdasarkan jumlah terbanyak
"""

import requests
import pandas as pd
import numpy as np
import psycopg2

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin"
}

RAMBAT_API = "http://100.118.244.91:8000/api/sentiment/daily"


def fetch_from_api() -> pd.DataFrame:
    """Fetch data sentimen harian dari API Rambat."""
    print(f"[INFO] Fetching data dari {RAMBAT_API} ...")
    resp = requests.get(RAMBAT_API, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    df   = pd.DataFrame(data)
    print(f"[INFO] Data diterima: {len(df)} hari")
    print(f"[INFO] Kolom: {list(df.columns)}")
    print(f"[INFO] Sample:\n{df.head(3).to_string()}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data dari format API Rambat ke format sentiment_daily."""
    df = df.copy()

    # Parse tanggal
    df["trade_date"] = pd.to_datetime(df["tanggal"]).dt.date

    # avg_sentiment = net score: pos_prob - neg_prob  → range -1 s/d +1
    df["avg_sentiment"]        = (df["avg_pos_prob"] - df["avg_neg_prob"]).round(6)
    df["sentiment_volatility"] = 0.0   # tidak tersedia dari API ini

    # Counts
    df["positive_count"] = df["positif"].astype(int)
    df["negative_count"] = df["negatif"].astype(int)
    df["total_news"]     = df["total_artikel"].astype(int)
    df["neutral_count"]  = (df["total_news"] - df["positive_count"] - df["negative_count"]).clip(lower=0)

    # Dominant sentiment
    def dominant(row):
        if row["positive_count"] >= row["negative_count"] and row["positive_count"] >= row["neutral_count"]:
            return "positif"
        elif row["negative_count"] >= row["neutral_count"]:
            return "negatif"
        return "netral"
    df["dominant_sentiment"] = df.apply(dominant, axis=1)

    # Keyword flags — tidak tersedia dari API, default 0
    for kw in ["has_inflation", "has_interest_rate", "has_ecb",
               "has_monetary_policy", "has_gdp", "has_recession",
               "has_growth", "has_trade", "has_forex", "has_currency"]:
        df[kw] = 0.0

    print(f"\n[INFO] Setelah transform:")
    print(f"  Total hari       : {len(df):,}")
    print(f"  Periode          : {df['trade_date'].min()} → {df['trade_date'].max()}")
    print(f"  Total artikel    : {df['total_news'].sum():,}")
    print(f"  Avg per hari     : {df['total_news'].mean():.1f}")
    print(f"  Avg sentiment    : {df['avg_sentiment'].mean():.6f}")

    dom = df["dominant_sentiment"].value_counts()
    print(f"\n  Distribusi dominant_sentiment:")
    for lbl, cnt in dom.items():
        print(f"    {lbl:8s}: {cnt} ({cnt/len(df)*100:.1f}%)")

    return df


def ensure_schema(conn):
    cur = conn.cursor()
    new_cols = [
        ("has_growth",   "FLOAT DEFAULT 0"),
        ("has_trade",    "FLOAT DEFAULT 0"),
        ("has_forex",    "FLOAT DEFAULT 0"),
        ("has_currency", "FLOAT DEFAULT 0"),
    ]
    for col_name, col_def in new_cols:
        try:
            cur.execute(f"ALTER TABLE sentiment_daily ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[WARN] Schema {col_name}: {e}")
    cur.close()


def save_to_postgres(daily: pd.DataFrame):
    print(f"\n[INFO] Menyimpan {len(daily):,} hari ke PostgreSQL...")

    conn = psycopg2.connect(**PG_CONFIG)
    ensure_schema(conn)
    cur = conn.cursor()

    # Hapus data lama
    cur.execute("DELETE FROM sentiment_daily")
    conn.commit()

    inserted = 0
    for _, row in daily.iterrows():
        cur.execute("""
            INSERT INTO sentiment_daily (
                trade_date,
                avg_sentiment, sentiment_volatility,
                positive_count, negative_count, neutral_count,
                total_news, dominant_sentiment,
                has_inflation, has_interest_rate, has_ecb,
                has_monetary_policy, has_gdp, has_recession,
                has_growth, has_trade, has_forex, has_currency
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (trade_date) DO UPDATE SET
                avg_sentiment        = EXCLUDED.avg_sentiment,
                sentiment_volatility = EXCLUDED.sentiment_volatility,
                positive_count       = EXCLUDED.positive_count,
                negative_count       = EXCLUDED.negative_count,
                neutral_count        = EXCLUDED.neutral_count,
                total_news           = EXCLUDED.total_news,
                dominant_sentiment   = EXCLUDED.dominant_sentiment,
                updated_at           = NOW()
        """, (
            row["trade_date"],
            float(row["avg_sentiment"]),
            float(row["sentiment_volatility"]),
            int(row["positive_count"]),
            int(row["negative_count"]),
            int(row["neutral_count"]),
            int(row["total_news"]),
            row["dominant_sentiment"],
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[OK] Inserted: {inserted} hari ke sentiment_daily")


if __name__ == "__main__":
    print("=" * 55)
    print("  LOAD SENTIMEN RAMBAT — dari API (model classifier)")
    print(f"  Source: {RAMBAT_API}")
    print("=" * 55)

    df_raw   = fetch_from_api()
    df_daily = transform(df_raw)
    save_to_postgres(df_daily)

    print(f"\n✅ Selesai! Sekarang jalankan:")
    print(f"   python3 modelling/model_offline.py")
