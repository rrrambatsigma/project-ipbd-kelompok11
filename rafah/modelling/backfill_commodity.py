"""
rafah/modelling/backfill_commodity.py — IPBD Kelompok 11 (RAFAH)
Backfill data harian historis 2021–2026 untuk GLD, BTC-USD, SI=F
dari yfinance ke tabel commodity_daily PostgreSQL.

Jalankan SEKALI sebelum training model offline.
"""

import psycopg2
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import date

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin",
}

# Mapping ticker → nama komoditas
TICKERS = {
    "GLD":     "Gold",
    "BTC-USD": "Bitcoin",
    "SI=F":    "Silver",
}

STABLE_THRESHOLD = 0.5  # %


def compute_label(pct: float) -> str:
    if abs(pct) < STABLE_THRESHOLD:
        return "stabil"
    return "naik" if pct > 0 else "turun"


def backfill(symbol: str, commodity: str, start: str = "2021-01-01"):
    print(f"\n[INFO] Download {commodity} ({symbol}) dari {start} ...")
    df = yf.download(symbol, start=start, auto_adjust=True, progress=False)

    if df.empty:
        print(f"[WARN] Tidak ada data untuk {symbol}")
        return 0

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"date": "trade_date"})
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Close-to-close return
    df["price_change"]     = df["close"].diff()
    df["price_change_pct"] = (df["close"].pct_change() * 100).round(4)
    df["open_price_adj"]   = df["close"].shift(1)
    df = df.dropna(subset=["price_change_pct"])

    df["label"]     = df["price_change_pct"].apply(compute_label)
    df["avg_price"] = df["close"].round(4)
    df["volatility"] = ((df["high"] - df["low"]) / df["close"].shift(1)).round(6)
    df = df.dropna(subset=["volatility"])

    df["ma5"]  = df["close"].rolling(5).mean().round(4)
    df["ma10"] = df["close"].rolling(10).mean().round(4)
    df = df.dropna(subset=["ma5", "ma10"])

    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor()

    # Hapus data lama untuk simbol ini
    cur.execute("DELETE FROM commodity_daily WHERE symbol = %s", (symbol,))
    conn.commit()

    inserted = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO commodity_daily (
                    trade_date, symbol, commodity,
                    open_price, high_price, low_price, close_price,
                    avg_price, volatility, price_change, price_change_pct,
                    ma5, ma10, tick_count, label
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (trade_date, symbol) DO UPDATE SET
                    open_price       = EXCLUDED.open_price,
                    high_price       = EXCLUDED.high_price,
                    low_price        = EXCLUDED.low_price,
                    close_price      = EXCLUDED.close_price,
                    avg_price        = EXCLUDED.avg_price,
                    volatility       = EXCLUDED.volatility,
                    price_change     = EXCLUDED.price_change,
                    price_change_pct = EXCLUDED.price_change_pct,
                    ma5              = EXCLUDED.ma5,
                    ma10             = EXCLUDED.ma10,
                    label            = EXCLUDED.label,
                    updated_at       = NOW()
            """, (
                row["trade_date"].date(), symbol, commodity,
                float(row.get("open_price_adj", row["close"])),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["avg_price"]),
                float(row["volatility"]),
                float(row["price_change"]),
                float(row["price_change_pct"]),
                float(row["ma5"]),
                float(row["ma10"]),
                0, row["label"],
            ))
            inserted += 1
        except Exception as e:
            conn.rollback()
            print(f"  [WARN] Skip {row['trade_date']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"[OK]  {commodity} ({symbol}): {inserted} hari berhasil diinsert.")
    return inserted


def verify():
    conn = psycopg2.connect(**PG_CONFIG)
    df = pd.read_sql("""
        SELECT
            commodity, symbol,
            COUNT(*)                                    AS total_hari,
            MIN(trade_date)                             AS dari,
            MAX(trade_date)                             AS sampai,
            ROUND(AVG(close_price)::numeric, 4)         AS avg_close,
            COUNT(CASE WHEN label='naik'   THEN 1 END)  AS naik,
            COUNT(CASE WHEN label='turun'  THEN 1 END)  AS turun,
            COUNT(CASE WHEN label='stabil' THEN 1 END)  AS stabil
        FROM commodity_daily
        GROUP BY commodity, symbol
        ORDER BY commodity
    """, conn)
    conn.close()

    print("\n" + "=" * 65)
    print("  HASIL BACKFILL commodity_daily")
    print("=" * 65)
    print(df.to_string(index=False))


if __name__ == "__main__":
    print("=" * 55)
    print("  BACKFILL KOMODITAS HISTORIS — yfinance 2021-2026")
    print("  GLD (Gold) | BTC-USD (Bitcoin) | SI=F (Silver)")
    print("=" * 55)

    total = 0
    for symbol, commodity in TICKERS.items():
        total += backfill(symbol, commodity, start="2021-01-01")

    print(f"\n[DONE] Total: {total} baris dimasukkan ke commodity_daily.")
    verify()
    print("\n✅ Sekarang jalankan: python3 rafah/modelling/model_commodity.py")
