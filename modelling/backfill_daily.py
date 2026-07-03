"""
backfill_daily.py — IPBD Kelompok 11 (JOJO)
Backfill data harian REAL EUR/USD dari yfinance ke kurs_daily.

Mengganti data dummy dengan data historis real 2021–sekarang.
Jalankan SEKALI sebelum training model offline.
"""

import psycopg2
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin"
}

SYMBOLS = {
    "EURUSD=X": "EUR/USD",
    "EURIDR=X": "EUR/IDR",
}

STABLE_THRESHOLD = 0.05  # % — sama dengan spark_stream.py


def compute_label(pct: float) -> str:
    if abs(pct) < STABLE_THRESHOLD:
        return "stabil"
    return "menguat" if pct > 0 else "melemah"


def backfill(symbol: str, start: str = "2021-01-01"):
    print(f"\n[INFO] Download data harian {symbol} dari {start} ...")
    df = yf.download(symbol, start=start, auto_adjust=True, progress=False)

    if df.empty:
        print(f"[WARN] Tidak ada data untuk {symbol}")
        return 0

    # Flatten MultiIndex kolom kalau ada
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"date": "trade_date"})

    # Untuk FX (EURUSD=X), open = close dari yfinance (adjusted)
    # Gunakan close-to-close return sebagai price_change_pct
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["price_change"]     = df["close"].diff()
    df["price_change_pct"] = (df["close"].pct_change() * 100).round(4)
    df["open_price_adj"]   = df["close"].shift(1)  # open = close kemarin

    # Drop baris pertama (NaN dari diff)
    df = df.dropna(subset=["price_change_pct"])

    df["label"]    = df["price_change_pct"].apply(compute_label)
    df["avg_price"] = df["close"].round(6)
    df["volatility"] = ((df["high"] - df["low"]) / df["close"].shift(1)).round(6)
    df = df.dropna(subset=["volatility"])

    # MA5 dan MA10
    df["ma5"]  = df["close"].rolling(5).mean().round(6)
    df["ma10"] = df["close"].rolling(10).mean().round(6)
    df = df.dropna(subset=["ma5", "ma10"])

    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor()

    # Hapus data dummy (harga terlalu berbeda dari real) dan insert ulang
    cur.execute("DELETE FROM kurs_daily WHERE symbol = %s", (symbol,))
    conn.commit()
    print(f"[INFO] Data lama {symbol} dihapus.")

    inserted = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO kurs_daily (
                    trade_date, symbol,
                    open_price, high_price, low_price, close_price,
                    avg_price, volatility,
                    price_change, price_change_pct,
                    ma5, ma10, tick_count, label
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
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
                row["trade_date"].date(), symbol,
                float(row.get("open_price_adj", row["open"])),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["avg_price"]),
                float(row["volatility"]),
                float(row["price_change"]),
                float(row["price_change_pct"]),
                float(row["ma5"]),
                float(row["ma10"]),
                0,
                row["label"]
            ))
            inserted += 1
        except Exception as e:
            print(f"[WARN] Skip {row['trade_date']}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"[OK]  {symbol}: {inserted} hari data real berhasil diinsert.")
    return inserted


def verify():
    """Cek hasil backfill."""
    conn = psycopg2.connect(**PG_CONFIG)
    df   = pd.read_sql("""
        SELECT
            symbol,
            COUNT(*) as total_hari,
            MIN(trade_date) as dari,
            MAX(trade_date) as sampai,
            ROUND(AVG(close_price)::numeric, 5) as avg_close,
            COUNT(CASE WHEN label='menguat' THEN 1 END) as menguat,
            COUNT(CASE WHEN label='melemah' THEN 1 END) as melemah,
            COUNT(CASE WHEN label='stabil'  THEN 1 END) as stabil
        FROM kurs_daily
        GROUP BY symbol
        ORDER BY symbol
    """, conn)
    conn.close()

    print("\n" + "="*65)
    print("  HASIL BACKFILL kurs_daily")
    print("="*65)
    print(df.to_string(index=False))


if __name__ == "__main__":
    print("="*55)
    print("  BACKFILL DATA HARIAN REAL — yfinance")
    print("  Mengganti data dummy dengan data historis 2021–2026")
    print("="*55)

    total = 0
    for symbol in SYMBOLS:
        total += backfill(symbol, start="2021-01-01")

    print(f"\n[DONE] Total: {total} baris data real dimasukkan ke kurs_daily.")
    verify()
    print("\n✅ Sekarang jalankan: python3 modelling/model_offline.py")
