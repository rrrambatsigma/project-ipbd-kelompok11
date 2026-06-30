"""
rafah/modelling/test_commodity_data.py — IPBD Kelompok 11 (RAFAH)
Verifikasi data komoditas di PostgreSQL: commodity_daily.

Jalankan dari root direktori:
    python3 rafah/modelling/test_commodity_data.py
"""

import psycopg2
import pandas as pd
from datetime import datetime

PG_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin",
}

print(f"[{datetime.now().strftime('%H:%M:%S')}] Menghubungkan ke PostgreSQL...")
conn = psycopg2.connect(**PG_CONFIG)
print(f"[{datetime.now().strftime('%H:%M:%S')}] Berhasil terhubung.\n")

# ── 1. Summary table ───────────────────────────────────────────────────────
print("=" * 70)
print("  COMMODITY DAILY — RINGKASAN PER SIMBOL")
print("=" * 70)
df = pd.read_sql("""
    SELECT symbol, commodity, COUNT(*) as rows,
           MIN(trade_date) as dari, MAX(trade_date) as sampai,
           ROUND(AVG(close_price)::numeric,4) as avg_close,
           COUNT(CASE WHEN label='naik' THEN 1 END) as naik,
           COUNT(CASE WHEN label='turun' THEN 1 END) as turun,
           COUNT(CASE WHEN label='stabil' THEN 1 END) as stabil
    FROM commodity_daily GROUP BY symbol, commodity ORDER BY commodity
""", conn)
print("COMMODITY DAILY:")
print(df.to_string(index=False))

# ── 2. Latest data per symbol ──────────────────────────────────────────────
print("\n" + "=" * 70)
print("  DATA TERBARU PER SIMBOL")
print("=" * 70)
df2 = pd.read_sql("""
    SELECT symbol, commodity, trade_date,
           ROUND(close_price::numeric,4) as close,
           ROUND(price_change_pct::numeric,4) as chg_pct, label
    FROM commodity_daily
    WHERE trade_date = (SELECT MAX(trade_date) FROM commodity_daily)
    ORDER BY commodity
""", conn)
print("\nDATA TERBARU:")
print(df2.to_string(index=False))

# ── 3. Cek kelengkapan fitur modelling ────────────────────────────────────
print("\n" + "=" * 70)
print("  CEK KOLOM FITUR MODELLING")
print("=" * 70)
df3 = pd.read_sql("""
    SELECT
        symbol,
        COUNT(*) as total,
        COUNT(open_price) as has_open,
        COUNT(high_price) as has_high,
        COUNT(low_price) as has_low,
        COUNT(close_price) as has_close,
        COUNT(volatility) as has_volatility,
        COUNT(price_change_pct) as has_chg_pct,
        COUNT(ma5) as has_ma5,
        COUNT(ma10) as has_ma10
    FROM commodity_daily
    GROUP BY symbol ORDER BY symbol
""", conn)
print(df3.to_string(index=False))

# ── 4. Distribusi label per simbol ────────────────────────────────────────
print("\n" + "=" * 70)
print("  DISTRIBUSI LABEL PER SIMBOL")
print("=" * 70)
df4 = pd.read_sql("""
    SELECT symbol, label, COUNT(*) as jumlah,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY symbol), 2) as persen
    FROM commodity_daily
    WHERE label IS NOT NULL
    GROUP BY symbol, label
    ORDER BY symbol, label
""", conn)
print(df4.to_string(index=False))

conn.close()
print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Test selesai ✅")
