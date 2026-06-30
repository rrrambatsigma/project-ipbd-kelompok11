"""
test_stream_data.py — IPBD Kelompok 11 (JOJO)
Cek dan verifikasi data hasil streaming di semua layer PostgreSQL.
"""

import psycopg2
import pandas as pd
from datetime import datetime

PG_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin"
}

def run_query(conn, sql, title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print('='*55)
    df = pd.read_sql(sql, conn)
    if df.empty:
        print("  (kosong)")
    else:
        print(df.to_string(index=False))
    return df

def main():
    conn = psycopg2.connect(**PG_CONFIG)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Koneksi ke PostgreSQL berhasil.\n")

    # ── 1. Ringkasan semua tabel ──────────────────────────────────────
    run_query(conn, """
        SELECT
            'kurs_raw'    AS tabel, COUNT(*) AS total_baris FROM kurs_raw
        UNION ALL SELECT
            'kurs_silver', COUNT(*) FROM kurs_silver
        UNION ALL SELECT
            'kurs_daily',  COUNT(*) FROM kurs_daily
        UNION ALL SELECT
            'commodity_daily', COUNT(*) FROM commodity_daily
        UNION ALL SELECT
            'sentiment_daily', COUNT(*) FROM sentiment_daily
    """, "RINGKASAN SEMUA TABEL")

    # ── 2. Bronze — 10 tick terbaru ───────────────────────────────────
    run_query(conn, """
        SELECT symbol, price, event_time, source, ingested_at
        FROM kurs_raw
        ORDER BY ingested_at DESC
        LIMIT 10
    """, "BRONZE: 10 TICK TERBARU (kurs_raw)")

    # ── 3. Silver — window terbaru per simbol ─────────────────────────
    run_query(conn, """
        SELECT
            symbol, window_start, window_end,
            ROUND(open_price::numeric, 5) AS open,
            ROUND(close_price::numeric, 5) AS close,
            ROUND(price_change_pct::numeric, 4) AS chg_pct,
            ROUND(volatility::numeric, 6) AS vol,
            tick_count, label
        FROM kurs_silver
        ORDER BY window_start DESC
        LIMIT 10
    """, "SILVER: 10 WINDOW TERBARU (kurs_silver)")

    # ── 4. Gold — data harian terbaru ─────────────────────────────────
    run_query(conn, """
        SELECT
            trade_date, symbol,
            ROUND(open_price::numeric, 5)  AS open,
            ROUND(close_price::numeric, 5) AS close,
            ROUND(price_change_pct::numeric, 4) AS chg_pct,
            ROUND(ma5::numeric, 5)  AS ma5,
            ROUND(ma10::numeric, 5) AS ma10,
            tick_count, label
        FROM kurs_daily
        ORDER BY trade_date DESC
        LIMIT 10
    """, "GOLD: 10 HARI TERBARU (kurs_daily)")

    # ── 5. Distribusi label ───────────────────────────────────────────
    run_query(conn, """
        SELECT
            label,
            COUNT(*) AS jumlah,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS persen
        FROM kurs_daily
        WHERE label IS NOT NULL
        GROUP BY label
        ORDER BY jumlah DESC
    """, "DISTRIBUSI LABEL (kurs_daily)")

    # ── 6. Distribusi per simbol ──────────────────────────────────────
    run_query(conn, """
        SELECT symbol, COUNT(*) AS tick_count,
               ROUND(AVG(price)::numeric, 5) AS avg_price,
               ROUND(MIN(price)::numeric, 5) AS min_price,
               ROUND(MAX(price)::numeric, 5) AS max_price
        FROM kurs_raw
        GROUP BY symbol
        ORDER BY tick_count DESC
    """, "RINGKASAN PER SIMBOL (kurs_raw)")

    # ── 7. v_market_signals — preview ─────────────────────────────────
    run_query(conn, """
        SELECT
            trade_date,
            ROUND(kurs_close::numeric, 5) AS kurs_close,
            ROUND(kurs_change_pct::numeric, 4) AS kurs_chg,
            kurs_label,
            wti_close, gold_close,
            avg_sentiment, total_news
        FROM v_market_signals
        LIMIT 10
    """, "VIEW v_market_signals (preview modelling bersama)")

    conn.close()
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Test selesai.")

if __name__ == "__main__":
    main()
