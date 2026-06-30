"""
spark_stream.py — IPBD Kelompok 11 (JOJO)
Pipeline: Kafka → Python Consumer → Preprocessing → PostgreSQL

Layer:
  Bronze  → kurs_raw       : tick mentah yang sudah divalidasi
  Silver  → kurs_silver    : aggregasi per window 1 menit + fitur
  Gold    → kurs_daily     : ringkasan harian + label arah + moving average
                             (tabel ini dipakai bersama Rafah & Rambat untuk modelling)

Tahapan preprocessing:
  1. Validasi schema  (symbol, price, event_time, source)
  2. Konversi timestamp (ms → datetime WIB)
  3. Cleaning nilai kurs (hapus harga 0 / null / tidak wajar)
  4. Deduplication    (symbol + event_time + source)
  5. Window aggregation (tumbling 1 menit)
  6. Feature engineering (price_change, price_change_pct, volatility)
  7. Labeling (menguat / melemah / stabil)
  8. Gold aggregation harian (diupdate setiap flush)
"""

import json
import time
import hashlib
import statistics
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch

print("=" * 60)
print("  STREAM PROCESSOR: KURS EUR — IPBD Kelompok 11")
print("=" * 60)

# ── Konfigurasi ───────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:29092"
TOPIC          = "kurs_eur_stream"
PG_CONFIG      = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin"
}
WINDOW_SECONDS = 60    # window aggregasi 1 menit
FLUSH_INTERVAL = 30    # kirim ke DB setiap 30 detik

# Threshold labeling perubahan kurs harian
# Jika |price_change_pct| < STABLE_THRESHOLD → stabil
STABLE_THRESHOLD = 0.05   # 0.05%

# ── Koneksi PostgreSQL ────────────────────────────────────────────────────
def connect_postgres():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = False
        print("[INFO] Berhasil terhubung ke PostgreSQL.")
        return conn
    except Exception as e:
        print(f"[ERROR] Gagal koneksi PostgreSQL: {e}")
        raise


def init_tables(conn):
    """Buat semua tabel (Bronze, Silver, Gold) jika belum ada."""
    with conn.cursor() as cur:

        # ── Bronze: tick mentah yang sudah valid ──────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kurs_raw (
                id          SERIAL PRIMARY KEY,
                symbol      VARCHAR(20)      NOT NULL,
                price       DOUBLE PRECISION NOT NULL,
                event_time  TIMESTAMP        NOT NULL,
                source      VARCHAR(50),
                ingested_at TIMESTAMP        DEFAULT NOW()
            );
        """)

        # ── Silver: aggregasi per window 1 menit ─────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kurs_silver (
                id               SERIAL PRIMARY KEY,
                symbol           VARCHAR(20)      NOT NULL,
                window_start     TIMESTAMP        NOT NULL,
                window_end       TIMESTAMP        NOT NULL,
                open_price       DOUBLE PRECISION,
                close_price      DOUBLE PRECISION,
                avg_price        DOUBLE PRECISION,
                volatility       DOUBLE PRECISION,
                tick_count       INTEGER,
                price_change     DOUBLE PRECISION,
                price_change_pct DOUBLE PRECISION,
                label            VARCHAR(10),
                source           VARCHAR(50),
                created_at       TIMESTAMP        DEFAULT NOW()
            );
        """)

        # ── Gold: ringkasan harian — dipakai bersama untuk modelling ──
        # Tabel ini di-UPSERT setiap flush sehingga selalu up-to-date
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kurs_daily (
                id               SERIAL PRIMARY KEY,
                trade_date       DATE             NOT NULL,
                symbol           VARCHAR(20)      NOT NULL,
                open_price       DOUBLE PRECISION,
                high_price       DOUBLE PRECISION,
                low_price        DOUBLE PRECISION,
                close_price      DOUBLE PRECISION,
                avg_price        DOUBLE PRECISION,
                volatility       DOUBLE PRECISION,
                price_change     DOUBLE PRECISION,
                price_change_pct DOUBLE PRECISION,
                ma5              DOUBLE PRECISION, -- moving average 5 hari
                ma10             DOUBLE PRECISION, -- moving average 10 hari
                tick_count       INTEGER,
                label            VARCHAR(10),      -- menguat / melemah / stabil
                updated_at       TIMESTAMP        DEFAULT NOW(),
                UNIQUE (trade_date, symbol)
            );
        """)

        # ── View untuk modelling bersama (Jojo + Rafah + Rambat) ─────
        # View ini menggabungkan kurs_daily dengan tabel komoditas & sentimen
        # yang dibuat oleh Rafah dan Rambat di database yang sama.
        # Tabel commodity_daily dan sentiment_daily dibuat oleh mereka.
        cur.execute("""
            CREATE OR REPLACE VIEW v_market_signals AS
            SELECT
                k.trade_date,
                k.symbol                          AS kurs_symbol,
                k.open_price                      AS kurs_open,
                k.close_price                     AS kurs_close,
                k.price_change_pct                AS kurs_change_pct,
                k.volatility                      AS kurs_volatility,
                k.ma5                             AS kurs_ma5,
                k.ma10                            AS kurs_ma10,
                k.label                           AS kurs_label,
                -- kolom dari Rafah (commodity_daily) — NULL jika belum ada
                cd.wti_close,
                cd.brent_close,
                cd.gold_close,
                cd.natgas_close,
                cd.copper_close,
                -- kolom dari Rambat (sentiment_daily) — NULL jika belum ada
                sd.avg_sentiment,
                sd.positive_count,
                sd.negative_count,
                sd.total_news,
                sd.sentiment_volatility
            FROM kurs_daily k
            LEFT JOIN commodity_daily  cd ON cd.trade_date  = k.trade_date
            LEFT JOIN sentiment_daily  sd ON sd.trade_date  = k.trade_date
            ORDER BY k.trade_date DESC;
        """)

        conn.commit()
    print("[INFO] Tabel kurs_raw, kurs_silver, kurs_daily, dan view v_market_signals siap.")


# ── Preprocessing helpers ─────────────────────────────────────────────────

def validate_and_clean(msg: dict) -> dict | None:
    """
    Tahap 1-3: Validasi schema, konversi timestamp, cleaning harga.
    Return None jika data tidak valid.
    """
    required = ["symbol", "price", "event_time", "source"]
    if not all(k in msg and msg[k] is not None for k in required):
        return None

    try:
        price = float(msg["price"])
    except (TypeError, ValueError):
        return None

    if price <= 0 or price >= 1_000_000:
        return None

    try:
        event_time = datetime.fromtimestamp(
            int(msg["event_time"]) / 1000,
            tz=timezone(timedelta(hours=7))  # WIB
        ).replace(tzinfo=None)
    except Exception:
        return None

    return {
        "symbol":     str(msg["symbol"]),
        "price":      price,
        "event_time": event_time,
        "source":     str(msg["source"]),
    }


def make_dedup_key(record: dict) -> str:
    """Tahap 4: Hash unik untuk deduplication."""
    raw = f"{record['symbol']}_{int(record['price']*1e6)}_{record['event_time'].isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()


def get_window_key(event_time: datetime, window_sec: int) -> datetime:
    """Hitung window start (tumbling window)."""
    ts = int(event_time.timestamp())
    return datetime.fromtimestamp((ts // window_sec) * window_sec)


def compute_label(price_change_pct: float) -> str:
    """
    Tahap 7: Labeling arah pergerakan kurs.
    menguat  = EUR/USD naik (euro menguat terhadap dollar)
    melemah  = EUR/USD turun
    stabil   = perubahan di bawah threshold
    """
    if abs(price_change_pct) < STABLE_THRESHOLD:
        return "stabil"
    return "menguat" if price_change_pct > 0 else "melemah"


def compute_silver(window_start: datetime, symbol: str, ticks: list) -> dict:
    """Tahap 5-6-7: Window aggregation + feature engineering + label."""
    window_end  = datetime.fromtimestamp(window_start.timestamp() + WINDOW_SECONDS)
    open_p      = ticks[0]
    close_p     = ticks[-1]
    avg_p       = round(sum(ticks) / len(ticks), 6)
    vol         = round(statistics.stdev(ticks), 6) if len(ticks) > 1 else 0.0
    change      = round(close_p - open_p, 6)
    change_pct  = round((close_p - open_p) / open_p * 100, 4) if open_p else 0.0
    label       = compute_label(change_pct)

    return {
        "symbol":           symbol,
        "window_start":     window_start,
        "window_end":       window_end,
        "open_price":       open_p,
        "close_price":      close_p,
        "avg_price":        avg_p,
        "volatility":       vol,
        "tick_count":       len(ticks),
        "price_change":     change,
        "price_change_pct": change_pct,
        "label":            label,
        "source":           "Yahoo Finance",
    }


# ── Tulis ke PostgreSQL ───────────────────────────────────────────────────

def flush_bronze(conn, records: list):
    if not records:
        return
    sql = """
        INSERT INTO kurs_raw (symbol, price, event_time, source)
        VALUES (%(symbol)s, %(price)s, %(event_time)s, %(source)s)
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, records)
    conn.commit()
    print(f"[Bronze] {len(records)} baris → kurs_raw")


def flush_silver(conn, windows: dict) -> list:
    """
    Tulis Silver layer. Return list of silver rows untuk Gold aggregation.
    """
    if not windows:
        return []

    rows = []
    for (symbol, window_start), prices in windows.items():
        row = compute_silver(window_start, symbol, prices)
        rows.append(row)

    sql = """
        INSERT INTO kurs_silver (
            symbol, window_start, window_end,
            open_price, close_price, avg_price, volatility, tick_count,
            price_change, price_change_pct, label, source
        ) VALUES (
            %(symbol)s, %(window_start)s, %(window_end)s,
            %(open_price)s, %(close_price)s, %(avg_price)s,
            %(volatility)s, %(tick_count)s,
            %(price_change)s, %(price_change_pct)s, %(label)s, %(source)s
        )
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows)
    conn.commit()

    print(f"[Silver] {len(rows)} window → kurs_silver")
    for r in rows:
        print(f"  → {r['symbol']:12s} | {r['window_start'].strftime('%H:%M')} "
              f"| open={r['open_price']:.5f} close={r['close_price']:.5f} "
              f"| Δ={r['price_change']:+.5f} ({r['price_change_pct']:+.4f}%) "
              f"| vol={r['volatility']:.5f} | label={r['label']}")

    return rows


def flush_gold(conn, silver_rows: list):
    """
    Tahap 8: Gold layer — ringkasan harian per simbol.
    UPSERT ke kurs_daily berdasarkan (trade_date, symbol).
    Hitung juga MA5 dan MA10 dari data historis di DB.
    """
    if not silver_rows:
        return

    # Kelompokkan silver rows per (symbol, tanggal)
    daily = defaultdict(list)
    for r in silver_rows:
        trade_date = r["window_start"].date()
        daily[(r["symbol"], trade_date)].append(r)

    with conn.cursor() as cur:
        for (symbol, trade_date), rows in daily.items():
            all_prices = []
            for r in rows:
                all_prices += [r["open_price"], r["close_price"]]

            open_p   = rows[0]["open_price"]
            close_p  = rows[-1]["close_price"]
            high_p   = max(r["close_price"] for r in rows)
            low_p    = min(r["close_price"] for r in rows)
            avg_p    = round(sum(all_prices) / len(all_prices), 6)
            vol      = round(statistics.stdev(all_prices), 6) if len(all_prices) > 1 else 0.0
            change   = round(close_p - open_p, 6)
            chg_pct  = round((close_p - open_p) / open_p * 100, 4) if open_p else 0.0
            label    = compute_label(chg_pct)
            ticks    = sum(r["tick_count"] for r in rows)

            # Hitung MA5 dan MA10 dari data historis
            cur.execute("""
                SELECT close_price FROM kurs_daily
                WHERE symbol = %s AND trade_date < %s
                ORDER BY trade_date DESC
                LIMIT 10
            """, (symbol, trade_date))
            hist = [row[0] for row in cur.fetchall()]

            prices_for_ma = [close_p] + hist
            ma5  = round(sum(prices_for_ma[:5])  / min(5,  len(prices_for_ma)), 6)
            ma10 = round(sum(prices_for_ma[:10]) / min(10, len(prices_for_ma)), 6)

            # UPSERT — update kalau sudah ada, insert kalau belum
            cur.execute("""
                INSERT INTO kurs_daily (
                    trade_date, symbol,
                    open_price, high_price, low_price, close_price,
                    avg_price, volatility,
                    price_change, price_change_pct,
                    ma5, ma10, tick_count, label, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (trade_date, symbol) DO UPDATE SET
                    close_price      = EXCLUDED.close_price,
                    high_price       = GREATEST(kurs_daily.high_price, EXCLUDED.high_price),
                    low_price        = LEAST(kurs_daily.low_price,  EXCLUDED.low_price),
                    avg_price        = EXCLUDED.avg_price,
                    volatility       = EXCLUDED.volatility,
                    price_change     = EXCLUDED.price_change,
                    price_change_pct = EXCLUDED.price_change_pct,
                    ma5              = EXCLUDED.ma5,
                    ma10             = EXCLUDED.ma10,
                    tick_count       = kurs_daily.tick_count + EXCLUDED.tick_count,
                    label            = EXCLUDED.label,
                    updated_at       = NOW()
            """, (
                trade_date, symbol,
                open_p, high_p, low_p, close_p,
                avg_p, vol,
                change, chg_pct,
                ma5, ma10, ticks, label
            ))

    conn.commit()
    print(f"[Gold]   {len(daily)} entri harian → kurs_daily (MA5, MA10, label)")


# ── Main Loop ─────────────────────────────────────────────────────────────

def main():
    conn = connect_postgres()
    init_tables(conn)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="kurs-eur-processor"
    )

    print(f"\n[INFO] Mendengarkan topik: {TOPIC}")
    print(f"[INFO] Bronze → kurs_raw | Silver → kurs_silver | Gold → kurs_daily")
    print(f"[INFO] Tekan Ctrl+C untuk berhenti.\n")

    bronze_buffer  = []
    silver_windows = defaultdict(list)
    seen_dedup     = set()
    last_flush     = time.time()

    try:
        for message in consumer:
            raw = message.value

            record = validate_and_clean(raw)
            if record is None:
                continue

            key = make_dedup_key(record)
            if key in seen_dedup:
                continue
            seen_dedup.add(key)

            bronze_buffer.append(record)

            window_start = get_window_key(record["event_time"], WINDOW_SECONDS)
            silver_windows[(record["symbol"], window_start)].append(record["price"])

            ts = record["event_time"].strftime("%H:%M:%S")
            print(f"[{ts}] {record['symbol']:12s} | {record['price']:.5f}")

            now = time.time()
            if now - last_flush >= FLUSH_INTERVAL:
                flush_bronze(conn, bronze_buffer)
                silver_rows = flush_silver(conn, silver_windows)
                flush_gold(conn, silver_rows)

                bronze_buffer  = []
                silver_windows = defaultdict(list)
                seen_dedup     = set()
                last_flush     = now

    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan. Flushing data terakhir...")
        flush_bronze(conn, bronze_buffer)
        silver_rows = flush_silver(conn, silver_windows)
        flush_gold(conn, silver_rows)
    finally:
        consumer.close()
        conn.close()
        print("[INFO] Selesai.")


if __name__ == "__main__":
    main()
