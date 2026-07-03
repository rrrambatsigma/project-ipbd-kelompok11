"""
rafah/spark_commodity.py — IPBD Kelompok 11 (RAFAH)
Pipeline: Kafka (commodity_stream) → Preprocessing → PostgreSQL

Layer:
  Bronze → commodity_raw    : tick mentah yang valid
  Silver → commodity_silver : aggregasi window 1 menit + fitur
  Gold   → commodity_daily  : ringkasan harian (dipakai modelling bersama)

Ticker: GLD (Gold), BTC-USD (Bitcoin), SI=F (Silver)

Tahapan preprocessing:
  1. Validasi schema  (symbol, commodity, price, event_time, source)
  2. Konversi timestamp (ms → datetime WIB)
  3. Cleaning harga (hapus 0, negatif, tidak wajar)
  4. Deduplication (symbol + event_time)
  5. Window aggregation (tumbling 1 menit)
  6. Feature engineering (price_change, price_change_pct, volatility, ma5, ma10)
  7. Labeling (naik / turun / stabil)
  8. Gold aggregation harian
"""

import json
import time
import hashlib
import statistics
import sys
import os
import io
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch
from telegram_notifier import notify_startup, notify_error, notify_shutdown

print("=" * 60)
print("  STREAM PROCESSOR: TOP 3 COMMODITY — IPBD Kelompok 11")
print("  Ticker: GLD | BTC-USD | SI=F")
print("=" * 60)

# ── Konfigurasi ───────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:29092"
TOPIC          = "commodity_stream"
PG_CONFIG      = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin",
}
WINDOW_SECONDS   = 60
FLUSH_INTERVAL   = 30
STABLE_THRESHOLD = 0.5   # % threshold untuk komoditas (lebih volatile dari kurs)

# Batas harga wajar per ticker
PRICE_BOUNDS = {
    "GLD":     (50,    1000),    # Gold ETF
    "BTC-USD": (100,   500000),  # Bitcoin
    "SI=F":    (5,     200),     # Silver futures
}
DEFAULT_BOUNDS = (0.01, 1_000_000)

# ── Konfigurasi MinIO ─────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET     = "commodity-eur"

# Arrow schemas untuk Parquet
BRONZE_SCHEMA = pa.schema([
    pa.field("symbol",     pa.string()),
    pa.field("commodity",  pa.string()),
    pa.field("price",      pa.float64()),
    pa.field("event_time", pa.string()),
    pa.field("source",     pa.string()),
])

SILVER_SCHEMA = pa.schema([
    pa.field("symbol",           pa.string()),
    pa.field("commodity",        pa.string()),
    pa.field("window_start",     pa.string()),
    pa.field("window_end",       pa.string()),
    pa.field("open_price",       pa.float64()),
    pa.field("close_price",      pa.float64()),
    pa.field("avg_price",        pa.float64()),
    pa.field("volatility",       pa.float64()),
    pa.field("tick_count",       pa.int32()),
    pa.field("price_change",     pa.float64()),
    pa.field("price_change_pct", pa.float64()),
    pa.field("label",            pa.string()),
    pa.field("source",           pa.string()),
])


def init_minio_bucket():
    """Buat bucket commodity-eur di MinIO jika belum ada. Return s3 client."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
        if MINIO_BUCKET not in existing:
            client.create_bucket(Bucket=MINIO_BUCKET)
            print(f"[MinIO] Bucket '{MINIO_BUCKET}' dibuat.")
        else:
            print(f"[MinIO] Bucket '{MINIO_BUCKET}' sudah ada.")
        return client
    except Exception as e:
        print(f"[MinIO] ⚠️  Gagal inisialisasi bucket: {e}")
        return None


def upload_parquet(records: list, schema: pa.Schema, s3_key: str):
    """Konversi list of dict → Parquet (snappy) → upload ke MinIO. Silent fail."""
    if not records:
        return
    try:
        # Serialisasi datetime ke string agar kompatibel dengan Arrow schema
        serialized = []
        for r in records:
            row = {}
            for field in schema:
                val = r.get(field.name)
                if val is None:
                    row[field.name] = None
                elif isinstance(val, datetime):
                    row[field.name] = val.isoformat()
                elif field.type == pa.int32():
                    row[field.name] = int(val)
                else:
                    row[field.name] = val
            serialized.append(row)

        table = pa.Table.from_pylist(serialized, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"[MinIO] ✅ s3://{MINIO_BUCKET}/{s3_key} ({len(records)} rows)")
    except Exception as e:
        print(f"[MinIO] ⚠️  Gagal upload {s3_key}: {e}")


# ── Koneksi PostgreSQL ────────────────────────────────────────────────────

def connect_postgres():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = False
        print("[INFO] Berhasil terhubung ke PostgreSQL.")
        notify_startup("rafah/spark_commodity.py — Commodity Processor")
        return conn
    except Exception as e:
        notify_error("spark_commodity.py", f"Koneksi gagal: {e}")
        raise


def init_tables(conn):
    with conn.cursor() as cur:

        # Bronze
        cur.execute("""
            CREATE TABLE IF NOT EXISTS commodity_raw (
                id          SERIAL           PRIMARY KEY,
                symbol      VARCHAR(20)      NOT NULL,
                commodity   VARCHAR(30)      NOT NULL,
                price       DOUBLE PRECISION NOT NULL,
                event_time  TIMESTAMP        NOT NULL,
                source      VARCHAR(50),
                ingested_at TIMESTAMP        DEFAULT NOW()
            );
        """)

        # Silver
        cur.execute("""
            CREATE TABLE IF NOT EXISTS commodity_silver (
                id               SERIAL           PRIMARY KEY,
                symbol           VARCHAR(20)      NOT NULL,
                commodity        VARCHAR(30)      NOT NULL,
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

        # Gold layer — ringkasan harian, dipakai modelling bersama
        cur.execute("""
            CREATE TABLE IF NOT EXISTS commodity_daily (
                id               SERIAL           PRIMARY KEY,
                trade_date       DATE             NOT NULL,
                symbol           VARCHAR(20)      NOT NULL,
                commodity        VARCHAR(30)      NOT NULL,
                open_price       DOUBLE PRECISION,
                high_price       DOUBLE PRECISION,
                low_price        DOUBLE PRECISION,
                close_price      DOUBLE PRECISION,
                avg_price        DOUBLE PRECISION,
                volatility       DOUBLE PRECISION,
                price_change     DOUBLE PRECISION,
                price_change_pct DOUBLE PRECISION,
                ma5              DOUBLE PRECISION,
                ma10             DOUBLE PRECISION,
                tick_count       INTEGER,
                label            VARCHAR(10),
                updated_at       TIMESTAMP        DEFAULT NOW(),
                UNIQUE (trade_date, symbol)
            );
        """)
        conn.commit()
    print("[INFO] Tabel commodity_raw, commodity_silver, commodity_daily siap.")


# ── Preprocessing ─────────────────────────────────────────────────────────

def validate_and_clean(msg: dict):
    required = ["symbol", "commodity", "price", "event_time", "source"]
    if not all(k in msg and msg[k] is not None for k in required):
        return None

    try:
        price = float(msg["price"])
    except (TypeError, ValueError):
        return None

    lo, hi = PRICE_BOUNDS.get(msg["symbol"], DEFAULT_BOUNDS)
    if price <= lo or price >= hi:
        return None

    try:
        event_time = datetime.fromtimestamp(
            int(msg["event_time"]) / 1000,
            tz=timezone(timedelta(hours=7))
        ).replace(tzinfo=None)
    except Exception:
        return None

    return {
        "symbol":    str(msg["symbol"]),
        "commodity": str(msg["commodity"]),
        "price":     price,
        "event_time": event_time,
        "source":    str(msg["source"]),
    }


def make_dedup_key(record: dict) -> str:
    raw = f"{record['symbol']}_{int(record['price']*1e4)}_{record['event_time'].isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()


def get_window_key(event_time: datetime) -> datetime:
    ts = int(event_time.timestamp())
    return datetime.fromtimestamp((ts // WINDOW_SECONDS) * WINDOW_SECONDS)


def compute_label(pct: float) -> str:
    if abs(pct) < STABLE_THRESHOLD:
        return "stabil"
    return "naik" if pct > 0 else "turun"


def compute_silver(window_start, symbol, commodity, ticks) -> dict:
    window_end  = datetime.fromtimestamp(window_start.timestamp() + WINDOW_SECONDS)
    open_p      = ticks[0]
    close_p     = ticks[-1]
    avg_p       = round(sum(ticks) / len(ticks), 4)
    vol         = round(statistics.stdev(ticks), 4) if len(ticks) > 1 else 0.0
    change      = round(close_p - open_p, 4)
    change_pct  = round((close_p - open_p) / open_p * 100, 4) if open_p else 0.0
    label       = compute_label(change_pct)

    return {
        "symbol":           symbol,
        "commodity":        commodity,
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


# ── Flush ke PostgreSQL ───────────────────────────────────────────────────

def flush_bronze(conn, records):
    if not records:
        return
    sql = """
        INSERT INTO commodity_raw (symbol, commodity, price, event_time, source)
        VALUES (%(symbol)s, %(commodity)s, %(price)s, %(event_time)s, %(source)s)
    """
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, records)
        conn.commit()
        print(f"[Bronze] {len(records)} tick → commodity_raw")
    except Exception as e:
        conn.rollback()
        notify_error("spark_commodity Bronze", str(e))
        return

    # Upload ke MinIO Parquet
    now = datetime.now()
    s3_key = (
        f"bronze/{now.strftime('%Y/%m/%d')}/"
        f"commodity_raw_{now.strftime('%H%M%S')}.parquet"
    )
    upload_parquet(records, BRONZE_SCHEMA, s3_key)


def flush_silver(conn, windows: dict) -> list:
    if not windows:
        return []

    rows = []
    for (symbol, commodity, window_start), prices in windows.items():
        rows.append(compute_silver(window_start, symbol, commodity, prices))

    sql = """
        INSERT INTO commodity_silver (
            symbol, commodity, window_start, window_end,
            open_price, close_price, avg_price, volatility, tick_count,
            price_change, price_change_pct, label, source
        ) VALUES (
            %(symbol)s, %(commodity)s, %(window_start)s, %(window_end)s,
            %(open_price)s, %(close_price)s, %(avg_price)s, %(volatility)s,
            %(tick_count)s, %(price_change)s, %(price_change_pct)s,
            %(label)s, %(source)s
        )
    """
    try:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows)
        conn.commit()
        print(f"[Silver] {len(rows)} window → commodity_silver")
        for r in rows:
            print(f"  {r['commodity']:8s} ({r['symbol']:8s}) | "
                  f"{r['window_start'].strftime('%H:%M')} | "
                  f"close={r['close_price']:.4f} "
                  f"Δ{r['price_change_pct']:+.3f}% | {r['label']}")
    except Exception as e:
        conn.rollback()
        notify_error("spark_commodity Silver", str(e))
        return rows

    # Upload ke MinIO Parquet
    now = datetime.now()
    s3_key = (
        f"silver/{now.strftime('%Y/%m/%d')}/"
        f"commodity_silver_{now.strftime('%H%M%S')}.parquet"
    )
    upload_parquet(rows, SILVER_SCHEMA, s3_key)

    return rows


def flush_gold(conn, silver_rows: list):
    if not silver_rows:
        return

    daily = defaultdict(list)
    for r in silver_rows:
        trade_date = r["window_start"].date()
        daily[(r["symbol"], r["commodity"], trade_date)].append(r)

    with conn.cursor() as cur:
        for (symbol, commodity, trade_date), rows in daily.items():
            prices = [p for r in rows for p in [r["open_price"], r["close_price"]]]
            open_p   = rows[0]["open_price"]
            close_p  = rows[-1]["close_price"]
            high_p   = max(r["close_price"] for r in rows)
            low_p    = min(r["close_price"] for r in rows)
            avg_p    = round(sum(prices) / len(prices), 4)
            vol      = round(statistics.stdev(prices), 4) if len(prices) > 1 else 0.0
            change   = round(close_p - open_p, 4)
            chg_pct  = round((close_p - open_p) / open_p * 100, 4) if open_p else 0.0
            label    = compute_label(chg_pct)
            ticks    = sum(r["tick_count"] for r in rows)

            cur.execute("""
                SELECT close_price FROM commodity_daily
                WHERE symbol = %s AND trade_date < %s
                ORDER BY trade_date DESC LIMIT 10
            """, (symbol, trade_date))
            hist = [row[0] for row in cur.fetchall()]
            prices_ma = [close_p] + hist
            ma5  = round(sum(prices_ma[:5]) / min(5, len(prices_ma)), 4)
            ma10 = round(sum(prices_ma[:10]) / min(10, len(prices_ma)), 4)

            cur.execute("""
                INSERT INTO commodity_daily (
                    trade_date, symbol, commodity,
                    open_price, high_price, low_price, close_price,
                    avg_price, volatility, price_change, price_change_pct,
                    ma5, ma10, tick_count, label, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (trade_date, symbol) DO UPDATE SET
                    close_price      = EXCLUDED.close_price,
                    high_price       = GREATEST(commodity_daily.high_price, EXCLUDED.high_price),
                    low_price        = LEAST(commodity_daily.low_price, EXCLUDED.low_price),
                    avg_price        = EXCLUDED.avg_price,
                    volatility       = EXCLUDED.volatility,
                    price_change     = EXCLUDED.price_change,
                    price_change_pct = EXCLUDED.price_change_pct,
                    ma5              = EXCLUDED.ma5,
                    ma10             = EXCLUDED.ma10,
                    tick_count       = commodity_daily.tick_count + EXCLUDED.tick_count,
                    label            = EXCLUDED.label,
                    updated_at       = NOW()
            """, (trade_date, symbol, commodity,
                  open_p, high_p, low_p, close_p,
                  avg_p, vol, change, chg_pct,
                  ma5, ma10, ticks, label))
    conn.commit()
    print(f"[Gold]   {len(daily)} entri harian → commodity_daily")


# ── Main Loop ─────────────────────────────────────────────────────────────

def main():
    conn = connect_postgres()
    init_tables(conn)
    init_minio_bucket()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="commodity-processor",
    )

    print(f"\n[INFO] Mendengarkan topik: {TOPIC}")
    print("[INFO] Bronze → commodity_raw | Silver → commodity_silver | Gold → commodity_daily")
    print("[INFO] Tekan Ctrl+C untuk berhenti.\n")

    bronze_buffer  = []
    silver_windows = defaultdict(list)
    seen_dedup     = set()
    last_flush     = time.time()
    total_tick     = 0

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
            total_tick += 1

            window_start = get_window_key(record["event_time"])
            silver_windows[
                (record["symbol"], record["commodity"], window_start)
            ].append(record["price"])

            ts = record["event_time"].strftime("%H:%M:%S")
            print(f"[{ts}] {record['commodity']:8s} ({record['symbol']:8s}) "
                  f"| {record['price']:.4f}")

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
        notify_shutdown("spark_commodity.py", {
            "total_tick": total_tick,
            "bronze":     len(bronze_buffer),
            "silver":     len(silver_windows),
        })
    except Exception as e:
        notify_error("spark_commodity.py", str(e))
        raise
    finally:
        consumer.close()
        conn.close()
        print("[INFO] Selesai.")


if __name__ == "__main__":
    main()
