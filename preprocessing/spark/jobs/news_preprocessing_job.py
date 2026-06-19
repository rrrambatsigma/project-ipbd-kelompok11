"""
jobs/news_preprocessing_job.py
─────────────────────────────────────────────────────────
Apache Spark Preprocessing Job — Euro News Pipeline
Kelompok 11 IPBD

Alur:
  1. EXTRACT     — baca raw JSON dari MinIO (semua source), paksa RAW_SCHEMA
  2. CLEAN       — hapus duplikat, null, HTML, karakter aneh
  3. LANG_FILTER — deteksi bahasa (fastText) dari raw_text, keep hanya English
  4. NORMALIZE   — standarisasi tanggal, teks, kolom
  5. AGGREGATE   — gabungkan semua source jadi 1 dataset, dedup lintas-source
  6. LOAD        — simpan ke MinIO sebagai Parquet

Input  : s3a://news-raw/{source}/{year}/{date}/*.json
Output : s3a://news-processed/articles/year={YYYY}/month={MM}/
─────────────────────────────────────────────────────────
"""

import os
import sys
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minio_utils import create_spark_session, ensure_bucket, list_source_paths
from lang_filter import filter_english
from schema import RAW_SCHEMA

# ── Konfigurasi ───────────────────────────────────────────
BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "news-raw")
BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED", "news-processed")
OUTPUT_PATH = f"s3a://{BUCKET_PROCESSED}/articles"

SOURCES = ["ecb", "guardian", "gdelt", "newsapi"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


# ═══════════════════════════════════════════
# TELEGRAM ALERT
# ═══════════════════════════════════════════

def send_telegram(message: str) -> None:
    """Kirim notifikasi Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"Telegram gagal: {e}")


# ═══════════════════════════════════════════
# STEP 1 — EXTRACT
# ═══════════════════════════════════════════

def extract(spark: SparkSession, source: str) -> DataFrame | None:
    """
    Baca semua raw JSON dari MinIO untuk satu source.
    Dipaksa pakai RAW_SCHEMA supaya kolom konsisten antar source
    (mis. kolom `tone` cuma ada di GDELT, `author` cuma di NewsAPI —
    dengan schema eksplisit, kolom yang gak ada otomatis jadi null,
    bukan bikin job gagal/skewed).
    """
    paths = list_source_paths(BUCKET_RAW, source)

    if not paths:
        logger.warning(f"[EXTRACT] {source} — tidak ada file JSON ditemukan")
        return None

    logger.info(f"[EXTRACT] {source} — membaca {len(paths)} file...")

    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .json(paths)
    )

    if "source" not in df.columns or df.filter(F.col("source").isNull()).count() == df.count():
        df = df.withColumn("source", F.lit(source))

    count = df.count()
    logger.success(f"[EXTRACT] {source} — {count} baris dibaca")
    return df


# ═══════════════════════════════════════════
# STEP 2 — CLEAN
# ═══════════════════════════════════════════

def clean(df: DataFrame, source: str) -> DataFrame:
    """
    Cleaning:
    - Hapus baris dengan title/url/published_at null
    - Hapus duplikat berdasarkan URL
    - Bersihkan HTML tags dari raw_text
    - Trim whitespace
    - Filter minimum panjang teks
    """
    logger.info(f"[CLEAN] {source} — mulai cleaning...")
    before = df.count()

    df = df.filter(
        F.col("title").isNotNull() &
        F.col("url").isNotNull() &
        F.col("published_at").isNotNull()
    )

    df = df.filter(
        (F.length(F.trim(F.col("title"))) > 0) &
        (F.length(F.trim(F.col("url"))) > 0)
    )

    df = df.dropDuplicates(["url"])

    df = df.withColumn(
        "raw_text",
        F.regexp_replace(F.col("raw_text"), "<[^>]+>", " ")
    )
    df = df.withColumn(
        "raw_text",
        F.regexp_replace(F.col("raw_text"), r"\s+", " ")
    )
    df = df.withColumn(
        "raw_text",
        F.regexp_replace(F.col("raw_text"), r"[^\x20-\x7E\u00C0-\u024F]", "")
    )

    df = df.withColumn("title", F.trim(F.col("title")))
    df = df.withColumn("raw_text", F.trim(F.col("raw_text")))

    df = df.filter(F.length(F.col("raw_text")) >= 20)

    after = df.count()
    logger.success(
        f"[CLEAN] {source} — selesai: {before} → {after} baris "
        f"(dihapus: {before - after})"
    )
    return df


# ═══════════════════════════════════════════
# STEP 3 — NORMALIZE
# ═══════════════════════════════════════════

def normalize(df: DataFrame, source: str) -> DataFrame:
    """
    Normalisasi:
    - Parse published_at ke Timestamp
    - Ekstrak year, month, day
    - Generate article_id (MD5 hash dari URL)
    - Standardisasi nama kolom
    - Isi nilai null dengan default (language pakai hasil deteksi fastText,
      BUKAN diasumsikan "en" begitu saja)
    """
    logger.info(f"[NORMALIZE] {source} — mulai normalisasi...")

    df = df.withColumn(
        "published_at_ts",
        F.coalesce(
            F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("published_at"), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col("published_at"), "yyyy-MM-dd"),
        )
    )

    df = df.filter(F.col("published_at_ts").isNotNull())

    df = (
        df
        .withColumn("year", F.year(F.col("published_at_ts")))
        .withColumn("month", F.month(F.col("published_at_ts")))
        .withColumn("day", F.dayofmonth(F.col("published_at_ts")))
    )

    df = df.filter(F.col("year").between(2021, 2026))

    df = df.withColumn("article_id", F.md5(F.col("url")))

    df = df.withColumn(
        "ingested_at_ts",
        F.coalesce(
            F.to_timestamp(F.col("_ingested_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            F.to_timestamp(F.col("_ingested_at"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.current_timestamp(),
        )
    )

    df = (
        df
        # language: prioritaskan hasil deteksi fastText (akurat),
        # fallback ke field mentah, fallback terakhir baru "en"
        .withColumn("language", F.coalesce(F.col("detected_language"), F.col("language"), F.lit("en")))
        .withColumn("source_tier", F.coalesce(F.col("source_tier"), F.lit(0)))
        .withColumn("category", F.coalesce(F.col("category"), F.lit("general")))
        .withColumn("provider", F.coalesce(F.col("provider"), F.lit("")))
        .withColumn("tone", F.coalesce(F.col("tone"), F.lit(None).cast("float")))
        .withColumn("is_backfill", F.coalesce(F.col("_backfill"), F.lit(False)))
    )

    df = df.select(
        F.col("article_id"),
        F.col("title"),
        F.col("raw_text").alias("clean_text"),
        F.col("published_at_ts").alias("published_at"),
        F.col("year"),
        F.col("month"),
        F.col("day"),
        F.col("source"),
        F.col("source_tier"),
        F.col("category"),
        F.col("language"),
        F.col("provider"),
        F.col("tone"),
        F.col("url"),
        F.col("is_backfill"),
        F.col("ingested_at_ts").alias("ingested_at"),
    )

    count = df.count()
    logger.success(f"[NORMALIZE] {source} — {count} baris setelah normalisasi")
    return df


# ═══════════════════════════════════════════
# STEP 4 — AGGREGATE
# ═══════════════════════════════════════════

def aggregate(dfs: list[DataFrame]) -> DataFrame:
    """
    Gabungkan semua source menjadi 1 DataFrame.
    Hapus duplikat lintas source berdasarkan article_id.
    """
    logger.info(f"[AGGREGATE] Menggabungkan {len(dfs)} source...")

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    before = combined.count()
    combined = combined.dropDuplicates(["article_id"])
    after = combined.count()

    logger.success(
        f"[AGGREGATE] Selesai: {before} → {after} artikel unik "
        f"(duplikat dihapus: {before - after})"
    )

    logger.info("[AGGREGATE] Distribusi per source:")
    combined.groupBy("source").count().orderBy("source").show()

    logger.info("[AGGREGATE] Distribusi per tahun:")
    combined.groupBy("year").count().orderBy("year").show()

    return combined


# ═══════════════════════════════════════════
# STEP 5 — LOAD
# ═══════════════════════════════════════════

def load(df: DataFrame) -> None:
    """
    Simpan hasil preprocessing ke MinIO sebagai Parquet.
    Partisi per year dan month untuk efisiensi query.
    """
    logger.info(f"[LOAD] Menyimpan ke {OUTPUT_PATH}...")

    total = df.count()

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUTPUT_PATH)
    )

    logger.success(f"[LOAD] Selesai — {total} artikel disimpan ke {OUTPUT_PATH}")
    logger.info(f"[LOAD] Struktur: {OUTPUT_PATH}/year=YYYY/month=MM/")


# ═══════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════

def run_preprocessing():
    """Jalankan full preprocessing pipeline."""
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 55)
    logger.info("  EURO NEWS PREPROCESSING — Apache Spark")
    logger.info(f"  Start: {start_time.isoformat()}")
    logger.info("=" * 55)

    send_telegram(
        "⚙️ <b>Preprocessing Dimulai</b>\n"
        f"{'─' * 30}\n"
        f"⏰ Start: {start_time.isoformat()[:19]} UTC\n"
        f"📦 Source: {', '.join(SOURCES)}"
    )

    spark = create_spark_session("EuroNewsPreprocessing")
    ensure_bucket(BUCKET_PROCESSED)

    processed_dfs = []
    stats = {}

    for source in SOURCES:
        logger.info(f"\n{'─'*40}")
        logger.info(f"Memproses source: {source.upper()}")
        logger.info(f"{'─'*40}")

        try:
            # 1. Extract
            raw_df = extract(spark, source)
            if raw_df is None:
                logger.warning(f"[{source}] Skip — tidak ada data")
                continue

            # 2. Clean
            clean_df = clean(raw_df, source)

            # 3. Language filter — dilakukan SEBELUM normalize,
            #    biar artikel non-English gak ikut diproses lebih jauh
            lang_df = filter_english(clean_df, text_col="raw_text")

            # 4. Normalize
            norm_df = normalize(lang_df, source)

            processed_dfs.append(norm_df)
            stats[source] = norm_df.count()

        except Exception as e:
            logger.error(f"[{source}] GAGAL: {e}")
            stats[source] = 0
            continue

    if not processed_dfs:
        logger.error("Tidak ada data yang berhasil diproses!")
        send_telegram("🚨 <b>Preprocessing GAGAL</b> — tidak ada data berhasil diproses!")
        spark.stop()
        return

    # 5. Aggregate
    logger.info("\n" + "─" * 40)
    final_df = aggregate(processed_dfs)

    # 6. Load
    logger.info("\n" + "─" * 40)
    load(final_df)

    # ── Summary ───────────────────────────────
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).seconds
    total_rows = final_df.count()

    source_lines = ""
    for src, cnt in stats.items():
        icon = "✅" if cnt > 0 else "⚠️"
        source_lines += f"\n  {icon} <b>{src.upper()}</b>: {cnt} artikel"

    logger.info("\n" + "=" * 55)
    logger.info("  PREPROCESSING SELESAI")
    logger.info(f"  Total artikel bersih : {total_rows}")
    logger.info(f"  Durasi               : {duration} detik")
    logger.info(f"  Output               : {OUTPUT_PATH}")
    logger.info("=" * 55)

    send_telegram(
        f"✅ <b>Preprocessing Selesai!</b>\n"
        f"{'─' * 30}\n"
        f"⏰ durasi        : {duration} detik\n"
        f"💾 total artikel : {total_rows}\n"
        f"📂 output        : news-processed/articles/\n"
        f"{'─' * 30}\n"
        f"📊 per source    :{source_lines}\n"
        f"{'─' * 30}\n"
        f"✅ Data siap untuk modelling sentimen!"
    )

    spark.stop()


# ═══════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Euro News Spark Preprocessing")
    parser.add_argument(
        "--source",
        default="all",
        help="Source yang diproses: all | ecb | guardian | gdelt | newsapi"
    )
    args = parser.parse_args()

    if args.source != "all":
        SOURCES = [args.source]

    run_preprocessing()