"""
jobs/schema.py
─────────────────────────────────────────────────────────
Definisi schema Spark untuk artikel berita Euro.
─────────────────────────────────────────────────────────
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, FloatType, BooleanType,
)

# ── Schema raw JSON dari MinIO ────────────────────────────
# Dipakai saat extract() supaya semua source dipaksa ke struktur
# kolom yang sama walau field-nya beda-beda per source.
RAW_SCHEMA = StructType([
    StructField("title",        StringType(),    True),
    StructField("url",          StringType(),    True),
    StructField("published_at", StringType(),    True),  # string dulu, nanti di-cast
    StructField("source",       StringType(),    True),
    StructField("source_tier",  StringType(),    True),  # string dulu — data mentah kadang "60s"
    StructField("category",     StringType(),    True),
    StructField("raw_text",     StringType(),    True),
    StructField("language",     StringType(),    True),  # mentah dari source, TIDAK reliable untuk filter
    StructField("provider",     StringType(),    True),
    StructField("tone",         StringType(),    True),  # string dulu — GDELT tone kadang aneh
    StructField("author",       StringType(),    True),  # khusus NewsAPI
    StructField("_ingested_at", StringType(),    True),
    StructField("_source_tier", StringType(),    True),
    StructField("_backfill",    BooleanType(),   True),
])

# ── Schema output setelah preprocessing ──────────────────
PROCESSED_SCHEMA = StructType([
    StructField("article_id",   StringType(),    False),  # hash unik MD5 dari URL
    StructField("title",        StringType(),    True),
    StructField("clean_text",   StringType(),    True),   # raw_text sudah dibersihkan
    StructField("published_at", TimestampType(), True),
    StructField("year",         IntegerType(),   True),
    StructField("month",        IntegerType(),   True),
    StructField("day",          IntegerType(),   True),
    StructField("source",       StringType(),    True),
    StructField("source_tier",  IntegerType(),   True),
    StructField("category",     StringType(),    True),
    StructField("language",     StringType(),    True),   # hasil deteksi fastText, bukan asumsi
    StructField("provider",     StringType(),    True),
    StructField("tone",         FloatType(),     True),
    StructField("url",          StringType(),    True),
    StructField("is_backfill",  BooleanType(),   True),
    StructField("ingested_at",  TimestampType(), True),
])