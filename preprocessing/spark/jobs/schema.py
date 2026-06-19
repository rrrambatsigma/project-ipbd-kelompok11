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
RAW_SCHEMA = StructType([
    StructField("title",        StringType(),    True),
    StructField("url",          StringType(),    True),
    StructField("published_at", StringType(),    True),  # string dulu, nanti di-cast
    StructField("source",       StringType(),    True),
    StructField("source_tier",  IntegerType(),   True),
    StructField("category",     StringType(),    True),
    StructField("raw_text",     StringType(),    True),
    StructField("language",     StringType(),    True),
    StructField("provider",     StringType(),    True),
    StructField("tone",         FloatType(),     True),  # khusus GDELT
    StructField("author",       StringType(),    True),  # khusus NewsAPI
    StructField("_ingested_at", StringType(),    True),
    StructField("_source_tier", StringType(),    True),
    StructField("_backfill",    BooleanType(),   True),
])

# ── Schema output setelah preprocessing ──────────────────
PROCESSED_SCHEMA = StructType([
    StructField("article_id",   StringType(),    False),  # hash unik MD5
    StructField("title",        StringType(),    True),
    StructField("clean_text",   StringType(),    True),   # raw_text sudah bersih
    StructField("published_at", TimestampType(), True),
    StructField("year",         IntegerType(),   True),
    StructField("month",        IntegerType(),   True),
    StructField("day",          IntegerType(),   True),
    StructField("source",       StringType(),    True),
    StructField("source_tier",  IntegerType(),   True),
    StructField("category",     StringType(),    True),
    StructField("language",     StringType(),    True),
    StructField("provider",     StringType(),    True),
    StructField("tone",         FloatType(),     True),
    StructField("url",          StringType(),    True),
    StructField("is_backfill",  BooleanType(),   True),
    StructField("ingested_at",  TimestampType(), True),
])