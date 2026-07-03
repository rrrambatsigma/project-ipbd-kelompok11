import os
import sys
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minio_utils import create_spark_session, ensure_bucket, get_minio_client
from lang_filter import filter_english, model_available
from sentiment_udfs import (
    vader_sentiment_udf, financial_relevance_udf, nlp_clean_udf,
    is_topic_relevant_udf, keyword_flags_udf, session_tag_udf,
    jaccard_similarity, FINANCIAL_KEYWORDS,
    MIN_RELEVANCE_SCORE, MIN_TEXT_LENGTH, FUZZY_DEDUP_THRESHOLD,
)
from schema import RAW_SCHEMA

BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "news-raw")
BUCKET_PROCESSED = os.getenv("MINIO_BUCKET_PROCESSED", "news-processed")
PROCESSED_PATH = f"s3a://{BUCKET_PROCESSED}/articles"
OUTPUT_ARTICLES = f"s3a://{BUCKET_PROCESSED}/sentiment/articles"
OUTPUT_AGGREGATED = f"s3a://{BUCKET_PROCESSED}/sentiment/aggregated"

SOURCES = ["ecb", "guardian", "gdelt", "newsapi"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

FILTER_STATS = {}


def _send_telegram(message: str) -> None:
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


PUSHGATEWAY_URL = "http://pushgateway:9091"


def _push_metrics(load_count: int, final_count: int, duration: int, filter_stats: dict):
    registry = CollectorRegistry()

    Gauge("preprocessing_articles_total", "Total input articles", registry=registry).set(load_count)
    Gauge("preprocessing_articles_passed", "Articles passed all filters", registry=registry).set(final_count)
    Gauge("preprocessing_duration_seconds", "Pipeline duration", registry=registry).set(duration)
    Gauge("preprocessing_pass_rate", "Pass rate percentage", registry=registry).set(
        final_count / max(load_count, 1) * 100
    )

    g_before = Gauge(f"preprocessing_filter_before", "Before filter", ["stage"], registry=registry)
    g_after = Gauge(f"preprocessing_filter_after", "After filter", ["stage"], registry=registry)
    g_removed = Gauge(f"preprocessing_filter_removed", "Removed by filter", ["stage"], registry=registry)

    for stage, stats in filter_stats.items():
        if stage == "LOAD":
            continue
        g_before.labels(stage=stage).set(stats["before"])
        g_after.labels(stage=stage).set(stats["after"])
        g_removed.labels(stage=stage).set(stats["removed"])

    try:
        push_to_gateway(PUSHGATEWAY_URL, job="preprocessing", registry=registry)
        logger.info(f"[Metrics] Pushed to Pushgateway ({PUSHGATEWAY_URL})")
    except Exception as e:
        logger.warning(f"[Metrics] Gagal push ke Pushgateway: {e}")


def _log_filter_stage(stage: str, before: int, after: int, detail: str = ""):
    removed = before - after
    pct = (removed / before * 100) if before > 0 else 0
    FILTER_STATS[stage] = {"before": before, "after": after, "removed": removed, "pct": round(pct, 1)}
    logger.info(
        f"[{stage}] {before} → {after} | dibuang: {removed} ({pct:.1f}%) {detail}"
    )


# ═══════════════════════════════════════════
# STEP 1 — LOAD DATA
# ═══════════════════════════════════════════

def _count_s3a_paths(bucket: str) -> int:
    client = get_minio_client()
    total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="articles/"):
        total += len(page.get("Contents", []))
    return total


def load_data(spark: SparkSession, use_raw: bool = False) -> DataFrame | None:
    if use_raw:
        logger.info("[LOAD] Membaca dari raw JSON di news-raw...")
        from minio_utils import list_source_paths
        all_dfs = []
        for source in SOURCES:
            paths = list_source_paths(BUCKET_RAW, source)
            if not paths:
                continue
            df = (
                spark.read
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .schema(RAW_SCHEMA)
                .json(paths)
            )
            if "source" not in df.columns or df.filter(F.col("source").isNull()).count() == df.count():
                df = df.withColumn("source", F.lit(source))
            all_dfs.append(df)

        if not all_dfs:
            return None
        df = all_dfs[0]
        for d in all_dfs[1:]:
            df = df.unionByName(d, allowMissingColumns=True)
        logger.info(f"[LOAD] Total dari raw: {df.count()} artikel")
        return df

    logger.info("[LOAD] Membaca dari news-processed/articles/*.parquet...")
    try:
        count = _count_s3a_paths(BUCKET_PROCESSED)
        logger.info(f"[LOAD] Ditemukan {count} file Parquet di {PROCESSED_PATH}")
        df = spark.read.parquet(PROCESSED_PATH)
        row_count = df.count()
        if row_count == 0:
            logger.warning("[LOAD] Parquet kosong, fallback ke raw JSON...")
            return load_data(spark, use_raw=True)
        logger.info(f"[LOAD] {row_count} artikel dimuat dari Parquet")
        return df
    except Exception as e:
        logger.warning(f"[LOAD] Gagal baca Parquet: {e}, fallback ke raw JSON...")
        return load_data(spark, use_raw=True)


# ═══════════════════════════════════════════
# LAYER 1A — LANGUAGE FILTER
# ═══════════════════════════════════════════

def layer_language_filter(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 1A] Language Filter — fastText English detection")
    before = df.count()

    text_col = "raw_text" if "raw_text" in df.columns else "clean_text"
    df = filter_english(df, text_col=text_col)

    after = df.count()
    _log_filter_stage("1A_LANGUAGE", before, after)
    return df


# ═══════════════════════════════════════════
# LAYER 1B — TOPIC RELEVANCE FILTER
# ═══════════════════════════════════════════

def layer_topic_filter(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info(f"[LAYER 1B] Topic Relevance Filter — threshold: ≥{MIN_RELEVANCE_SCORE} poin")
    before = df.count()

    text_col = "clean_text" if "clean_text" in df.columns else "raw_text"
    title_col = "title"

    df = df.withColumn("_relevance_score", financial_relevance_udf(F.col(text_col)))
    df = df.withColumn("_is_topic_relevant", is_topic_relevant_udf(F.col(text_col)))
    df = df.withColumn("_title_relevant", is_topic_relevant_udf(F.col(title_col)))

    df = df.filter(
        (F.col("_is_topic_relevant") == True) |
        (F.col("_title_relevant") == True) |
        (F.col("_relevance_score") >= MIN_RELEVANCE_SCORE)
    )

    after = df.count()
    _log_filter_stage("1B_TOPIC", before, after)
    return df


# ═══════════════════════════════════════════
# LAYER 1C — QUALITY FILTER
# ═══════════════════════════════════════════

def layer_quality_filter(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info(f"[LAYER 1C] Quality Filter — min text: {MIN_TEXT_LENGTH} chars")
    before = df.count()

    text_col = "clean_text" if "clean_text" in df.columns else "raw_text"

    df = df.filter(
        F.col(text_col).isNotNull() &
        (F.length(F.trim(F.col(text_col))) >= MIN_TEXT_LENGTH) &
        F.col("title").isNotNull() &
        (F.length(F.trim(F.col("title"))) >= 10) &
        (~F.col(text_col).rlike(r"(?i)read more|click here|subscribe|sign up|advertisement")) &
        (~F.col(text_col).rlike(r"^https?://"))
    )

    if "published_at" in df.columns:
        df = df.filter(F.col("published_at").isNotNull())

    if "year" in df.columns:
        df = df.filter(F.col("year").between(2021, 2026))

    after = df.count()
    _log_filter_stage("1C_QUALITY", before, after)
    return df


# ═══════════════════════════════════════════
# LAYER 2 — ADVANCED DEDUP
# ═══════════════════════════════════════════

def layer_dedup(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 2] Advanced Dedup — URL exact + fuzzy title")
    before = df.count()

    id_col = "article_id" if "article_id" in df.columns else "url"
    df = df.dropDuplicates([id_col])
    df = df.dropDuplicates(["url"])

    has_year = "year" in df.columns
    df = df.withColumn("_title_key", F.lower(F.regexp_replace(F.col("title"), r"[^\w\s]", "")))

    if has_year:
        window_spec = Window.partitionBy("_title_key").orderBy(F.desc("year"), F.desc("month"))
    else:
        window_spec = Window.partitionBy("_title_key").orderBy("_title_key")
    df = df.withColumn("_rn", F.row_number().over(window_spec))
    df = df.filter(F.col("_rn") == 1).drop("_title_key", "_rn")

    after = df.count()
    _log_filter_stage("2_DEDUP", before, after)
    return df


def layer_fuzzy_dedup(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info(f"[LAYER 2b] Fuzzy Dedup — Jaccard threshold: {FUZZY_DEDUP_THRESHOLD}")

    before = df.count()

    pandas_df = df.select("article_id", "title").toPandas()

    to_remove = set()
    titles = list(pandas_df.iterrows())
    for i, (idx_a, row_a) in enumerate(titles):
        if row_a["article_id"] in to_remove:
            continue
        for j, (idx_b, row_b) in enumerate(titles):
            if i >= j:
                continue
            if row_b["article_id"] in to_remove:
                continue
            sim = jaccard_similarity(row_a["title"], row_b["title"])
            if sim >= FUZZY_DEDUP_THRESHOLD:
                to_remove.add(row_b["article_id"])

    if to_remove:
        df = df.filter(~F.col("article_id").isin(list(to_remove)))

    after = df.count()
    _log_filter_stage("2b_FUZZY_DEDUP", before, after)
    return df


# ═══════════════════════════════════════════
# LAYER 3A — SENTIMENT SCORING
# ═══════════════════════════════════════════

def layer_sentiment(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 3A] VADER Sentiment Scoring")
    before = df.count()

    text_col = "clean_text" if "clean_text" in df.columns else "raw_text"

    df = df.withColumn("_sentiment", vader_sentiment_udf(F.col(text_col)))

    df = df.select(
        "*",
        F.col("_sentiment.compound").alias("vader_compound"),
        F.col("_sentiment.pos").alias("vader_pos"),
        F.col("_sentiment.neg").alias("vader_neg"),
        F.col("_sentiment.neu").alias("vader_neu"),
    ).drop("_sentiment")

    after = df.count()
    _log_filter_stage("3A_SENTIMENT", before, after, "(scoring only, no removal)")
    return df


# ═══════════════════════════════════════════
# LAYER 3B — NLP CLEAN (tokenize + stopword + lemmatize)
# ═══════════════════════════════════════════

def layer_nlp_clean(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 3B] NLP Clean — tokenize, stopword removal, lemmatize")

    text_col = "clean_text" if "clean_text" in df.columns else "raw_text"
    df = df.withColumn("tokens_lemma", nlp_clean_udf(F.col(text_col)))

    return df


# ═══════════════════════════════════════════
# LAYER 3C — KEYWORD FLAGS
# ═══════════════════════════════════════════

def layer_keywords(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 3C] Financial Keyword Extraction")

    text_col = "clean_text" if "clean_text" in df.columns else "raw_text"
    df = df.withColumn("_keywords", keyword_flags_udf(F.col(text_col)))

    for kw in FINANCIAL_KEYWORDS:
        df = df.withColumn(f"has_{kw}", F.col(f"_keywords.has_{kw}"))

    df = df.drop("_keywords", "_relevance_score", "_is_topic_relevant", "_title_relevant")
    return df


# ═══════════════════════════════════════════
# LAYER 3D — SESSION TAGGING
# ═══════════════════════════════════════════

def layer_session_tag(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[LAYER 3D] Session Tagging — mapping UTC ke sesi pasar Eropa")

    ts_col = "published_at"
    if ts_col not in df.columns:
        logger.warning("[SESSION] Kolom published_at tidak ditemukan, skip")
        return df.withColumn("session_tag", F.lit("unknown"))

    df = df.withColumn("_ts",
        F.coalesce(
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col(ts_col), "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.col(ts_col)),
        )
    )
    df = df.withColumn("_hour", F.hour(F.col("_ts")))
    df = df.withColumn("session_tag", session_tag_udf(F.col("_hour")))
    df = df.drop("_ts", "_hour")

    return df


# ═══════════════════════════════════════════
# STEP 7 — AGGREGATE BY SESSION
# ═══════════════════════════════════════════

def aggregate_sentiment(df: DataFrame) -> DataFrame:
    logger.info("─" * 40)
    logger.info("[AGGREGATE] Aggregasi sentimen per sesi pasar")

    agg_exprs = [
        F.avg("vader_compound").alias("avg_compound"),
        F.avg("vader_pos").alias("avg_positive"),
        F.avg("vader_neg").alias("avg_negative"),
        F.avg("vader_neu").alias("avg_neutral"),
        F.count("article_id").alias("article_count"),
        F.stddev("vader_compound").alias("std_compound"),
        F.sum(F.when(F.col("vader_compound") > 0.05, 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("vader_compound") < -0.05, 1).otherwise(0)).alias("negative_count"),
        F.sum(F.when(F.abs(F.col("vader_compound")) <= 0.05, 1).otherwise(0)).alias("neutral_count"),
    ]

    for kw in FINANCIAL_KEYWORDS:
        agg_exprs.append(
            F.sum(F.when(F.col(f"has_{kw}"), 1).otherwise(0)).alias(f"{kw}_mentions")
        )

    date_col = "date"
    if date_col not in df.columns:
        df = df.withColumn("_ts",
            F.coalesce(
                F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                F.to_timestamp(F.col("published_at")),
            )
        )
        df = df.withColumn("date", F.to_date(F.col("_ts")))
        df = df.drop("_ts")

    agg_df = (
        df.groupBy("date", "session_tag")
        .agg(*agg_exprs)
        .orderBy("date", "session_tag")
    )

    count = agg_df.count()
    logger.success(f"[AGGREGATE] {count} baris agregasi per sesi")

    logger.info("\n[AGGREGATE] Distribusi sesi:")
    agg_df.groupBy("session_tag").count().orderBy("session_tag").show()

    return agg_df


# ═══════════════════════════════════════════
# STEP 8 — SAVE
# ═══════════════════════════════════════════

def save_results(articles_df: DataFrame, agg_df: DataFrame) -> None:
    logger.info("─" * 40)
    logger.info(f"[SAVE] Menyimpan {articles_df.count()} artikel sentimen...")

    ensure_bucket(BUCKET_PROCESSED)

    (
        articles_df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUTPUT_ARTICLES)
    )
    logger.success(f"[SAVE] Artikel sentimen → {OUTPUT_ARTICLES}")

    logger.info(f"[SAVE] Menyimpan {agg_df.count()} baris agregasi...")
    (
        agg_df.write
        .mode("overwrite")
        .parquet(f"{OUTPUT_AGGREGATED}/sentiment_by_session")
    )
    logger.success(f"[SAVE] Agregasi sesi → {OUTPUT_AGGREGATED}/sentiment_by_session")

    (
        agg_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{OUTPUT_AGGREGATED}/sentiment_by_session_csv")
    )
    logger.success(f"[SAVE] CSV agregasi → {OUTPUT_AGGREGATED}/sentiment_by_session_csv")


# ═══════════════════════════════════════════
# FILTERING REPORT
# ═══════════════════════════════════════════

def print_filter_report(final_count: int):
    logger.info("\n" + "=" * 55)
    logger.info("  FILTERING REPORT")
    logger.info("=" * 55)

    for stage, stats in FILTER_STATS.items():
        pct_remaining = (stats["after"] / stats["before"] * 100) if stats["before"] > 0 else 0
        bar = "█" * int(pct_remaining / 5) + "░" * (20 - int(pct_remaining / 5))
        logger.info(
            f"  {stage:20s} │ {stats['before']:6d} → {stats['after']:6d} "
            f"│ {stats['removed']:5d} removed │ {bar} {stats['pct']:.0f}%"
        )

    logger.info("=" * 55)
    logger.info(f"  📥 Input total         : {FILTER_STATS.get('LOAD', {}).get('before', 0)}")
    logger.info(f"  ✅ Lolos ke Sentimen   : {final_count}")
    if FILTER_STATS.get('LOAD', {}).get('before', 0) > 0:
        overall_pct = final_count / FILTER_STATS['LOAD']['before'] * 100
        logger.info(f"  📊 Overall pass rate   : {overall_pct:.1f}%")
    logger.info("=" * 55)


# ═══════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════

def run_sentiment_pipeline(use_raw: bool = False, skip_fuzzy: bool = True):
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 55)
    logger.info("  EURO NEWS SENTIMENT PIPELINE")
    logger.info(f"  Start: {start_time.isoformat()}")
    logger.info(f"  Mode : {'RAW JSON' if use_raw else 'PROCESSED PARQUET'}")
    logger.info("=" * 55)

    FILTER_STATS.clear()

    _send_telegram(
        "🧠 <b>Sentiment Pipeline Dimulai</b>\n"
        f"{'─' * 30}\n"
        f"⏰ Start: {start_time.isoformat()[:19]} UTC\n"
        f"📦 Mode: {'RAW' if use_raw else 'PROCESSED'}"
    )

    spark = create_spark_session("EuroNewsSentiment")

    try:
        # ── LOAD ──────────────────────────────────
        logger.info("\n" + "─" * 40)
        logger.info("  📥 LOAD DATA")
        df = load_data(spark, use_raw=use_raw)
        if df is None:
            logger.error("Tidak ada data untuk diproses!")
            _send_telegram("🚨 Sentiment Pipeline: Tidak ada data!")
            return

        load_count = df.count()
        FILTER_STATS["LOAD"] = {"before": load_count, "after": load_count, "removed": 0, "pct": 0.0}
        logger.info(f"  Input: {load_count} artikel")

        # ── LAYER 1A: Language ────────────────────
        if model_available():
            df = layer_language_filter(df)
        else:
            logger.warning("[LAYER 1A] fastText model unavailable — SKIP language filter")

        # ── LAYER 1B: Topic Relevance ─────────────
        df = layer_topic_filter(df)

        # ── LAYER 1C: Quality ─────────────────────
        df = layer_quality_filter(df)

        if df.count() == 0:
            logger.warning("DataFrame kosong setelah filter — lompat ke save laporan")
            print_filter_report(0)
            _send_telegram("⚠️ Sentiment Pipeline: 0 artikel lolos filter")
            return

        # ── Ensure required columns (sebelum fuzzy dedup) ──
        if "article_id" not in df.columns:
            df = df.withColumn("article_id", F.md5(F.col("url")))

        # ── LAYER 2: Dedup ────────────────────────
        df = layer_dedup(df)

        if not skip_fuzzy:
            df = layer_fuzzy_dedup(df)

        # ── Ensure remaining required columns ──────
        if "clean_text" not in df.columns and "raw_text" in df.columns:
            df = df.withColumn("clean_text", F.col("raw_text"))
        if "year" not in df.columns:
            df = df.withColumn("_ts",
                F.coalesce(
                    F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
                    F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                    F.to_timestamp(F.col("published_at"), "yyyy-MM-dd'T'HH:mm:ss"),
                    F.to_timestamp(F.col("published_at")),
                )
            )
            df = df.withColumn("year", F.year(F.col("_ts")))
            df = df.withColumn("month", F.month(F.col("_ts")))
            df = df.drop("_ts")

        # ── LAYER 3A: Sentiment ───────────────────
        df = layer_sentiment(df)

        # ── LAYER 3B: NLP Clean ──────────────────
        df = layer_nlp_clean(df)

        # ── LAYER 3C: Keywords ────────────────────
        df = layer_keywords(df)

        # ── LAYER 3D: Session Tag ─────────────────
        df = layer_session_tag(df)

        # ── Final count ───────────────────────────
        final_count = df.count()
        logger.success(f"\n✅ Total setelah semua layer: {final_count} artikel")

        # ── Print filter report ───────────────────
        print_filter_report(final_count)

        # ── Aggregate ─────────────────────────────
        agg_df = aggregate_sentiment(df)

        # ── Save ──────────────────────────────────
        save_results(df, agg_df)

        # ── Done ──────────────────────────────────
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).seconds

        _send_telegram(
            f"✅ <b>Sentiment Pipeline Selesai!</b>\n"
            f"{'─' * 30}\n"
            f"⏰ durasi        : {duration} detik\n"
            f"📥 input         : {load_count} artikel\n"
            f"✅ lolos filter  : {final_count} artikel\n"
            f"📊 pass rate     : {final_count / max(load_count, 1) * 100:.1f}%\n"
            f"📂 output        : sentiment/articles/ + sentiment/aggregated/\n"
            f"{'─' * 30}\n"
            f"📋 <b>Filtering Summary:</b>\n"
            + "\n".join(
                f"  {'✅' if v['removed'] == 0 else '🗑️'} {k}: {v['before']} → {v['after']}"
                for k, v in FILTER_STATS.items()
                if k != "LOAD"
            )
        )

        # ── Push metrics ────────────────────────────
        _push_metrics(load_count, final_count, duration, FILTER_STATS)

    except Exception as e:
        logger.error(f"Pipeline GAGAL: {e}")
        _send_telegram(f"🚨 <b>Sentiment Pipeline GAGAL</b>\n{str(e)[:200]}")
        raise
    finally:
        spark.stop()


# ═══════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Euro News Sentiment Pipeline")
    parser.add_argument("--raw", action="store_true", help="Baca dari raw JSON (default: parquet)")
    parser.add_argument("--fuzzy", action="store_true", help="Aktifkan fuzzy dedup (lambat, O(n²))")
    args = parser.parse_args()

    run_sentiment_pipeline(use_raw=args.raw, skip_fuzzy=not args.fuzzy)
