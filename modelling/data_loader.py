import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
import pandas as pd
from loguru import logger

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BUCKET_PROCESSED,
    ARTICLES_PATH,
)


def _get_s3fs() -> S3FileSystem:
    return S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=MINIO_ENDPOINT,
        scheme="http",
    )


def load_articles() -> pd.DataFrame:
    fs = _get_s3fs()
    path = f"{BUCKET_PROCESSED}/{ARTICLES_PATH}"

    logger.info(f"Membaca artikel dari MinIO: {path}")
    try:
        table = pq.read_table(path, filesystem=fs)
    except Exception as e:
        logger.error(f"Gagal baca parquet: {e}")
        raise

    df = table.to_pandas()
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    cols_needed = [
        "article_id", "clean_text", "vader_compound",
        "vader_pos", "vader_neg", "vader_neu",
        "published_at", "source", "category",
    ]
    kw_cols = [c for c in df.columns if c.startswith("has_")]
    cols_needed.extend(kw_cols)

    keep = [c for c in cols_needed if c in df.columns]
    missing = [c for c in cols_needed if c not in df.columns]
    if missing:
        logger.warning(f"Kolom tidak ditemukan: {missing}")

    df = df[keep].copy()
    df.dropna(subset=["clean_text"], inplace=True)
    df["clean_text"] = df["clean_text"].astype(str)

    min_words = 10
    before = len(df)
    df = df[df["clean_text"].str.split().str.len() >= min_words]
    logger.info(f"Filter artikel <{min_words} kata: {before} -> {len(df)}")

    return df


def load_aggregated() -> pd.DataFrame:
    fs = _get_s3fs()
    path = f"{BUCKET_PROCESSED}/sentiment/aggregated/sentiment_by_session"

    logger.info(f"Membaca agregasi dari MinIO: {path}")
    try:
        table = pq.read_table(path, filesystem=fs)
    except Exception as e:
        logger.warning(f"Gagal baca aggregated parquet: {e}")
        return pd.DataFrame()

    df = table.to_pandas()
    logger.info(f"Loaded {len(df)} aggregated rows")
    return df
