import io
import joblib
import pandas as pd
from loguru import logger
import boto3
from botocore.client import Config

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    BUCKET_PROCESSED,
    MODEL_PATH,
)


def _get_s3():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def save_pkl(obj, bucket: str, key: str):
    s3 = _get_s3()
    buffer = io.BytesIO()
    joblib.dump(obj, buffer)
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket, key)
    logger.info(f"Saved {key} to {bucket}")


def load_pkl(bucket: str, key: str):
    s3 = _get_s3()
    try:
        buffer = io.BytesIO()
        s3.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        return joblib.load(buffer)
    except Exception as e:
        logger.warning(f"Gagal load {key}: {e}")
        return None


def save_csv(df: pd.DataFrame, bucket: str, key: str):
    s3 = _get_s3()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Saved {key} to {bucket}")


def save_all(
    run_id: str,
    lda_result: dict,
    sentiment_result: dict,
):
    prefix = f"{MODEL_PATH}/{run_id}"

    # LDA
    lda_prefix = f"{prefix}/lda"
    save_pkl(lda_result["vectorizer"], BUCKET_PROCESSED, f"{lda_prefix}/vectorizer.pkl")
    save_pkl(lda_result["lda_model"], BUCKET_PROCESSED, f"{lda_prefix}/lda.pkl")

    topics_key = f"{lda_prefix}/topics.parquet"
    import io
    buf = io.BytesIO()
    lda_result["topics_df"].to_parquet(buf, index=False)
    buf.seek(0)
    s3 = _get_s3()
    s3.upload_fileobj(buf, BUCKET_PROCESSED, topics_key)

    save_csv(lda_result["top_words_df"], BUCKET_PROCESSED, f"{lda_prefix}/top_words.csv")

    # Sentiment
    sent_prefix = f"{prefix}/sentiment"
    save_pkl(sentiment_result["vectorizer"], BUCKET_PROCESSED, f"{sent_prefix}/vectorizer.pkl")
    save_pkl(sentiment_result["classifier"], BUCKET_PROCESSED, f"{sent_prefix}/classifier.pkl")
    save_csv(sentiment_result["report_df"], BUCKET_PROCESSED, f"{sent_prefix}/report.csv")
    save_csv(sentiment_result["confusion_df"], BUCKET_PROCESSED, f"{sent_prefix}/confusion_matrix.csv")
    save_csv(sentiment_result["test_predictions"], BUCKET_PROCESSED, f"{sent_prefix}/test_predictions.csv")

    logger.info(f"All models saved to {BUCKET_PROCESSED}/{prefix}/")


def save_latest_symlink(prefix_type: str, run_id: str):
    """Simpan marker file 'latest' yang berisi run_id terbaru."""
    s3 = _get_s3()
    s3.put_object(
        Bucket=BUCKET_PROCESSED,
        Key=f"{MODEL_PATH}/{prefix_type}/latest.txt",
        Body=run_id.encode(),
    )
