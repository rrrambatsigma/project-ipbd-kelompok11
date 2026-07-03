import os
import io
import pandas as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
import boto3
from botocore.client import Config
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Euro News Sentiment API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MINIO_EP = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_AK = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SK = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_EP}",
    aws_access_key_id=MINIO_AK,
    aws_secret_access_key=MINIO_SK,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

BUCKET = "news-processed"


def _s3fs():
    return S3FileSystem(
        access_key=MINIO_AK,
        secret_key=MINIO_SK,
        endpoint_override=MINIO_EP,
        scheme="http",
    )


def _read_csv(key: str):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception:
        return None


def _read_parquet(key: str):
    try:
        fs = _s3fs()
        table = pq.read_table(f"{BUCKET}/{key}", filesystem=fs)
        return table.to_pandas()
    except Exception:
        return None


def _read_text(key: str):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read().decode().strip()
    except Exception:
        return None


def _json_resp(df):
    if df is None or df.empty:
        return Response("[]", media_type="application/json")
    return Response(
        df.to_json(orient="records", date_format="iso"),
        media_type="application/json",
    )


def _csv_resp(df, filename: str):
    if df is None or df.empty:
        return Response(
            "",
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    return Response(
        df.to_csv(index=False),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/")
def root():
    return {
        "status": "ok",
        "endpoints": [
            "GET /health",
            "GET /api/sentiment/daily",
            "GET /api/sentiment/daily.csv",
            "GET /api/sentiment/predictions",
            "GET /api/sentiment/report",
            "GET /api/sentiment/confusion-matrix",
            "GET /api/lda/top-words",
            "GET /api/aggregated/sessions",
            "GET /api/aggregated/sessions.csv",
        ],
    }


@app.get("/health")
def health():
    try:
        s3.list_buckets()
        return {"status": "alive", "minio": True}
    except Exception:
        return {"status": "alive", "minio": False}


@app.get("/api/sentiment/daily")
def daily_json():
    return _json_resp(_read_csv("models/latest/predictions_daily.csv"))


@app.get("/api/sentiment/daily.csv")
def daily_csv():
    return _csv_resp(_read_csv("models/latest/predictions_daily.csv"), "predictions_daily.csv")


@app.get("/api/sentiment/predictions")
def predictions():
    run_id = _read_text("models/sentiment/latest.txt")
    if not run_id:
        return Response("[]", media_type="application/json")
    return _json_resp(_read_csv(f"models/{run_id}/sentiment/test_predictions.csv"))


@app.get("/api/sentiment/report")
def report():
    run_id = _read_text("models/sentiment/latest.txt")
    if not run_id:
        return Response("[]", media_type="application/json")
    return _json_resp(_read_csv(f"models/{run_id}/sentiment/report.csv"))


@app.get("/api/sentiment/confusion-matrix")
def confusion():
    run_id = _read_text("models/sentiment/latest.txt")
    if not run_id:
        return Response("[]", media_type="application/json")
    return _json_resp(_read_csv(f"models/{run_id}/sentiment/confusion_matrix.csv"))


@app.get("/api/lda/top-words")
def top_words():
    run_id = _read_text("models/lda/latest.txt")
    if not run_id:
        return Response("[]", media_type="application/json")
    return _json_resp(_read_csv(f"models/{run_id}/lda/top_words.csv"))


@app.get("/api/lda/topics")
def topics():
    run_id = _read_text("models/lda/latest.txt")
    if not run_id:
        return Response("[]", media_type="application/json")
    return _json_resp(_read_parquet(f"models/{run_id}/lda/topics.parquet"))


@app.get("/api/aggregated/sessions")
def sessions_json():
    return _json_resp(_read_parquet("sentiment/aggregated/sentiment_by_session"))


@app.get("/api/aggregated/sessions.csv")
def sessions_csv():
    return _csv_resp(_read_parquet("sentiment/aggregated/sentiment_by_session"), "sentiment_by_session.csv")
