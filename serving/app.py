import os
import io
import pandas as pd
import boto3
from botocore.client import Config
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Euro News Sentiment API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MINIO_EP = os.getenv("MINIO_ENDPOINT", "minio:9000")
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
KEY = "models/latest/predictions_daily.csv"


@app.get("/")
def root():
    return {
        "status": "ok",
        "endpoints": [
            "/api/sentiment/daily      -> JSON",
            "/api/sentiment/daily.csv  -> CSV",
        ],
    }


@app.get("/api/sentiment/daily")
def get_daily_json():
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return Response(
        df.to_json(orient="records", date_format="iso"),
        media_type="application/json",
    )


@app.get("/api/sentiment/daily.csv")
def get_daily_csv():
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    return Response(
        obj["Body"].read(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=predictions_daily.csv"},
    )
