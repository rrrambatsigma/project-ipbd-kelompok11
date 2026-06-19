"""
jobs/minio_utils.py
─────────────────────────────────────────────────────────
Utility koneksi MinIO ↔ Apache Spark via S3A connector.
─────────────────────────────────────────────────────────
"""

import os
import boto3
from botocore.client import Config
from loguru import logger
from pyspark.sql import SparkSession


def get_minio_client():
    """Boto3 client untuk operasi MinIO (list, check bucket, dll)."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def create_spark_session(app_name: str = "EuroNewsPreprocessing") -> SparkSession:
    """
    Buat SparkSession dengan konfigurasi S3A untuk MinIO.
    Pakai hadoop-aws agar Spark bisa baca/tulis MinIO seperti S3.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ── Hadoop S3A jars (lokal, pre-downloaded saat Docker build) ──
        # Pakai spark.jars (path lokal) BUKAN spark.jars.packages,
        # supaya Spark gak coba resolve/download dari Maven Central
        # tiap kali SparkSession dibuat (container gak ada akses internet runtime).
        .config(
            "spark.jars",
            "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
        )

        # ── S3A → MinIO konfigurasi ──────────────────
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── Spark tuning ─────────────────────────────
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession dibuat: {app_name} | master: local[*]")
    return spark


def ensure_bucket(bucket_name: str) -> None:
    """Buat bucket MinIO jika belum ada."""
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket sudah ada: {bucket_name}")
    except Exception:
        client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket dibuat: {bucket_name}")


def list_source_paths(bucket: str, source: str) -> list[str]:
    """
    List semua path S3A untuk satu source di bucket raw.
    Return list path yang bisa dibaca Spark.
    """
    client = get_minio_client()
    prefix = f"{source}/"
    paths = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                paths.append(f"s3a://{bucket}/{key}")

    logger.info(f"[{source}] Ditemukan {len(paths)} file JSON di MinIO")
    return paths