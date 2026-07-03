"""
storage/minio_client.py
─────────────────────────────────────────────────────────
Abstraksi MinIO client untuk upload raw article JSON.

Struktur path di bucket:
  news-raw/
    {source}/
      {YYYY}/
        {YYYY-MM-DD}/
          {source}_{YYYYMMDD_HHMMSS}_{uuid4[:8]}.json

FIX: upload_article() sekarang cek stat_object terlebih dahulu.
     Jika objek sudah ada (duplikat lintas batch), skip tanpa error.
─────────────────────────────────────────────────────────
"""

import json
import uuid
import os
from datetime import datetime
from io import BytesIO

from dateutil.parser import parse as parse_date
from minio import Minio
from minio.error import S3Error
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from utils.telegram_alert import alert_article
    TELEGRAM_ENABLED = True
except ImportError:
    TELEGRAM_ENABLED = False


class MinIOClient:
    """Wrapper MinIO untuk menyimpan raw articles JSON."""

    def __init__(self):
        endpoint   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False,
        )
        self.bucket_raw = os.getenv("MINIO_BUCKET_RAW", "news-raw")
        logger.info(f"MinIO client initialized → endpoint: {endpoint}")

    # ──────────────────────────────────────
    # INTERNAL HELPERS
    # ──────────────────────────────────────

    def _ensure_bucket(self, bucket: str) -> None:
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Bucket dibuat: {bucket}")
        except S3Error as e:
            logger.error(f"Gagal membuat bucket {bucket}: {e}")
            raise

    def _build_object_name(self, source: str, article_id: str, timestamp: datetime) -> str:
        """
        Path deterministik berdasarkan _id artikel:
          source/YYYY/YYYY-MM-DD/{source}_{YYYYMMDD}_{article_id[:12]}.json

        Menggunakan article_id (SHA256 dari URL+title+date) sebagai bagian
        nama file sehingga objek yang sama selalu punya path yang sama.
        Ini yang mencegah duplikat lintas batch.
        """
        year_str = timestamp.strftime("%Y")
        date_str = timestamp.strftime("%Y-%m-%d")
        date_compact = timestamp.strftime("%Y%m%d")
        short_id = article_id[:12]
        return f"{source}/{year_str}/{date_str}/{source}_{date_compact}_{short_id}.json"

    def _parse_article_datetime(self, article: dict) -> datetime:
        pub = article.get("published_at", "")
        if pub:
            try:
                dt = parse_date(pub)
                return dt.replace(tzinfo=None)
            except Exception:
                pass
        return datetime.utcnow()

    def _object_exists(self, object_name: str) -> bool:
        """
        Cek apakah object sudah ada di MinIO.
        FIX: Ini yang mencegah duplikat lintas batch.
        """
        try:
            self.client.stat_object(self.bucket_raw, object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            # Error lain (network, dll) — anggap tidak ada, coba upload
            logger.warning(f"stat_object error untuk {object_name}: {e}")
            return False

    # ──────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    def upload_article(self, article: dict, source: str) -> str | None:
        """
        Upload satu artikel ke bucket raw.

        Returns
        -------
        str   — object name jika berhasil upload (baru)
        None  — jika artikel sudah ada (duplikat lintas batch, di-skip)
        """
        self._ensure_bucket(self.bucket_raw)

        article_id  = article.get("_id") or str(uuid.uuid4())
        article_dt  = self._parse_article_datetime(article)
        object_name = self._build_object_name(source, article_id, article_dt)

        # ── FIX: Cek duplikat lintas batch ──────────────────────────
        if self._object_exists(object_name):
            logger.debug(f"[{source}] Skip duplikat: {object_name}")
            article["_skipped_duplicate"] = True
            return None

        # Tambahkan metadata ingestion
        article["_ingested_at"] = datetime.utcnow().isoformat()
        article["_source_tier"] = source

        payload       = json.dumps(article, ensure_ascii=False, indent=2)
        payload_bytes = payload.encode("utf-8")

        self.client.put_object(
            bucket_name  = self.bucket_raw,
            object_name  = object_name,
            data         = BytesIO(payload_bytes),
            length       = len(payload_bytes),
            content_type = "application/json",
        )

        logger.success(f"[{source}] Uploaded → {self.bucket_raw}/{object_name}")

        if TELEGRAM_ENABLED:
            try:
                alert_article(article)
            except Exception:
                pass

        return object_name

    def upload_batch(self, articles: list[dict], source: str) -> list[str]:
        """
        Upload batch artikel. Skip otomatis jika sudah ada (duplikat lintas batch).
        Returns list of object names yang benar-benar baru diupload.
        """
        uploaded = []
        skipped  = 0
        failed   = 0

        for article in articles:
            try:
                obj_name = self.upload_article(article, source)
                if obj_name is None:
                    skipped += 1
                else:
                    uploaded.append(obj_name)
            except Exception as e:
                logger.error(f"[{source}] Gagal upload artikel: {e}")
                failed += 1

        logger.info(
            f"[{source}] Batch selesai — "
            f"upload baru: {len(uploaded)}, "
            f"skip duplikat: {skipped}, "
            f"gagal: {failed}"
        )
        return uploaded

    def list_objects(
        self,
        source: str,
        year: str = None,
        date_str: str = None,
    ) -> list[str]:
        """
        List object berdasarkan source / tahun / tanggal.
        Contoh:
          list_objects('guardian')                    → semua guardian
          list_objects('guardian', '2021')            → guardian 2021
          list_objects('guardian', '2021','2021-03-15') → tanggal spesifik
        """
        if date_str:
            year   = year or date_str[:4]
            prefix = f"{source}/{year}/{date_str}/"
        elif year:
            prefix = f"{source}/{year}/"
        else:
            prefix = f"{source}/"

        objects = self.client.list_objects(self.bucket_raw, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]

    def health_check(self) -> bool:
        try:
            self.client.list_buckets()
            logger.info("MinIO health check: OK")
            return True
        except Exception as e:
            logger.error(f"MinIO health check FAILED: {e}")
            return False
