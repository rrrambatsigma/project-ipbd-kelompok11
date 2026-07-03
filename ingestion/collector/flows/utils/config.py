"""
utils/config.py
─────────────────────────────────────────────────────────
Konfigurasi terpusat untuk Euro News Ingestion Pipeline.
Semua nilai dibaca dari environment variables (.env).
─────────────────────────────────────────────────────────
"""

import os
from dotenv import load_dotenv

load_dotenv()


# ── MinIO ─────────────────────────────────────────────────
MINIO_ENDPOINT          = os.getenv("MINIO_ENDPOINT",          "localhost:9000")
MINIO_ACCESS_KEY        = os.getenv("MINIO_ACCESS_KEY",        "minioadmin")
MINIO_SECRET_KEY        = os.getenv("MINIO_SECRET_KEY",        "minioadmin123")
MINIO_BUCKET_RAW        = os.getenv("MINIO_BUCKET_RAW",        "news-raw")
MINIO_BUCKET_PROCESSED  = os.getenv("MINIO_BUCKET_PROCESSED",  "news-processed")

# ── API Keys ──────────────────────────────────────────────
NEWSAPI_KEY             = os.getenv("NEWSAPI_KEY",             "")
GUARDIAN_API_KEY        = os.getenv("GUARDIAN_API_KEY",        "test")

# ── Prefect ───────────────────────────────────────────────
PREFECT_API_URL         = os.getenv("PREFECT_API_URL",         "http://localhost:4200/api")

# ── Selenium ──────────────────────────────────────────────
SELENIUM_HUB_URL        = os.getenv("SELENIUM_HUB_URL",        "http://localhost:4444/wd/hub")

# ── Scraper Settings ──────────────────────────────────────
SCRAPER_REQUEST_TIMEOUT = int(os.getenv("SCRAPER_REQUEST_TIMEOUT", 30))
SCRAPER_DELAY_SECONDS   = float(os.getenv("SCRAPER_DELAY_SECONDS", 2))
SCRAPER_MAX_RETRIES     = int(os.getenv("SCRAPER_MAX_RETRIES",     3))

# ── Telegram ──────────────────────────────────────────────
TELEGRAM_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN",      "")
TELEGRAM_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID",        "")

# ── Backfill ──────────────────────────────────────────────
BACKFILL_START          = os.getenv("BACKFILL_START",          "2021-01-01")
BACKFILL_END            = os.getenv("BACKFILL_END",            "2025-12-31")

# ── Lookback per sesi (dalam jam) ────────────────────────
LOOKBACK_PER_SESSION: dict[str, int] = {
    "pre_market": 7,
    "open":       3,
    "mid":        4,
    "pre_close":  4,
    "overlap":    4,
}
