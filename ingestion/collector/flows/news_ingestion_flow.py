"""
flows/news_ingestion_flow.py
─────────────────────────────────────────────────────────
Prefect Batch Flow — Euro News Ingestion
Orkestrasi: ECB (Tier 1) + GDELT (Tier 2) + NewsAPI (Tier 3)

Jadwal batch (WIB / UTC):
  06:00 WIB = 23:00 UTC (hari sebelumnya)  Pre-market  — lookback 7h
  09:00 WIB = 02:00 UTC                    Open        — lookback 3h
  13:00 WIB = 06:00 UTC                    Mid         — lookback 4h
  17:00 WIB = 10:00 UTC                    Pre-close   — lookback 4h
  21:00 WIB = 14:00 UTC                    Overlap     — lookback 4h

  Senin–Jumat aktif, Sabtu–Minggu SKIP.
Output: Raw JSON → MinIO bucket 'news-raw'
─────────────────────────────────────────────────────────
"""

import hashlib
import os
import sys
from datetime import datetime, timezone

_FLOWS_DIR = os.path.dirname(os.path.abspath(__file__))
if _FLOWS_DIR not in sys.path:
    sys.path.insert(0, _FLOWS_DIR)

from dotenv import load_dotenv
from loguru import logger
from prefect import flow, task, get_run_logger
from prefect.futures import PrefectFuture

load_dotenv()

from scrapers.ecb_scraper import scrape_ecb
from scrapers.gdelt_scraper import scrape_gdelt
from scrapers.newsapi_scraper import scrape_newsapi
from storage.minio_client import MinIOClient
from utils.telegram_alert import alert_batch_success, alert_batch_failed


# ═══════════════════════════════════════════
# LOOKBACK HOURS PER SESI
# ═══════════════════════════════════════════
LOOKBACK_PER_SESSION: dict[str, int] = {
    "pre_market": 7,
    "open":       3,
    "mid":        4,
    "pre_close":  4,
    "overlap":    4,
}


# ═══════════════════════════════════════════
# HELPER: Deduplication dalam batch
# Dedup lintas batch ditangani di MinIOClient.upload_article()
# dengan cek stat_object sebelum upload
# ═══════════════════════════════════════════

def _make_article_id(article: dict) -> str:
    """
    Fingerprint unik dari artikel berdasarkan URL atau judul+tanggal.
    Dipakai untuk dedup dalam satu batch sebelum kirim ke MinIO.
    """
    url       = article.get("url")  or article.get("link")        or ""
    title     = article.get("title")                               or ""
    published = article.get("published_at") or article.get("publishedAt") or ""
    raw       = f"{url}|{title}|{published}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _deduplicate(articles: list[dict]) -> list[dict]:
    """Hapus duplikat dalam satu batch (by fingerprint URL+title+date)."""
    seen: set[str]    = set()
    unique: list[dict] = []
    for art in articles:
        fid = _make_article_id(art)
        if fid not in seen:
            seen.add(fid)
            art["_id"] = fid
            unique.append(art)
    return unique


# ═══════════════════════════════════════════
# TASKS
# ═══════════════════════════════════════════

@task(
    name="health-check-minio",
    retries=3,
    retry_delay_seconds=10,
    tags=["infrastructure"],
)
def task_health_check_minio() -> bool:
    run_logger = get_run_logger()
    client = MinIOClient()
    ok = client.health_check()
    if not ok:
        raise ConnectionError("MinIO tidak dapat diakses.")
    run_logger.info("MinIO health check: PASSED")
    return ok


@task(
    name="scrape-ecb",
    retries=2,
    retry_delay_seconds=30,
    tags=["scraper", "tier-1", "ecb"],
)
def task_scrape_ecb() -> list[dict]:
    run_logger = get_run_logger()
    run_logger.info("[ECB] Memulai scraping RSS...")
    articles = scrape_ecb(max_articles_per_feed=20)
    unique   = _deduplicate(articles)
    run_logger.info(f"[ECB] Selesai — raw: {len(articles)}, unik: {len(unique)}")
    return unique


@task(
    name="scrape-gdelt",
    retries=2,
    retry_delay_seconds=30,
    tags=["scraper", "tier-2", "gdelt"],
)
def task_scrape_gdelt() -> list[dict]:
    run_logger = get_run_logger()
    run_logger.info("[GDELT] Memulai scraping RSS + Guardian...")
    articles = scrape_gdelt(max_per_query=25)
    unique   = _deduplicate(articles)
    run_logger.info(f"[GDELT] Selesai — raw: {len(articles)}, unik: {len(unique)}")
    return unique


@task(
    name="scrape-newsapi",
    retries=2,
    retry_delay_seconds=60,
    tags=["scraper", "tier-3", "newsapi"],
)
def task_scrape_newsapi(lookback_hours: int = 4) -> list[dict]:
    run_logger = get_run_logger()
    run_logger.info(f"[NewsAPI] Query lookback {lookback_hours}h...")
    articles = scrape_newsapi(lookback_hours=lookback_hours)
    unique   = _deduplicate(articles)
    run_logger.info(f"[NewsAPI] Selesai — raw: {len(articles)}, unik: {len(unique)}")
    return unique


@task(
    name="upload-to-minio",
    retries=3,
    retry_delay_seconds=15,
    tags=["storage", "minio"],
)
def task_upload_to_minio(articles: list[dict], source: str) -> dict:
    run_logger = get_run_logger()
    if not articles:
        run_logger.warning(f"[{source}] Tidak ada artikel untuk diupload")
        return {"source": source, "total_input": 0, "total_uploaded": 0, "failed": 0}

    client   = MinIOClient()
    # upload_batch sekarang skip otomatis jika artikel sudah ada di MinIO
    uploaded = client.upload_batch(articles, source)
    skipped  = sum(1 for a in articles if a.get("_skipped_duplicate"))
    failed   = len(articles) - len(uploaded) - skipped

    run_logger.info(
        f"[{source}] Upload — berhasil: {len(uploaded)}, "
        f"skip duplikat: {skipped}, gagal: {failed}"
    )
    return {
        "source":         source,
        "total_input":    len(articles),
        "total_uploaded": len(uploaded),
        "skipped":        skipped,
        "failed":         max(failed, 0),
    }


@task(
    name="generate-summary",
    tags=["reporting"],
)
def task_generate_summary(
    upload_results: list[dict],
    batch_start: str,
    session: str,
) -> dict:
    run_logger = get_run_logger()
    batch_end      = datetime.now(timezone.utc).isoformat()
    total_uploaded = sum(r["total_uploaded"] for r in upload_results)
    total_failed   = sum(r["failed"]         for r in upload_results)
    total_skipped  = sum(r.get("skipped", 0) for r in upload_results)
    total_input    = sum(r["total_input"]     for r in upload_results)

    summary = {
        "session":        session,
        "batch_start":    batch_start,
        "batch_end":      batch_end,
        "total_input":    total_input,
        "total_uploaded": total_uploaded,
        "total_skipped":  total_skipped,
        "total_failed":   total_failed,
        "per_source":     upload_results,
        "status":         "SUCCESS" if total_failed == 0 else "PARTIAL",
    }

    run_logger.info(
        f"\n{'─'*55}\n"
        f"  BATCH SUMMARY — Sesi: {session.upper()}\n"
        f"  Start   : {batch_start}\n"
        f"  End     : {batch_end}\n"
        f"  Input   : {total_input}\n"
        f"  Upload  : {total_uploaded}\n"
        f"  Skip    : {total_skipped} (duplikat lintas batch)\n"
        f"  Failed  : {total_failed}\n"
        f"  Status  : {summary['status']}\n"
        f"{'─'*55}"
    )
    return summary


# ═══════════════════════════════════════════
# FLOW UTAMA
# FIX: scraping berjalan PARALEL, bukan serial
# ═══════════════════════════════════════════

@flow(
    name="euro_news_batch",
    description=(
        "Batch ingestion berita Eropa — ECB, GDELT, NewsAPI. "
        "Jadwal mengikuti sesi pasar Eropa (Senin–Jumat). "
        "Output: raw JSON ke MinIO bucket news-raw."
    ),
)
def euro_news_batch_flow(
    session:        str  = "manual",
    lookback_hours: int  = -1,
    run_ecb:        bool = True,
    run_gdelt:      bool = True,
    run_newsapi:    bool = True,
):
    if lookback_hours == -1:
        lookback_hours = LOOKBACK_PER_SESSION.get(session, 4)

    batch_start = datetime.now(timezone.utc).isoformat()
    logger.info(
        f"[{session.upper()}] Flow dimulai: {batch_start} | lookback: {lookback_hours}h"
    )

    # Health check dulu
    task_health_check_minio()

    # ── FIX: Submit semua scraping tasks PARALEL ──────────────────
    scrape_futures: list[tuple[str, PrefectFuture]] = []

    if run_ecb:
        scrape_futures.append(("ecb", task_scrape_ecb.submit()))

    if run_gdelt:
        scrape_futures.append(("gdelt", task_scrape_gdelt.submit()))

    if run_newsapi:
        scrape_futures.append(("newsapi", task_scrape_newsapi.submit(lookback_hours=lookback_hours)))

    # ── Tunggu hasil scraping, lalu upload ───────────────────────
    upload_results = []
    for source, future in scrape_futures:
        try:
            articles = future.result()
            result   = task_upload_to_minio(articles, source=source)
            upload_results.append(result)
        except Exception as e:
            logger.error(f"[{source}] Task gagal: {e}")
            upload_results.append({
                "source":         source,
                "total_input":    0,
                "total_uploaded": 0,
                "skipped":        0,
                "failed":         1,
            })

    summary = task_generate_summary(upload_results, batch_start, session)

    try:
        if summary["status"] == "SUCCESS":
            alert_batch_success(summary)
        else:
            alert_batch_failed(session, f"PARTIAL — {summary['total_failed']} artikel gagal")
    except Exception as e:
        logger.warning(f"Alert Telegram gagal (non-fatal): {e}")

    return summary


# ═══════════════════════════════════════════
# ENTRYPOINT
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Euro News Batch Flow")
    parser.add_argument("--session",    default="manual")
    parser.add_argument("--lookback",   type=int, default=-1)
    parser.add_argument("--no-ecb",     action="store_false", dest="run_ecb",     default=True)
    parser.add_argument("--no-gdelt",   action="store_false", dest="run_gdelt",   default=True)
    parser.add_argument("--no-newsapi", action="store_false", dest="run_newsapi", default=True)
    args = parser.parse_args()

    euro_news_batch_flow(
        session=args.session,
        lookback_hours=args.lookback,
        run_ecb=args.run_ecb,
        run_gdelt=args.run_gdelt,
        run_newsapi=args.run_newsapi,
    )
