"""
flows/backfill_flow.py
─────────────────────────────────────────────────────────
Backfill historis berita Euro 2021–2025
Sumber  : The Guardian API (500 req/hari, gratis)
Output  : Raw JSON → MinIO bucket 'news-raw'
          Path: guardian/{YYYY-MM-DD}/guardian_backfill_*.json

Strategi: per bulan, dari 2021-01 sampai 2025-12
Total batch: ~60 bulan × 4 query = ~240 request
Dengan 500 req/hari → selesai dalam 1 hari
─────────────────────────────────────────────────────────
"""

import os
import sys
import time
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from storage.minio_client import MinIOClient
from utils.telegram_alert import alert_backfill_progress, alert_backfill_done

# ── Konfigurasi ───────────────────────────────────────────
GUARDIAN_API_URL = "https://content.guardianapis.com/search"
GUARDIAN_API_KEY = os.getenv("GUARDIAN_API_KEY", "test")

BACKFILL_START = "2021-01-01"
BACKFILL_END   = "2025-12-31"

# Query keywords relevan dengan tema Euro/forex
GUARDIAN_QUERIES = [
    "euro exchange rate EUR USD",
    "ECB European Central Bank monetary policy",
    "eurozone inflation interest rate",
    "European economy forex market",
]

REQUEST_TIMEOUT = 30
DELAY_SECONDS   = 1.5   # Jaga rate limit: 500 req/hari = ~1 req/2.9 detik


# ── Fetcher ──────────────────────────────────────────────

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=15))
def _fetch_guardian_page(
    query: str,
    date_from: str,
    date_to: str,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """Fetch satu halaman artikel Guardian untuk rentang tanggal tertentu."""
    params = {
        "q":            query,
        "api-key":      GUARDIAN_API_KEY,
        "from-date":    date_from,    # format: YYYY-MM-DD
        "to-date":      date_to,
        "page":         page,
        "page-size":    page_size,
        "order-by":     "oldest",
        "show-fields":  "trailText,bodyText,wordcount",
        "section":      "business|money|world",
    }
    resp = requests.get(GUARDIAN_API_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("response", {})


def _response_to_articles(results: list[dict], query: str) -> list[dict]:
    """Konversi hasil Guardian ke schema standar."""
    articles = []
    for r in results:
        url   = r.get("webUrl", "")
        title = r.get("webTitle", "").strip()
        if not url or not title:
            continue

        fields   = r.get("fields", {})
        raw_text = (
            fields.get("bodyText")
            or fields.get("trailText")
            or title
        )

        articles.append({
            "title":        title,
            "url":          url,
            "published_at": r.get("webPublicationDate", ""),
            "source":       "guardian",
            "source_tier":  2,
            "category":     query,
            "raw_text":     raw_text[:3000],
            "language":     "en",
            "provider":     "theguardian.com",
            "tone":         None,
            "_backfill":    True,
        })
    return articles


# ── Core backfill per bulan ───────────────────────────────

def backfill_month(year: int, month: int) -> int:
    """
    Backfill satu bulan untuk semua query.
    Return total artikel yang berhasil diupload.
    """
    # Hitung rentang tanggal bulan ini
    date_from = f"{year:04d}-{month:02d}-01"
    last_day  = (
        datetime(year, month, 1) + relativedelta(months=1) - relativedelta(days=1)
    )
    date_to   = last_day.strftime("%Y-%m-%d")

    logger.info(f"[Backfill] Proses: {date_from} → {date_to}")

    client        = MinIOClient()
    total_uploaded = 0
    seen_urls: set[str] = set()

    for query in GUARDIAN_QUERIES:
        try:
            # Ambil halaman 1 dulu (max 50 artikel per request)
            response  = _fetch_guardian_page(query, date_from, date_to, page=1)
            results   = response.get("results", [])
            total_res = response.get("total", 0)

            articles = _response_to_articles(results, query)

            # Deduplikasi
            unique = []
            for a in articles:
                if a["url"] not in seen_urls:
                    seen_urls.add(a["url"])
                    unique.append(a)

            if unique:
                uploaded = client.upload_batch(unique, source="guardian")
                total_uploaded += len(uploaded)

            logger.success(
                f"  [{date_from[:7]}] '{query[:40]}' → "
                f"{len(unique)} artikel (total tersedia: {total_res})"
            )

            time.sleep(DELAY_SECONDS)

        except Exception as e:
            logger.error(f"  [{date_from[:7]}] Gagal query '{query[:30]}': {e}")
            continue

    return total_uploaded


# ── Main backfill runner ──────────────────────────────────

def run_backfill(
    start_date: str = BACKFILL_START,
    end_date: str   = BACKFILL_END,
    dry_run: bool   = False,
):
    """
    Jalankan backfill dari start_date sampai end_date per bulan.

    Parameters
    ----------
    start_date : str   format YYYY-MM-DD
    end_date   : str   format YYYY-MM-DD
    dry_run    : bool  True = hanya print plan, tidak upload
    """
    logger.info("=" * 55)
    logger.info("  GUARDIAN BACKFILL — Euro News 2021–2025")
    logger.info(f"  Range  : {start_date} → {end_date}")
    logger.info(f"  Queries: {len(GUARDIAN_QUERIES)}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info("=" * 55)

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    # Hitung total bulan
    current     = start.replace(day=1)
    months_list = []
    while current <= end:
        months_list.append((current.year, current.month))
        current += relativedelta(months=1)

    total_months = len(months_list)
    logger.info(f"  Total bulan: {total_months} ({total_months * len(GUARDIAN_QUERIES)} requests estimasi)")

    if dry_run:
        logger.info("[Dry Run] Bulan yang akan diproses:")
        for y, m in months_list:
            print(f"  {y:04d}-{m:02d}")
        return

    # Cek koneksi MinIO
    client = MinIOClient()
    if not client.health_check():
        logger.error("MinIO tidak dapat diakses. Batalkan backfill.")
        return

    # Proses per bulan
    grand_total = 0
    for idx, (year, month) in enumerate(months_list, 1):
        logger.info(f"\n[{idx}/{total_months}] Memproses {year:04d}-{month:02d}...")
        uploaded = backfill_month(year, month)
        grand_total += uploaded
        logger.info(f"  Bulan ini: {uploaded} artikel | Total: {grand_total}")

        # Progress indicator
        pct = (idx / total_months) * 100
        logger.info(f"  Progress: {pct:.1f}% ({idx}/{total_months} bulan)")

        # Kirim alert progress setiap 3 bulan sekali
        if idx % 3 == 0 or idx == total_months:
            try:
                alert_backfill_progress(year, month, uploaded, grand_total, pct)
            except Exception:
                pass

    logger.info("\n" + "=" * 55)
    logger.info(f"  BACKFILL SELESAI")
    logger.info(f"  Total artikel diupload: {grand_total}")
    logger.info(f"  Periode: {start_date} → {end_date}")
    logger.info("=" * 55)

    # Alert backfill selesai
    try:
        alert_backfill_done(grand_total, start_date, end_date)
    except Exception:
        pass

    return grand_total


# ── Entrypoint ────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Guardian Backfill 2021-2025")
    parser.add_argument("--start",   default=BACKFILL_START, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",     default=BACKFILL_END,   help="End date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",    help="Preview saja, tidak upload")
    parser.add_argument("--year",    type=int,               help="Backfill 1 tahun saja, e.g. 2023")
    args = parser.parse_args()

    # Shortcut: backfill 1 tahun saja
    if args.year:
        args.start = f"{args.year}-01-01"
        args.end   = f"{args.year}-12-31"

    run_backfill(
        start_date = args.start,
        end_date   = args.end,
        dry_run    = args.dry_run,
    )