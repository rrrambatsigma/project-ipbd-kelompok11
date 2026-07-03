"""
scrapers/gdelt_scraper.py
─────────────────────────────────────────────────────────
Tier 2 — GDELT RSS Feed (bukan DOC API, tidak ada rate limit)
Sumber  : GDELT RSS via query URL
URL     : https://api.gdeltproject.org/api/v2/doc/doc?...&format=rss
Fallback: The Guardian Open API (gratis, 500 req/hari)
─────────────────────────────────────────────────────────
"""

import os
import time
from datetime import datetime, timezone

import feedparser
import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── GDELT RSS (tidak kena rate limit seperti JSON API) ───
GDELT_RSS_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

GDELT_RSS_QUERIES = [
    "euro exchange rate EUR USD",
    "ECB European Central Bank",
    "eurozone inflation interest rate",
]

# ── Guardian API (fallback, gratis tanpa key untuk basic) ─
GUARDIAN_API_URL = "https://content.guardianapis.com/search"
GUARDIAN_API_KEY = os.getenv("GUARDIAN_API_KEY", "test")  # "test" = anonymous, 12 req/hari
GUARDIAN_QUERIES = [
    "euro exchange rate",
    "ECB European Central Bank",
]

REQUEST_TIMEOUT = int(os.getenv("SCRAPER_REQUEST_TIMEOUT", 30))
DELAY_SECONDS   = float(os.getenv("SCRAPER_DELAY_SECONDS", 5))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0)",
    "Accept":     "application/rss+xml, application/xml, text/xml",
}


# ── Helper ───────────────────────────────────────────────

def _parse_published_at(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ── GDELT RSS Scraper ─────────────────────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=5, max=20))
def _fetch_gdelt_rss(query: str, max_records: int = 25) -> list[dict]:
    """Fetch via RSS format — jauh lebih ringan dari JSON API."""
    params = {
        "query":      query,
        "mode":       "ArtList",
        "maxrecords": max_records,
        "sort":       "DateDesc",
        "format":     "rss",
        "timespan":   "24h",
    }
    resp = requests.get(
        GDELT_RSS_BASE,
        params=params,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()

    # Parse RSS response
    feed     = feedparser.parse(resp.content)
    articles = []

    for entry in feed.entries:
        title = (entry.get("title") or "").strip()
        url   = entry.get("link", "")
        if not title or not url:
            continue

        articles.append({
            "title":        title,
            "url":          url,
            "published_at": _parse_published_at(entry),
            "source":       "gdelt",
            "source_tier":  2,
            "category":     query[:50],
            "raw_text":     title,
            "language":     "en",
            "provider":     entry.get("source", {}).get("href", ""),
            "tone":         None,
        })

    return articles


def scrape_gdelt_rss(max_per_query: int = 25) -> list[dict]:
    all_articles: list[dict] = []
    seen_urls: set[str]      = set()

    for query in GDELT_RSS_QUERIES:
        try:
            logger.info(f"[GDELT-RSS] Query: {query}")
            articles = _fetch_gdelt_rss(query, max_per_query)

            for a in articles:
                if a["url"] in seen_urls:
                    continue
                seen_urls.add(a["url"])
                all_articles.append(a)

            logger.success(f"[GDELT-RSS] '{query}' → {len(articles)} artikel")
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            logger.error(f"[GDELT-RSS] Gagal '{query}': {e}")
            continue

    return all_articles


# ── Guardian API Scraper (fallback) ──────────────────────

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=3, max=10))
def _fetch_guardian(query: str, page_size: int = 20) -> list[dict]:
    params = {
        "q":          query,
        "api-key":    GUARDIAN_API_KEY,
        "page-size":  page_size,
        "order-by":   "newest",
        "show-fields": "trailText,bodyText",
        "section":    "business|world|money",
    }
    resp = requests.get(GUARDIAN_API_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", {}).get("results", [])


def scrape_guardian(max_per_query: int = 20) -> list[dict]:
    all_articles: list[dict] = []
    seen_urls: set[str]      = set()

    for query in GUARDIAN_QUERIES:
        try:
            logger.info(f"[Guardian] Query: {query}")
            results = _fetch_guardian(query, max_per_query)

            for r in results:
                url   = r.get("webUrl", "")
                title = r.get("webTitle", "").strip()
                if not url or not title or url in seen_urls:
                    continue

                fields   = r.get("fields", {})
                raw_text = fields.get("bodyText") or fields.get("trailText") or title

                seen_urls.add(url)
                all_articles.append({
                    "title":        title,
                    "url":          url,
                    "published_at": r.get("webPublicationDate", datetime.now(timezone.utc).isoformat()),
                    "source":       "guardian",
                    "source_tier":  2,
                    "category":     query,
                    "raw_text":     raw_text[:2000],
                    "language":     "en",
                    "provider":     "theguardian.com",
                    "tone":         None,
                })

            logger.success(f"[Guardian] '{query}' → {len(results)} artikel")
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            logger.error(f"[Guardian] Gagal '{query}': {e}")
            continue

    return all_articles


# ── Main scrape_gdelt (kombinasi RSS + Guardian) ──────────

def scrape_gdelt(max_per_query: int = 25) -> list[dict]:
    """
    Scrape GDELT RSS + Guardian API.
    Tidak memerlukan API key khusus.
    """
    all_articles: list[dict] = []

    # Coba GDELT RSS dulu
    logger.info("[GDELT] Mencoba GDELT RSS feed...")
    gdelt_articles = scrape_gdelt_rss(max_per_query)
    all_articles.extend(gdelt_articles)
    logger.info(f"[GDELT-RSS] {len(gdelt_articles)} artikel")

    # Selalu tambah Guardian (reliable, gratis)
    logger.info("[GDELT] Menambah Guardian API...")
    guardian_articles = scrape_guardian(max_per_query)
    all_articles.extend(guardian_articles)
    logger.info(f"[Guardian] {len(guardian_articles)} artikel")

    logger.info(f"[GDELT+Guardian] Total: {len(all_articles)} artikel")
    return all_articles


if __name__ == "__main__":
    articles = scrape_gdelt()
    for a in articles[:5]:
        print(f"  [{a['source']}|{a['provider'][:30]}] {a['title'][:70]}")
        print(f"   → {a['published_at']}\n")