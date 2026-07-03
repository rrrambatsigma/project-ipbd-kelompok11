"""
scrapers/ecb_scraper.py
ECB RSS feeds - URL yang masih aktif per 2025
"""

import os
import time
from datetime import datetime, timezone

import feedparser
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

ECB_RSS_FEEDS = {
    "economic_bulletin": "https://www.ecb.europa.eu/rss/pub.html",
    "press_releases":    "https://www.ecb.europa.eu/rss/press.html",
    "speeches":          "https://www.ecb.europa.eu/rss/intpol.html",
    "working_papers":    "https://www.ecb.europa.eu/rss/wppub.html",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_TIMEOUT = int(os.getenv("SCRAPER_REQUEST_TIMEOUT", 30))
DELAY_SECONDS   = float(os.getenv("SCRAPER_DELAY_SECONDS", 2))


def _parse_published_at(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


def _extract_raw_text(entry) -> str:
    raw_html = ""
    if hasattr(entry, "content") and entry.content:
        raw_html = entry.content[0].get("value", "")
    elif hasattr(entry, "summary"):
        raw_html = entry.summary
    if raw_html:
        soup = BeautifulSoup(raw_html, "lxml")
        return soup.get_text(separator=" ", strip=True)
    return entry.get("title", "")


def _entry_to_article(entry: dict, feed_category: str) -> dict:
    return {
        "title":        entry.get("title", "").strip(),
        "url":          entry.get("link", ""),
        "published_at": _parse_published_at(entry),
        "source":       "ecb",
        "source_tier":  1,
        "category":     feed_category,
        "raw_text":     _extract_raw_text(entry),
        "language":     "en",
    }


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=8))
def _fetch_feed(feed_url: str) -> feedparser.FeedParserDict:
    logger.debug(f"[ECB] Fetching: {feed_url}")
    try:
        resp = requests.get(feed_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except Exception:
        return feedparser.parse(feed_url, request_headers=HEADERS)


def scrape_ecb(max_articles_per_feed: int = 20) -> list[dict]:
    all_articles: list[dict] = []

    for category, feed_url in ECB_RSS_FEEDS.items():
        try:
            logger.info(f"[ECB] Scraping feed: {category}")
            feed    = _fetch_feed(feed_url)
            entries = feed.entries[:max_articles_per_feed]

            if not entries:
                logger.warning(f"[ECB] {category} → 0 entri")
                time.sleep(DELAY_SECONDS)
                continue

            articles = [_entry_to_article(e, category) for e in entries]
            articles = [a for a in articles if a["title"] and a["url"]]
            all_articles.extend(articles)
            logger.success(f"[ECB] {category} → {len(articles)} artikel")
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            logger.error(f"[ECB] Gagal scrape {category}: {e}")
            continue

    logger.info(f"[ECB] Total: {len(all_articles)} artikel")
    return all_articles


if __name__ == "__main__":
    articles = scrape_ecb()
    for a in articles[:3]:
        print(f"  [{a['category']}] {a['title'][:80]}")
