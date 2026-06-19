"""
scrapers/reuters_scraper.py
─────────────────────────────────────────────────────────
Tier 2 — Reuters
Sumber  : RSS Feed publik Reuters (Economics & Markets)
Metode  : feedparser + BeautifulSoup (no Selenium diperlukan)
         Selenium digunakan hanya jika RSS tidak cukup detail
Output  : list[dict] dengan schema standar artikel

RSS Reuters yang relevan:
  - Business & Finance
  - Markets
  - European Economics
─────────────────────────────────────────────────────────
"""

import os
import time
from datetime import datetime, timezone

import feedparser
import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Konstanta ────────────────────────────────────────────
REUTERS_RSS_FEEDS = {
    "markets":        "https://feeds.reuters.com/reuters/businessNews",
    "top_news":       "https://feeds.reuters.com/reuters/topNews",
    "euro_markets":   "https://feeds.reuters.com/reuters/UKFocus",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Keyword filter — hanya ambil artikel yang relevan dengan EUR/ekonomi Eropa
EURO_KEYWORDS = [
    "euro", "eur", "ecb", "european central bank",
    "eurozone", "europe", "inflation", "interest rate",
    "forex", "exchange rate", "monetary policy",
    "germany", "france", "italy", "european commission",
]

REQUEST_TIMEOUT = int(os.getenv("SCRAPER_REQUEST_TIMEOUT", 30))
DELAY_SECONDS   = float(os.getenv("SCRAPER_DELAY_SECONDS", 2))


# ── Helper ───────────────────────────────────────────────

def _is_relevant(title: str, summary: str) -> bool:
    """
    Filter artikel: hanya ambil yang mengandung keyword EUR/Eropa.
    Case-insensitive.
    """
    combined = f"{title} {summary}".lower()
    return any(kw in combined for kw in EURO_KEYWORDS)


def _parse_published_at(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


def _clean_text(raw_html: str) -> str:
    """Strip HTML tags, kembalikan plain text."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "lxml")
    return soup.get_text(separator=" ", strip=True)


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=5),
)
def _fetch_article_body(url: str) -> str:
    """
    Opsional: fetch body artikel dari URL untuk raw_text lebih lengkap.
    Hanya digunakan jika summary RSS terlalu pendek (< 100 char).
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")

        # Reuters menyimpan artikel di <div class="article-body__content">
        # atau <div data-testid="paragraph">
        paragraphs = soup.find_all(
            "p",
            attrs={"data-testid": "paragraph"}
        )
        if paragraphs:
            return " ".join(p.get_text(strip=True) for p in paragraphs)

        # Fallback: ambil semua <p> di main content
        main = soup.find("article") or soup.find("main") or soup
        return " ".join(
            p.get_text(strip=True)
            for p in main.find_all("p")
            if len(p.get_text(strip=True)) > 30
        )

    except Exception as e:
        logger.debug(f"[Reuters] Gagal fetch body {url}: {e}")
        return ""


def _entry_to_article(entry, category: str) -> dict | None:
    """
    Konversi RSS entry ke schema standar.
    Return None jika tidak relevan.
    """
    title   = (entry.get("title") or "").strip()
    summary = _clean_text(entry.get("summary", ""))
    url     = entry.get("link", "")

    # Filter relevansi
    if not _is_relevant(title, summary):
        return None

    # Jika summary terlalu pendek, fetch body
    raw_text = summary
    if len(summary) < 100 and url:
        logger.debug(f"[Reuters] Summary pendek, fetch body: {url[:60]}...")
        raw_text = _fetch_article_body(url) or summary
        time.sleep(1)  # Delay setelah fetch body

    return {
        "title":        title,
        "url":          url,
        "published_at": _parse_published_at(entry),
        "source":       "reuters",
        "source_tier":  2,
        "category":     category,
        "raw_text":     raw_text,
        "language":     "en",
    }


# ── Scraper utama ─────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def _fetch_feed(feed_url: str) -> feedparser.FeedParserDict:
    return feedparser.parse(feed_url, request_headers=HEADERS)


def scrape_reuters(max_articles_per_feed: int = 30) -> list[dict]:
    """
    Scrape Reuters RSS feeds, filter artikel terkait EUR/Eropa.

    Parameters
    ----------
    max_articles_per_feed : int
        Batas entri yang diproses per feed (default 30)

    Returns
    -------
    list[dict]  — list artikel schema standar
    """
    all_articles: list[dict] = []
    seen_urls: set[str]      = set()

    for category, feed_url in REUTERS_RSS_FEEDS.items():
        try:
            logger.info(f"[Reuters] Scraping feed: {category}")
            feed    = _fetch_feed(feed_url)
            entries = feed.entries[:max_articles_per_feed]

            count = 0
            for entry in entries:
                article = _entry_to_article(entry, category)
                if article is None:
                    continue
                if article["url"] in seen_urls:
                    continue

                seen_urls.add(article["url"])
                all_articles.append(article)
                count += 1

            logger.success(
                f"[Reuters] {category} → {count} artikel relevan "
                f"dari {len(entries)} entri"
            )

            time.sleep(DELAY_SECONDS)

        except Exception as e:
            logger.error(f"[Reuters] Gagal scrape feed {category}: {e}")
            continue

    logger.info(f"[Reuters] Total artikel relevan: {len(all_articles)}")
    return all_articles


# ── Entrypoint standalone ─────────────────────────────────

if __name__ == "__main__":
    articles = scrape_reuters()
    for a in articles[:3]:
        print(f"  [{a['category']}] {a['title'][:80]}")
        print(f"   → {a['url']}")
        print(f"   → {a['published_at']}\n")