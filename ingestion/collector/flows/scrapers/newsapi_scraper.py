"""
scrapers/newsapi_scraper.py
─────────────────────────────────────────────────────────
Tier 3 — NewsAPI.org
Sumber  : REST API resmi (https://newsapi.org/docs)
Auth    : API Key (env: NEWSAPI_KEY)
Limit   : 100 request/hari (free tier)
Output  : list[dict] dengan schema standar artikel

Query keywords yang dipakai:
  - "euro exchange rate"
  - "ECB European Central Bank"
  - "EUR USD forex"
  - "European economy"
─────────────────────────────────────────────────────────
"""

import os
import time
from datetime import datetime, timedelta, timezone

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# ── Konstanta ────────────────────────────────────────────
NEWSAPI_BASE_URL = "https://newsapi.org/v2/everything"

# Keyword queries sesuai tema: Euro / pasar keuangan Eropa
NEWSAPI_QUERIES = [
    "euro exchange rate EUR USD",
    "ECB European Central Bank monetary policy",
    "European economy inflation",
    "EUR forex market",
]

# Sumber berita keuangan yang relevan
TRUSTED_SOURCES = (
    "reuters.com,ft.com,bloomberg.com,"
    "cnbc.com,wsj.com,bbc.co.uk,dw.com"
)

REQUEST_TIMEOUT = int(os.getenv("SCRAPER_REQUEST_TIMEOUT", 30))
DELAY_SECONDS   = float(os.getenv("SCRAPER_DELAY_SECONDS", 2))


# ── Helper ───────────────────────────────────────────────

def _get_api_key() -> str:
    key = os.getenv("NEWSAPI_KEY", "")
    if not key:
        raise EnvironmentError(
            "NEWSAPI_KEY tidak ditemukan di environment. "
            "Set di file .env: NEWSAPI_KEY=your_key_here"
        )
    return key


def _article_to_schema(article: dict, query: str) -> dict:
    """
    Konversi response NewsAPI ke schema standar:
    {
        title        : str
        url          : str
        published_at : str (ISO 8601)
        source       : str
        source_tier  : int
        category     : str  (query yang dipakai)
        raw_text     : str  (description + content digabung)
        language     : str
        author       : str | None
    }
    """
    # Gabungkan description & content sebagai raw_text
    desc    = article.get("description") or ""
    content = article.get("content") or ""
    # NewsAPI truncate content di 200 char, gabung keduanya
    raw_text = f"{desc} {content}".strip()

    # Normalisasi published_at ke ISO 8601 UTC
    pub_raw = article.get("publishedAt", "")
    try:
        dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
        published_at = dt.isoformat()
    except Exception:
        published_at = datetime.now(timezone.utc).isoformat()

    return {
        "title":        (article.get("title") or "").strip(),
        "url":          article.get("url", ""),
        "published_at": published_at,
        "source":       "newsapi",
        "source_tier":  3,
        "category":     query,
        "raw_text":     raw_text,
        "language":     "en",
        "author":       article.get("author"),
        "provider":     (article.get("source") or {}).get("name", ""),
    }


# ── Fetcher ──────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
)
def _fetch_articles(query: str, api_key: str, from_date: str) -> list[dict]:
    """Fetch satu query dari NewsAPI /v2/everything."""
    params = {
        "q":          query,
        "from":       from_date,
        "sortBy":     "publishedAt",
        "language":   "en",
        "pageSize":   20,        # Hemat kuota: 20 per query
        "apiKey":     api_key,
    }

    logger.debug(f"[NewsAPI] Query: '{query}' | from: {from_date}")

    response = requests.get(
        NEWSAPI_BASE_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code == 401:
        raise PermissionError(
            "NEWSAPI_KEY tidak valid. Cek ulang key kamu."
        )
    if response.status_code == 429:
        raise RuntimeError(
            "NewsAPI rate limit tercapai (100 req/hari untuk free tier)."
        )

    response.raise_for_status()
    data = response.json()

    articles = data.get("articles", [])
    logger.debug(
        f"[NewsAPI] '{query}' → {len(articles)} artikel diterima "
        f"(total tersedia: {data.get('totalResults', '?')})"
    )
    return articles


# ── Scraper utama ─────────────────────────────────────────

def scrape_newsapi(lookback_hours: int = 24) -> list[dict]:
    """
    Scrape NewsAPI untuk semua query keywords.

    Parameters
    ----------
    lookback_hours : int
        Ambil berita N jam ke belakang dari sekarang (default 24)

    Returns
    -------
    list[dict]  — list artikel schema standar, sudah deduplikasi by URL
    """
    try:
        api_key = _get_api_key()
    except EnvironmentError as e:
        logger.error(str(e))
        return []

    # Hitung from_date
    from_dt   = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    from_date = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_articles: list[dict] = []
    seen_urls: set[str]      = set()

    for query in NEWSAPI_QUERIES:
        try:
            raw_articles = _fetch_articles(query, api_key, from_date)

            for raw in raw_articles:
                article = _article_to_schema(raw, query)

                # Skip jika URL duplikat atau tidak ada title
                if not article["title"] or not article["url"]:
                    continue
                if article["url"] in seen_urls:
                    continue

                seen_urls.add(article["url"])
                all_articles.append(article)

            logger.success(
                f"[NewsAPI] '{query}' → "
                f"{len(raw_articles)} artikel, "
                f"total unik sejauh ini: {len(all_articles)}"
            )

        except Exception as e:
            logger.error(f"[NewsAPI] Gagal query '{query}': {e}")
            continue

        time.sleep(DELAY_SECONDS)  # Jaga rate limit

    logger.info(f"[NewsAPI] Total artikel unik: {len(all_articles)}")
    return all_articles


# ── Entrypoint standalone ─────────────────────────────────

if __name__ == "__main__":
    articles = scrape_newsapi(lookback_hours=24)
    for a in articles[:3]:
        print(f"  [{a['provider']}] {a['title'][:80]}")
        print(f"   → {a['url']}")
        print(f"   → {a['published_at']}\n")