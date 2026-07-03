import os
import requests
import psycopg2
from datetime import datetime

NEWS_API = os.getenv("NEWS_API", "http://100.118.244.91:8000").rstrip("/")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "kurs_eur_db")
PG_USER = os.getenv("PG_USER", "kursadmin")
PG_PASSWORD = os.getenv("PG_PASSWORD", "kursadmin")


def get_first(row, names, default=None):
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return default


def to_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def parse_date(v):
    if not v:
        return None
    s = str(v).strip()
    try:
        return str(datetime.fromisoformat(s.replace("Z", "+00:00")).date())
    except Exception:
        return s[:10]


def fetch_news_rows():
    url = f"{NEWS_API}/api/sentiment/daily"
    print(f"[INFO] Fetching News sentiment: {url}")

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Could not fetch News API: {e}")
        return []

    if isinstance(data, dict):
        rows = data.get("data", data.get("items", []))
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    cleaned = []

    for row in rows:
        if not isinstance(row, dict):
            continue

        trade_date = parse_date(get_first(row, ["trade_date", "date", "tanggal", "time"]))
        if not trade_date:
            continue

        positive_count = to_int(get_first(row, ["positive_count", "positif", "positive", "pos_count"]))
        negative_count = to_int(get_first(row, ["negative_count", "negatif", "negative", "neg_count"]))
        neutral_count = to_int(get_first(row, ["neutral_count", "netral", "neutral", "neu_count"]))

        total_news = to_int(
            get_first(row, ["total_news", "article_count", "jumlah_artikel", "total", "count"]),
            positive_count + negative_count + neutral_count,
        )

        if total_news <= 0:
            total_news = positive_count + negative_count + neutral_count

        cleaned.append(
            (
                trade_date,
                positive_count,
                negative_count,
                neutral_count,
                total_news,
            )
        )

    print(f"[INFO] News rows parsed: {len(cleaned)}")
    return cleaned


def main():
    news_rows = fetch_news_rows()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS sentiment_daily;
                CREATE TABLE sentiment_daily (
                    trade_date DATE PRIMARY KEY,
                    positive_count INTEGER DEFAULT 0,
                    negative_count INTEGER DEFAULT 0,
                    neutral_count INTEGER DEFAULT 0,
                    total_news INTEGER DEFAULT 0
                );
            """)

            if news_rows:
                cur.executemany("""
                    INSERT INTO sentiment_daily (
                        trade_date,
                        positive_count,
                        negative_count,
                        neutral_count,
                        total_news
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date)
                    DO UPDATE SET
                        positive_count = EXCLUDED.positive_count,
                        negative_count = EXCLUDED.negative_count,
                        neutral_count = EXCLUDED.neutral_count,
                        total_news = EXCLUDED.total_news;
                """, news_rows)

            # Ensure every commodity date has at least one sentiment row,
            # so the model can train even when news dates do not fully overlap.
            cur.execute("""
                INSERT INTO sentiment_daily (
                    trade_date,
                    positive_count,
                    negative_count,
                    neutral_count,
                    total_news
                )
                SELECT DISTINCT
                    trade_date::date,
                    0,
                    0,
                    0,
                    0
                FROM commodity_daily
                ON CONFLICT (trade_date) DO NOTHING;
            """)

            cur.execute("SELECT COUNT(*) FROM sentiment_daily;")
            count = cur.fetchone()[0]

            cur.execute("""
                SELECT *
                FROM sentiment_daily
                ORDER BY trade_date DESC
                LIMIT 10;
            """)
            sample = cur.fetchall()

    print(f"[OK] sentiment_daily ready. Rows: {count}")
    print("[SAMPLE]")
    for row in sample:
        print(row)


if __name__ == "__main__":
    main()
