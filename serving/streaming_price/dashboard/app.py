import os
from datetime import datetime

import pandas as pd
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem
import streamlit as st


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

MINIO_BUCKET_MARKET = os.getenv("MINIO_BUCKET_MARKET", "market-processed")
MINIO_BUCKET_NEWS_PROCESSED = os.getenv("MINIO_BUCKET_NEWS_PROCESSED", "news-processed")

SILVER_PATH = f"{MINIO_BUCKET_MARKET}/eurusd/silver"
GOLD_PATH = f"{MINIO_BUCKET_MARKET}/eurusd/gold_window_1m"
SENTIMENT_SESSION_PATH = f"{MINIO_BUCKET_NEWS_PROCESSED}/sentiment/aggregated/sentiment_by_session"


st.set_page_config(
    page_title="EUR/USD Market Flow Dashboard",
    layout="wide",
)


def get_s3_fs() -> S3FileSystem:
    return S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=MINIO_ENDPOINT,
        scheme="http",
    )


@st.cache_data(ttl=10)
def read_parquet_dataset(path: str) -> pd.DataFrame:
    fs = get_s3_fs()

    try:
        dataset = ds.dataset(
            path,
            filesystem=fs,
            format="parquet",
            partitioning="hive",
        )
        table = dataset.to_table()
        return table.to_pandas()
    except Exception as e:
        st.warning(f"Could not read `{path}`: {e}")
        return pd.DataFrame()


def assign_session_tag(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return "unknown"

    hour = ts.hour

    if hour >= 23 or hour < 2:
        return "pre_market"
    if hour < 6:
        return "open"
    if hour < 10:
        return "mid"
    if hour < 14:
        return "pre_close"
    return "overlap"


def prepare_silver(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "event_ts" in df.columns:
        df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce", utc=True)
    elif "event_time" in df.columns:
        df["event_ts"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["event_ts", "price"])
    df = df.sort_values("event_ts")

    df["date"] = df["event_ts"].dt.date.astype(str)
    df["session_tag"] = df["event_ts"].apply(assign_session_tag)

    df["return"] = df["price"].pct_change()
    df["ma_10"] = df["price"].rolling(10).mean()
    df["volatility_10"] = df["return"].rolling(10).std()

    return df


def prepare_gold(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for col in ["window_start", "window_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    numeric_cols = [
        "tick_count",
        "open_price",
        "close_price",
        "avg_price",
        "min_price",
        "max_price",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "window_start" in df.columns:
        df = df.dropna(subset=["window_start"]).sort_values("window_start")
        df["date"] = df["window_start"].dt.date.astype(str)
        df["session_tag"] = df["window_start"].apply(assign_session_tag)

    return df


def prepare_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)

    numeric_cols = [
        "avg_compound",
        "avg_positive",
        "avg_negative",
        "avg_neutral",
        "article_count",
        "std_compound",
        "positive_count",
        "negative_count",
        "neutral_count",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "session_tag" in df.columns:
        df["session_tag"] = df["session_tag"].astype(str)

    return df


def make_session_price_features(silver_df: pd.DataFrame) -> pd.DataFrame:
    if silver_df.empty:
        return pd.DataFrame()

    session_df = (
        silver_df
        .dropna(subset=["date", "session_tag", "price"])
        .groupby(["date", "session_tag"], as_index=False)
        .agg(
            tick_count=("price", "count"),
            open_price=("price", "first"),
            close_price=("price", "last"),
            avg_price=("price", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max"),
            volatility=("return", "std"),
        )
    )

    session_df["session_return"] = (
        session_df["close_price"] - session_df["open_price"]
    ) / session_df["open_price"]

    return session_df


def join_price_sentiment(price_session_df: pd.DataFrame, sentiment_df: pd.DataFrame) -> pd.DataFrame:
    if price_session_df.empty or sentiment_df.empty:
        return pd.DataFrame()

    joined = price_session_df.merge(
        sentiment_df,
        on=["date", "session_tag"],
        how="left",
        suffixes=("_price", "_sentiment"),
    )

    return joined.sort_values(["date", "session_tag"])


st.title("EUR/USD Market Flow Dashboard")
st.caption("yfinance WebSocket → Kafka → Spark Structured Streaming → MinIO → Sentiment Join")

silver_df = prepare_silver(read_parquet_dataset(SILVER_PATH))
gold_df = prepare_gold(read_parquet_dataset(GOLD_PATH))
sentiment_df = prepare_sentiment(read_parquet_dataset(SENTIMENT_SESSION_PATH))

if silver_df.empty:
    st.error("No EUR/USD silver data found yet. Keep producer and processor running for 1–2 minutes.")
    st.stop()

price_session_df = make_session_price_features(silver_df)
joined_df = join_price_sentiment(price_session_df, sentiment_df)

latest = silver_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Latest EUR/USD", f"{latest['price']:.6f}")

with col2:
    source = latest.get("source", "-")
    st.metric("Source", str(source))

with col3:
    st.metric("Total Ticks", f"{len(silver_df):,}")

with col4:
    latest_time = latest.get("event_ts")
    if pd.notna(latest_time):
        st.metric("Latest Event Time", latest_time.strftime("%H:%M:%S UTC"))
    else:
        st.metric("Latest Event Time", "-")

st.divider()

tab_live, tab_gold, tab_sentiment, tab_joined = st.tabs([
    "Live EUR/USD",
    "1-Minute Gold",
    "News Sentiment",
    "Joined Market Flow",
])

with tab_live:
    left, right = st.columns([2, 1])

    with left:
        st.subheader("Live EUR/USD Tick Price")
        chart_cols = ["price"]
        if "ma_10" in silver_df.columns:
            chart_cols.append("ma_10")

        st.line_chart(
            silver_df.tail(500).set_index("event_ts")[chart_cols]
        )

    with right:
        st.subheader("Recent Ticks")
        display_cols = [
            col for col in [
                "event_ts",
                "symbol",
                "source",
                "price",
                "session_tag",
                "quality_status",
            ]
            if col in silver_df.columns
        ]
        st.dataframe(
            silver_df.tail(30)[display_cols].sort_values("event_ts", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Tick Return")
        if "return" in silver_df.columns:
            st.line_chart(silver_df.tail(500).set_index("event_ts")[["return"]])

    with col_b:
        st.subheader("Rolling Volatility")
        if "volatility_10" in silver_df.columns:
            st.line_chart(silver_df.tail(500).set_index("event_ts")[["volatility_10"]])

with tab_gold:
    st.subheader("1-Minute Window Aggregation")

    if gold_df.empty:
        st.info("Gold window data not found yet. Spark may still be waiting for 1-minute windows.")
    else:
        if "window_start" in gold_df.columns and "close_price" in gold_df.columns:
            chart_cols = [c for c in ["close_price", "avg_price"] if c in gold_df.columns]
            st.line_chart(gold_df.tail(200).set_index("window_start")[chart_cols])

        st.dataframe(
            gold_df.tail(100).sort_values("window_start", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

with tab_sentiment:
    st.subheader("Aggregated News Sentiment by Market Session")

    if sentiment_df.empty:
        st.warning(
            "No sentiment aggregation found yet. Run Rambat's preprocessing first, "
            "or make sure this path exists: "
            f"`{SENTIMENT_SESSION_PATH}`"
        )
    else:
        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric("Sentiment Rows", f"{len(sentiment_df):,}")

        with c2:
            if "article_count" in sentiment_df.columns:
                st.metric("Total Articles", f"{int(sentiment_df['article_count'].sum()):,}")

        with c3:
            if "avg_compound" in sentiment_df.columns:
                st.metric("Avg Compound", f"{sentiment_df['avg_compound'].mean():.4f}")

        if "date" in sentiment_df.columns and "avg_compound" in sentiment_df.columns:
            sentiment_chart = (
                sentiment_df
                .dropna(subset=["date", "avg_compound"])
                .copy()
            )
            sentiment_chart["date_session"] = (
                sentiment_chart["date"].astype(str)
                + " "
                + sentiment_chart["session_tag"].astype(str)
            )
            st.line_chart(sentiment_chart.set_index("date_session")[["avg_compound"]])

        st.dataframe(
            sentiment_df.tail(100),
            use_container_width=True,
            hide_index=True,
        )

with tab_joined:
    st.subheader("Joined Market Flow: EUR/USD + Sentiment")

    if joined_df.empty:
        st.warning(
            "Joined dataset is empty. This usually means sentiment data is missing "
            "or dates/session_tag do not overlap yet."
        )

        st.write("Price session rows:")
        st.dataframe(price_session_df.tail(30), use_container_width=True, hide_index=True)

        st.write("Sentiment rows:")
        st.dataframe(sentiment_df.tail(30), use_container_width=True, hide_index=True)
    else:
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric("Joined Rows", f"{len(joined_df):,}")

        with c2:
            latest_ret = joined_df["session_return"].dropna()
            st.metric(
                "Latest Session Return",
                f"{latest_ret.iloc[-1]:.5f}" if not latest_ret.empty else "-"
            )

        with c3:
            if "avg_compound" in joined_df.columns:
                latest_sent = joined_df["avg_compound"].dropna()
                st.metric(
                    "Latest Sentiment",
                    f"{latest_sent.iloc[-1]:.4f}" if not latest_sent.empty else "-"
                )

        with c4:
            if "negative_count" in joined_df.columns:
                latest_neg = joined_df["negative_count"].dropna()
                st.metric(
                    "Latest Negative News",
                    f"{int(latest_neg.iloc[-1])}" if not latest_neg.empty else "-"
                )

        chart_df = joined_df.copy()
        chart_df["date_session"] = (
            chart_df["date"].astype(str)
            + " "
            + chart_df["session_tag"].astype(str)
        )

        plot_cols = [
            col for col in [
                "session_return",
                "avg_compound",
                "negative_count",
                "article_count",
            ]
            if col in chart_df.columns
        ]

        if plot_cols:
            st.line_chart(chart_df.set_index("date_session")[plot_cols])

        st.dataframe(
            joined_df.sort_values(["date", "session_tag"], ascending=False),
            use_container_width=True,
            hide_index=True,
        )

st.caption(f"Last dashboard refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
