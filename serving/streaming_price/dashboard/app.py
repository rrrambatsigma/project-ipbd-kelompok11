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

SILVER_PATH = f"{MINIO_BUCKET_MARKET}/eurusd/silver"
GOLD_PATH = f"{MINIO_BUCKET_MARKET}/eurusd/gold_window_1m"


st.set_page_config(
    page_title="EUR/USD Streaming Dashboard",
    layout="wide",
)


@st.cache_data(ttl=10)
def read_parquet_dataset(path: str) -> pd.DataFrame:
    fs = S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=MINIO_ENDPOINT,
        scheme="http",
    )

    try:
        dataset = ds.dataset(
            path,
            filesystem=fs,
            format="parquet",
            partitioning="hive",
        )
        table = dataset.to_table()
        df = table.to_pandas()
        return df
    except Exception as e:
        st.warning(f"Could not read {path}: {e}")
        return pd.DataFrame()


def prepare_silver(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "event_ts" in df.columns:
        df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce")
    elif "event_time" in df.columns:
        df["event_ts"] = pd.to_datetime(df["event_time"], errors="coerce")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["event_ts", "price"])
    df = df.sort_values("event_ts")

    df["return"] = df["price"].pct_change()
    df["ma_10"] = df["price"].rolling(10).mean()
    df["volatility_10"] = df["return"].rolling(10).std()

    return df


def prepare_gold(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    for col in ["window_start", "window_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

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

    return df


st.title("EUR/USD Streaming Processing Dashboard")
st.caption("yfinance WebSocket → Kafka → Spark Structured Streaming → MinIO → Streamlit")

silver_df = prepare_silver(read_parquet_dataset(SILVER_PATH))
gold_df = prepare_gold(read_parquet_dataset(GOLD_PATH))

if silver_df.empty:
    st.error("No silver EUR/USD data found yet. Let the producer and processor run for 1–2 minutes.")
    st.stop()

latest = silver_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Latest EUR/USD", f"{latest['price']:.6f}")

with col2:
    source = latest.get("source", "-")
    st.metric("Source", str(source))

with col3:
    tick_count = len(silver_df)
    st.metric("Total Ticks", f"{tick_count:,}")

with col4:
    latest_time = latest.get("event_ts")
    if pd.notna(latest_time):
        st.metric("Latest Event Time", latest_time.strftime("%H:%M:%S UTC"))
    else:
        st.metric("Latest Event Time", "-")

st.divider()

left, right = st.columns([2, 1])

with left:
    st.subheader("Live EUR/USD Tick Price")

    chart_df = silver_df.tail(300).set_index("event_ts")[["price", "ma_10"]]
    st.line_chart(chart_df)

with right:
    st.subheader("Recent Ticks")
    display_cols = [
        col for col in [
            "event_ts",
            "symbol",
            "source",
            "price",
            "quality_status",
        ]
        if col in silver_df.columns
    ]
    st.dataframe(
        silver_df.tail(20)[display_cols].sort_values("event_ts", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Return")
    if "return" in silver_df.columns:
        st.line_chart(silver_df.tail(300).set_index("event_ts")[["return"]])

with col_b:
    st.subheader("Rolling Volatility")
    if "volatility_10" in silver_df.columns:
        st.line_chart(silver_df.tail(300).set_index("event_ts")[["volatility_10"]])

st.divider()

st.subheader("1-Minute Window Aggregation")

if gold_df.empty:
    st.info("Gold window data not found yet. This can appear later because Spark waits for 1-minute windows.")
else:
    gold_display = gold_df.tail(100)

    if "window_start" in gold_display.columns and "close_price" in gold_display.columns:
        st.line_chart(gold_display.set_index("window_start")[["close_price", "avg_price"]])

    st.dataframe(
        gold_display.sort_values("window_start", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.caption(f"Last dashboard refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
