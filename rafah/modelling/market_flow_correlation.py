import os
import json
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split


load_dotenv(override=True)

KURS_API = os.getenv("KURS_API", "http://100.118.244.91:8002")
NEWS_API = os.getenv("NEWS_API", "http://100.118.244.91:8000")
COMMODITY_API = os.getenv("COMMODITY_API", "http://100.92.242.101:8001")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_MODELLING") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

OUT_DIR = Path("rafah/modelling/market_flow_outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def get_json(url):
    print(f"[GET] {url}")
    r = requests.get(url, timeout=45)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict):
        return data.get("data", data)
    return data


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram token/chat id kosong. Skip notification.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    try:
        requests.post(url, json=payload, timeout=15).raise_for_status()
        print("[OK] Telegram notification sent.")
    except Exception as e:
        print(f"[WARN] Telegram failed: {e}")


def load_kurs():
    rows = get_json(f"{KURS_API}/kurs/daily")
    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("Kurs API returned empty data.")

    date_col = "trade_date" if "trade_date" in df.columns else "date"
    df["date"] = pd.to_datetime(df[date_col]).dt.date.astype(str)

    df["kurs_close"] = pd.to_numeric(
        df.get("kurs_close", df.get("close", df.get("close_price"))),
        errors="coerce"
    )

    df["kurs_change_pct"] = pd.to_numeric(
        df.get("kurs_change_pct", df.get("change_pct", df.get("price_change_pct"))),
        errors="coerce"
    )

    return df[["date", "kurs_close", "kurs_change_pct"]].dropna()


def load_news():
    rows = get_json(f"{NEWS_API}/api/sentiment/daily")
    df = pd.DataFrame(rows)

    if df.empty:
        raise RuntimeError("News API returned empty data.")

    date_col = (
        "tanggal" if "tanggal" in df.columns
        else "date" if "date" in df.columns
        else "trade_date"
    )
    df["date"] = pd.to_datetime(df[date_col]).dt.date.astype(str)

    # Support several possible column naming styles from News API.
    if "positif" in df.columns and "positive_count" not in df.columns:
        df["positive_count"] = df["positif"]
    if "negatif" in df.columns and "negative_count" not in df.columns:
        df["negative_count"] = df["negatif"]
    if "avg_compound" in df.columns and "net_sentiment" not in df.columns:
        df["net_sentiment"] = df["avg_compound"]
    if "sentiment" in df.columns and "net_sentiment" not in df.columns:
        df["net_sentiment"] = df["sentiment"]
    if "article_count" in df.columns and "total_news" not in df.columns:
        df["total_news"] = df["article_count"]

    for col in [
        "positive_count",
        "negative_count",
        "net_sentiment",
        "avg_pos_prob",
        "avg_neg_prob",
        "total_news",
    ]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df[
        [
            "date",
            "positive_count",
            "negative_count",
            "net_sentiment",
            "avg_pos_prob",
            "avg_neg_prob",
            "total_news",
        ]
    ]


def load_commodity():
    # Commodity API max limit is 500, so fetch each ticker separately.
    symbols = ["GLD", "BTC-USD", "SI=F"]
    all_rows = []

    for symbol in symbols:
        rows = get_json(f"{COMMODITY_API}/commodity/daily?symbol={symbol}&limit=500")
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)

    if df.empty:
        raise RuntimeError("Commodity API returned empty data.")

    df["date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
    df["close"] = pd.to_numeric(df.get("close", df.get("close_price")), errors="coerce")
    df["change_pct"] = pd.to_numeric(df.get("change_pct", df.get("price_change_pct")), errors="coerce")
    df["volatility"] = pd.to_numeric(df.get("volatility"), errors="coerce")

    pivot = df.pivot_table(
        index="date",
        columns="symbol",
        values=["close", "change_pct", "volatility"],
        aggfunc="last"
    )

    pivot.columns = [
        f"{metric}_{symbol.replace('-', '_').replace('=', '')}"
        for metric, symbol in pivot.columns
    ]

    return pivot.reset_index()


def main():
    print("=" * 70)
    print("MARKET FLOW CORRELATION MODELLING")
    print("X = News/Sentiment + Commodity")
    print("Y = Kurs EUR/USD change")
    print("=" * 70)
    print(f"KURS_API      = {KURS_API}")
    print(f"NEWS_API      = {NEWS_API}")
    print(f"COMMODITY_API = {COMMODITY_API}")
    print("=" * 70)

    print("[INFO] Loading Kurs...")
    kurs = load_kurs()
    print(f"[INFO] Kurs rows: {len(kurs)}")

    print("[INFO] Loading News...")
    news = load_news()
    print(f"[INFO] News rows: {len(news)}")

    print("[INFO] Loading Commodity...")
    commodity = load_commodity()
    print(f"[INFO] Commodity rows: {len(commodity)}")

    df = (
        kurs
        .merge(news, on="date", how="left")
        .merge(commodity, on="date", how="left")
        .sort_values("date")
        .fillna(0)
    )

    feature_cols = [c for c in df.columns if c not in ["date", "kurs_close", "kurs_change_pct"]]
    target_col = "kurs_change_pct"

    corr = (
        df[feature_cols + [target_col]]
        .corr(numeric_only=True)[target_col]
        .dropna()
        .sort_values(ascending=False)
    )

    joined_path = OUT_DIR / "market_flow_joined_dataset.csv"
    corr_path = OUT_DIR / "correlation_vs_kurs_change.csv"
    report_path = OUT_DIR / "market_flow_model_report.json"

    df.to_csv(joined_path, index=False)
    corr.to_csv(corr_path, header=["pearson_r"])

    model_metrics = {
        "created_at": datetime.now().isoformat(),
        "rows_joined": int(len(df)),
        "features": feature_cols,
        "target": target_col,
        "top_correlation": corr.head(10).to_dict(),
    }

    if len(df) >= 20 and len(feature_cols) > 0:
        X = df[feature_cols]
        y = df[target_col]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.25,
            shuffle=False
        )

        model = RandomForestRegressor(
            n_estimators=200,
            max_depth=6,
            random_state=42
        )

        model.fit(X_train, y_train)
        pred = model.predict(X_test)

        model_metrics.update({
            "model_type": "RandomForestRegressor",
            "r2": float(r2_score(y_test, pred)),
            "mae": float(mean_absolute_error(y_test, pred)),
            "note": "Model regresi sederhana untuk membaca pengaruh relatif fitur X terhadap perubahan kurs.",
        })

        importances = pd.Series(model.feature_importances_, index=feature_cols)
        importances = importances.sort_values(ascending=False)
        importances.to_csv(OUT_DIR / "feature_importance.csv", header=["importance"])

        model_metrics["top_feature_importance"] = importances.head(10).to_dict()
    else:
        model_metrics["model_type"] = "correlation_only"
        model_metrics["note"] = "Data gabungan kurang dari 20 baris atau fitur kosong. Korelasi tetap dihitung."

    with open(report_path, "w") as f:
        json.dump(model_metrics, f, indent=2)

    print("\n[OK] Saved outputs:")
    print(f"  {joined_path}")
    print(f"  {corr_path}")
    print(f"  {report_path}")
    if (OUT_DIR / "feature_importance.csv").exists():
        print(f"  {OUT_DIR / 'feature_importance.csv'}")

    print("\nTop correlation vs kurs_change_pct:")
    print(corr.head(10))

    telegram_msg = (
        "✅ <b>Market Flow Modelling Selesai</b>\n"
        "X: News/Sentiment + Commodity\n"
        "Y: Kurs EUR/USD change\n"
        f"Rows gabungan: {len(df)}\n\n"
        "<b>Top Korelasi:</b>\n"
        + "\n".join([f"- {k}: {v:.4f}" for k, v in corr.head(5).items()])
    )

    send_telegram(telegram_msg)


if __name__ == "__main__":
    main()
