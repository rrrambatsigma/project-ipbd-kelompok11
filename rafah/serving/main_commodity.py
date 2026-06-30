"""
rafah/serving/main_commodity.py — IPBD Kelompok 11 (RAFAH)
FastAPI REST API untuk data komoditas GLD, BTC-USD, SI=F

Endpoints:
  GET /                           → status
  GET /commodity/latest           → tick terbaru semua komoditas
  GET /commodity/daily            → data harian (Gold layer)
  GET /commodity/daily/{symbol}   → data harian per simbol
  GET /commodity/silver           → window aggregasi 1 menit
  GET /predict/{symbol}           → prediksi arah harga besok
  GET /stats/summary              → ringkasan pipeline

Cara jalankan:
  uvicorn rafah.serving.main_commodity:app --host 0.0.0.0 --port 8001 --reload
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import psycopg2
import psycopg2.extras
import joblib
import pandas as pd
import numpy as np

app = FastAPI(
    title="IPBD Kelompok 11 — Commodity API (Rafah)",
    description="Data harga komoditas GLD (Gold), BTC-USD (Bitcoin), SI=F (Silver).",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

PG_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "kurs_eur_db",
    "user": "kursadmin", "password": "kursadmin",
}

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR  = os.path.join(BASE_DIR, "..", "modelling")

TICKER_MAP = {
    "GLD":     "Gold",
    "BTC-USD": "Bitcoin",
    "SI=F":    "Silver",
}

FEATURE_COLS = [
    "price_change_pct", "volatility",
    "high_low_range", "close_vs_open",
    "close_vs_ma5", "close_vs_ma10", "ma5_vs_ma10",
    "lag1_change_pct", "lag2_change_pct", "lag3_change_pct",
    "lag1_volatility", "lag2_volatility", "lag1_hl_range",
    "rolling3_avg_change", "rolling5_avg_change",
    "rolling3_volatility", "rolling5_volatility",
    "momentum_5d", "positive_ratio", "negative_ratio",
]


def get_db():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


@app.get("/", tags=["Status"])
def root():
    return {
        "status":    "ok",
        "service":   "IPBD Kelompok 11 — Commodity API (Rafah)",
        "tickers":   list(TICKER_MAP.keys()),
        "endpoints": [
            "/commodity/latest",
            "/commodity/daily",
            "/commodity/silver",
            "/predict/{symbol}",
            "/stats/summary",
            "/docs",
        ],
    }


@app.get("/commodity/latest", tags=["Commodity"])
def commodity_latest(limit: int = Query(default=30, le=200)):
    """Tick terbaru semua komoditas dari Bronze layer."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, commodity, price, event_time, source, ingested_at
                FROM commodity_raw
                ORDER BY event_time DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/commodity/daily", tags=["Commodity"])
def commodity_daily(
    symbol: Optional[str] = Query(default=None, description="GLD | BTC-USD | SI=F"),
    start:  Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    end:    Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit:  int = Query(default=60, le=500),
):
    """Data harian komoditas dari Gold layer."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            q = """
                SELECT
                    trade_date, symbol, commodity,
                    ROUND(open_price::numeric,4)       AS open,
                    ROUND(high_price::numeric,4)       AS high,
                    ROUND(low_price::numeric,4)        AS low,
                    ROUND(close_price::numeric,4)      AS close,
                    ROUND(price_change_pct::numeric,4) AS change_pct,
                    ROUND(volatility::numeric,6)       AS volatility,
                    ROUND(ma5::numeric,4)              AS ma5,
                    ROUND(ma10::numeric,4)             AS ma10,
                    label, tick_count, updated_at
                FROM commodity_daily
                WHERE 1=1
            """
            params = []
            if symbol:
                q += " AND symbol = %s"; params.append(symbol)
            if start:
                q += " AND trade_date >= %s"; params.append(start)
            if end:
                q += " AND trade_date <= %s"; params.append(end)
            q += " ORDER BY trade_date DESC, symbol LIMIT %s"
            params.append(limit)
            cur.execute(q, params)
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/commodity/silver", tags=["Commodity"])
def commodity_silver(
    symbol: Optional[str] = Query(default=None),
    limit:  int = Query(default=30, le=200),
):
    """Window aggregasi 1 menit dari Silver layer."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            q = """
                SELECT symbol, commodity, window_start, window_end,
                    ROUND(open_price::numeric,4)       AS open,
                    ROUND(close_price::numeric,4)      AS close,
                    ROUND(price_change_pct::numeric,4) AS change_pct,
                    ROUND(volatility::numeric,6)       AS volatility,
                    tick_count, label
                FROM commodity_silver
            """
            params = []
            if symbol:
                q += " WHERE symbol = %s"; params.append(symbol)
            q += " ORDER BY window_start DESC LIMIT %s"
            params.append(limit)
            cur.execute(q, params)
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/predict/{symbol}", tags=["Prediksi"])
def predict(symbol: str):
    """Prediksi arah harga komoditas hari berikutnya."""
    safe = symbol.replace("/", "-").replace("=", "")
    model_path   = os.path.join(MODEL_DIR, f"xgb_{safe}.pkl")
    encoder_path = os.path.join(MODEL_DIR, f"encoder_{safe}.pkl")

    if not os.path.exists(model_path):
        raise HTTPException(status_code=503,
            detail=f"Model belum ada. Jalankan: python3 rafah/modelling/model_commodity.py --ticker {symbol}")

    model = joblib.load(model_path)
    le    = joblib.load(encoder_path)

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    trade_date,
                    COALESCE(price_change_pct, 0) AS price_change_pct,
                    COALESCE(volatility,       0) AS volatility,
                    COALESCE(high_price - low_price, 0) AS high_low_range,
                    COALESCE(close_price - open_price, 0) AS close_vs_open,
                    COALESCE(close_price - ma5,  0) AS close_vs_ma5,
                    COALESCE(close_price - ma10, 0) AS close_vs_ma10,
                    COALESCE(ma5 - ma10, 0)          AS ma5_vs_ma10,
                    COALESCE(ma5, 0) AS ma5, COALESCE(ma10, 0) AS ma10,
                    COALESCE(close_price, 0) AS close_price
                FROM commodity_daily
                WHERE symbol = %s
                ORDER BY trade_date DESC LIMIT 10
            """, (symbol,))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Belum ada data untuk {symbol}.")

    latest = rows[0]
    prev   = rows[1] if len(rows) > 1 else latest
    prev2  = rows[2] if len(rows) > 2 else latest

    features = {
        "price_change_pct":    latest["price_change_pct"],
        "volatility":          latest["volatility"],
        "high_low_range":      latest["high_low_range"],
        "close_vs_open":       latest["close_vs_open"],
        "close_vs_ma5":        latest["close_vs_ma5"],
        "close_vs_ma10":       latest["close_vs_ma10"],
        "ma5_vs_ma10":         latest["ma5_vs_ma10"],
        "lag1_change_pct":     prev["price_change_pct"],
        "lag2_change_pct":     prev2["price_change_pct"],
        "lag3_change_pct":     rows[3]["price_change_pct"] if len(rows) > 3 else 0,
        "lag1_volatility":     prev["volatility"],
        "lag2_volatility":     prev2["volatility"],
        "lag1_hl_range":       prev["high_low_range"],
        "rolling3_avg_change": np.mean([r["price_change_pct"] for r in rows[:3]]),
        "rolling5_avg_change": np.mean([r["price_change_pct"] for r in rows[:5]]),
        "rolling3_volatility": np.mean([r["volatility"] for r in rows[:3]]),
        "rolling5_volatility": np.mean([r["volatility"] for r in rows[:5]]),
        "momentum_5d":         (latest["close_price"] / rows[4]["close_price"] - 1) * 100
                               if len(rows) > 4 else 0,
        "positive_ratio":      0,
        "negative_ratio":      0,
    }

    X = pd.DataFrame([features])
    pred_enc   = model.predict(X)[0]
    pred_proba = model.predict_proba(X)[0]
    pred_label = le.inverse_transform([pred_enc])[0]

    emoji = {"naik": "📈", "turun": "📉", "stabil": "➡️"}.get(pred_label, "❓")

    return {
        "symbol":        symbol,
        "commodity":     TICKER_MAP.get(symbol, symbol),
        "based_on_date": str(latest["trade_date"]),
        "prediction":    pred_label,
        "emoji":         emoji,
        "confidence":    round(float(max(pred_proba)) * 100, 2),
        "probabilities": {
            cls: round(float(prob) * 100, 2)
            for cls, prob in zip(le.classes_, pred_proba)
        },
    }


@app.get("/stats/summary", tags=["Stats"])
def stats_summary():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM commodity_raw)   AS total_ticks,
                    (SELECT COUNT(*) FROM commodity_silver) AS total_windows,
                    (SELECT COUNT(*) FROM commodity_daily)  AS total_days
            """)
            row = dict(cur.fetchone())

            cur.execute("""
                SELECT symbol, commodity,
                    COUNT(*)                               AS hari,
                    ROUND(AVG(close_price)::numeric, 4)   AS avg_close,
                    MAX(close_price)                       AS max_close,
                    MIN(close_price)                       AS min_close,
                    (SELECT label FROM commodity_daily cd2
                     WHERE cd2.symbol = cd.symbol
                     ORDER BY trade_date DESC LIMIT 1)    AS latest_label
                FROM commodity_daily cd
                GROUP BY symbol, commodity
                ORDER BY commodity
            """)
            per_symbol = [dict(r) for r in cur.fetchall()]

        row["per_symbol"] = per_symbol
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    print("=" * 55)
    print("  IPBD Kelompok 11 — Commodity API (Rafah)")
    print("  http://localhost:8001")
    print("  http://localhost:8001/docs")
    print("=" * 55)
    uvicorn.run("main_commodity:app", host="0.0.0.0", port=8001, reload=True)
