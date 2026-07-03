"""
serving/main.py — IPBD Kelompok 11
FastAPI Backend — Serving Layer

Endpoint yang tersedia:
  GET /                          → status API
  GET /kurs/latest               → harga kurs terbaru (real-time dari Bronze)
  GET /kurs/daily                → data harian kurs (Gold layer)
  GET /kurs/daily/{symbol}       → data harian per simbol
  GET /kurs/silver               → window aggregation terbaru (Silver)
  GET /market/signals            → v_market_signals (gabungan kurs+komoditas+sentimen)
  GET /market/signals/latest     → sinyal terbaru untuk dashboard
  GET /predict/today             → prediksi arah EUR/USD hari ini
  GET /stats/summary             → ringkasan statistik pipeline

Cara jalankan:
  python3 serving/main.py
  atau
  uvicorn serving.main:app --host 0.0.0.0 --port 8000 --reload
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import psycopg2
import psycopg2.extras
from datetime import datetime, date
import joblib

app = FastAPI(
    title="IPBD Kelompok 11 — Market Flow API",
    description=(
        "API untuk mengakses data kurs EUR/USD, komoditas, dan sentimen publik. "
        "Digunakan bersama oleh Jojo (kurs), Rafah (komoditas), dan Rambat (sentimen+dashboard)."
    ),
    version="1.0.0"
)

# CORS — izinkan semua origin supaya Rambat bisa akses dari Streamlit/Redash
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Konfigurasi DB ────────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "kurs_eur_db",
    "user":     "kursadmin",
    "password": "kursadmin"
}

MODEL_PATH   = os.path.join(os.path.dirname(__file__), "../modelling/rf_model.pkl")
ENCODER_PATH = os.path.join(os.path.dirname(__file__), "../modelling/label_encoder.pkl")


def get_db():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


# ── Root ──────────────────────────────────────────────────────────────────

@app.get("/", tags=["Status"])
def root():
    return {
        "status":  "ok",
        "service": "IPBD Kelompok 11 — Market Flow API",
        "version": "1.0.0",
        "endpoints": [
            "/kurs/latest",
            "/kurs/daily",
            "/kurs/silver",
            "/market/signals",
            "/market/signals/latest",
            "/predict/today",
            "/stats/summary",
            "/docs"
        ]
    }


# ── KURS Endpoints (data dari Jojo) ───────────────────────────────────────

@app.get("/kurs/latest", tags=["Kurs"])
def kurs_latest(limit: int = Query(default=20, le=100)):
    """
    Harga kurs terbaru dari Bronze layer (real-time tick).
    Dipakai untuk komponen live price di dashboard.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, price, event_time, source, ingested_at
                FROM kurs_raw
                ORDER BY event_time DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/kurs/daily", tags=["Kurs"])
def kurs_daily(
    symbol: str = Query(default="EURUSD=X"),
    start:  Optional[str] = Query(default=None, description="Format: YYYY-MM-DD"),
    end:    Optional[str] = Query(default=None, description="Format: YYYY-MM-DD"),
    limit:  int = Query(default=30, le=365)
):
    """
    Data kurs harian dari Gold layer.
    Berisi open, high, low, close, MA5, MA10, label, volatility.
    Dipakai untuk chart tren dan analisis.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT
                    trade_date, symbol,
                    ROUND(open_price::numeric, 5)       AS open,
                    ROUND(high_price::numeric, 5)       AS high,
                    ROUND(low_price::numeric, 5)        AS low,
                    ROUND(close_price::numeric, 5)      AS close,
                    ROUND(avg_price::numeric, 5)        AS avg,
                    ROUND(volatility::numeric, 6)       AS volatility,
                    ROUND(price_change::numeric, 6)     AS price_change,
                    ROUND(price_change_pct::numeric, 4) AS price_change_pct,
                    ROUND(ma5::numeric, 5)              AS ma5,
                    ROUND(ma10::numeric, 5)             AS ma10,
                    tick_count,
                    label,
                    updated_at
                FROM kurs_daily
                WHERE symbol = %s
            """
            params = [symbol]
            if start:
                query += " AND trade_date >= %s"
                params.append(start)
            if end:
                query += " AND trade_date <= %s"
                params.append(end)
            query += " ORDER BY trade_date DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
        return {"symbol": symbol, "data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/kurs/silver", tags=["Kurs"])
def kurs_silver(
    symbol: Optional[str] = Query(default=None),
    limit:  int = Query(default=20, le=200)
):
    """
    Window aggregation 1 menit dari Silver layer.
    Dipakai untuk grafik intraday.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT
                    symbol, window_start, window_end,
                    ROUND(open_price::numeric, 5)       AS open,
                    ROUND(close_price::numeric, 5)      AS close,
                    ROUND(avg_price::numeric, 5)        AS avg,
                    ROUND(volatility::numeric, 6)       AS volatility,
                    ROUND(price_change_pct::numeric, 4) AS price_change_pct,
                    tick_count, label
                FROM kurs_silver
            """
            params = []
            if symbol:
                query += " WHERE symbol = %s"
                params.append(symbol)
            query += " ORDER BY window_start DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── MARKET SIGNALS (gabungan Jojo + Rafah + Rambat) ───────────────────────

@app.get("/market/signals", tags=["Market Signals"])
def market_signals(
    start: Optional[str] = Query(default=None, description="Format: YYYY-MM-DD"),
    end:   Optional[str] = Query(default=None, description="Format: YYYY-MM-DD"),
    limit: int = Query(default=30, le=365)
):
    """
    Data gabungan kurs + komoditas + sentimen dari v_market_signals.
    Ini adalah endpoint utama untuk modelling dan dashboard Rambat.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT *
                FROM v_market_signals
                WHERE 1=1
            """
            params = []
            if start:
                query += " AND trade_date >= %s"
                params.append(start)
            if end:
                query += " AND trade_date <= %s"
                params.append(end)
            query += " LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
        return {"data": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/market/signals/latest", tags=["Market Signals"])
def market_signals_latest():
    """
    Sinyal pasar terbaru — dipakai untuk komponen summary di dashboard.
    Menampilkan kondisi hari ini: arah kurs, komoditas, sentimen.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM v_market_signals
                LIMIT 1
            """)
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Belum ada data.")

        data = dict(row)

        # Tambah interpretasi ringkas
        label = data.get("kurs_label", "stabil")
        label_emoji = "📈" if label == "menguat" else "📉" if label == "melemah" else "➡️"
        data["summary"] = (
            f"{label_emoji} EUR/USD {label.upper()} "
            f"({data.get('kurs_change_pct', 0):+.4f}%) "
            f"| MA5={data.get('kurs_ma5', 0):.5f}"
        )
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── PREDIKSI ──────────────────────────────────────────────────────────────

@app.get("/predict/today", tags=["Prediksi"])
def predict_today():
    """
    Prediksi arah EUR/USD hari berikutnya menggunakan Random Forest.
    Output: label (menguat/melemah/stabil) + probabilitas.
    """
    if not os.path.exists(MODEL_PATH):
        raise HTTPException(
            status_code=503,
            detail="Model belum tersedia. Jalankan: python3 modelling/model_kurs.py --mode train"
        )

    try:
        import pandas as pd
        import numpy as np

        model = joblib.load(MODEL_PATH)
        le    = joblib.load(ENCODER_PATH)

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    trade_date,
                    COALESCE(price_change_pct, 0)  AS kurs_change_pct,
                    COALESCE(volatility, 0)         AS kurs_volatility,
                    COALESCE(ma5, 0)                AS kurs_ma5,
                    COALESCE(ma10, 0)               AS kurs_ma10,
                    0 AS wti_change_pct,
                    0 AS brent_change_pct,
                    0 AS gold_change_pct,
                    0 AS natgas_change_pct,
                    0 AS copper_change_pct,
                    0 AS avg_sentiment,
                    0 AS positive_count,
                    0 AS negative_count,
                    0 AS total_news,
                    0 AS sentiment_volatility
                FROM kurs_daily
                WHERE symbol = 'EURUSD=X'
                ORDER BY trade_date DESC
                LIMIT 3
            """)
            rows = cur.fetchall()
        conn.close()

        if not rows:
            raise HTTPException(status_code=404, detail="Belum ada data kurs harian.")

        latest = dict(rows[0])
        prev1  = dict(rows[1]) if len(rows) > 1 else latest
        prev2  = dict(rows[2]) if len(rows) > 2 else latest

        features = {
            "kurs_change_pct":        latest["kurs_change_pct"],
            "kurs_volatility":        latest["kurs_volatility"],
            "kurs_ma5":               latest["kurs_ma5"],
            "kurs_ma10":              latest["kurs_ma10"],
            "wti_change_pct":         0,
            "brent_change_pct":       0,
            "gold_change_pct":        0,
            "natgas_change_pct":      0,
            "copper_change_pct":      0,
            "avg_sentiment":          0,
            "positive_count":         0,
            "negative_count":         0,
            "total_news":             0,
            "sentiment_volatility":   0,
            "kurs_change_pct_lag1":   prev1["kurs_change_pct"],
            "kurs_change_pct_lag2":   prev2["kurs_change_pct"],
            "kurs_volatility_lag1":   prev1["kurs_volatility"],
            "kurs_volatility_lag2":   prev2["kurs_volatility"],
        }

        X = pd.DataFrame([features])
        pred_enc   = model.predict(X)[0]
        pred_proba = model.predict_proba(X)[0]
        pred_label = le.inverse_transform([pred_enc])[0]

        label_emoji = "📈" if pred_label == "menguat" else "📉" if pred_label == "melemah" else "➡️"

        return {
            "based_on_date":  str(latest["trade_date"]),
            "prediction":     pred_label,
            "emoji":          label_emoji,
            "confidence":     round(float(max(pred_proba)) * 100, 2),
            "probabilities": {
                cls: round(float(prob) * 100, 2)
                for cls, prob in zip(le.classes_, pred_proba)
            },
            "context": {
                "kurs_change_pct": latest["kurs_change_pct"],
                "kurs_ma5":        latest["kurs_ma5"],
                "kurs_ma10":       latest["kurs_ma10"],
                "kurs_volatility": latest["kurs_volatility"],
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── STATS SUMMARY ─────────────────────────────────────────────────────────

@app.get("/stats/summary", tags=["Stats"])
def stats_summary():
    """
    Ringkasan statistik pipeline — dipakai untuk monitoring card di dashboard.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM kurs_raw)          AS total_ticks,
                    (SELECT COUNT(*) FROM kurs_silver)       AS total_windows,
                    (SELECT COUNT(*) FROM kurs_daily)        AS total_days,
                    (SELECT MAX(ingested_at) FROM kurs_raw)  AS last_tick_at,
                    (SELECT AVG(price) FROM kurs_raw WHERE symbol = 'EURUSD=X'
                     AND ingested_at >= NOW() - INTERVAL '1 hour')
                                                             AS avg_price_1h,
                    (SELECT label FROM kurs_daily
                     WHERE symbol = 'EURUSD=X'
                     ORDER BY trade_date DESC LIMIT 1)       AS latest_label,
                    (SELECT close_price FROM kurs_daily
                     WHERE symbol = 'EURUSD=X'
                     ORDER BY trade_date DESC LIMIT 1)       AS latest_close,
                    (SELECT COUNT(*) FROM commodity_daily)   AS commodity_days,
                    (SELECT COUNT(*) FROM sentiment_daily)   AS sentiment_days
            """)
            row = dict(cur.fetchone())

        # Tambah label distribusi
        with conn.cursor() as cur:
            cur.execute("""
                SELECT label, COUNT(*) AS count
                FROM kurs_daily
                WHERE label IS NOT NULL
                GROUP BY label
            """)
            label_dist = {r["label"]: r["count"] for r in cur.fetchall()}

        row["label_distribution"] = label_dist
        row["pipeline_status"] = "active"
        row["timestamp"] = datetime.now().isoformat()

        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    print("="*55)
    print("  IPBD Kelompok 11 — Market Flow API")
    print("  http://localhost:8000")
    print("  http://localhost:8000/docs  ← Swagger UI")
    print("="*55)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
