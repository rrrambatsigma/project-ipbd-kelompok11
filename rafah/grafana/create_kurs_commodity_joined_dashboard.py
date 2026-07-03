import json
import os
import requests


GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3300").rstrip("/")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASS = os.getenv("GRAFANA_PASS", "admin")
DS_UID = os.getenv("GRAFANA_POSTGRES_UID", "marketflow-postgres")


def post(path, payload):
    r = requests.post(
        f"{GRAFANA_URL}{path}",
        auth=(GRAFANA_USER, GRAFANA_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def target(sql, ref_id="A", fmt="time_series"):
    return {
        "refId": ref_id,
        "format": fmt,
        "rawSql": sql,
        "rawQuery": True,
        "datasource": {"type": "postgres", "uid": DS_UID},
    }


def stat(pid, title, x, y, w, h, sql):
    return {
        "id": pid,
        "type": "stat",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "postgres", "uid": DS_UID},
        "targets": [target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "decimals": 4,
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "red", "value": None},
                        {"color": "yellow", "value": 0},
                        {"color": "green", "value": 0.01}
                    ]
                }
            },
            "overrides": []
        },
        "options": {"colorMode": "background", "graphMode": "area", "justifyMode": "center"},
    }


def timeseries(pid, title, x, y, w, h, sql):
    return {
        "id": pid,
        "type": "timeseries",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "postgres", "uid": DS_UID},
        "targets": [target(sql)],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "smooth",
                    "lineWidth": 2,
                    "fillOpacity": 18,
                    "showPoints": "never"
                }
            },
            "overrides": []
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "none"}
        },
    }


def bar(pid, title, x, y, w, h, sql):
    return {
        "id": pid,
        "type": "barchart",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "postgres", "uid": DS_UID},
        "targets": [target(sql, fmt="table")],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {
            "orientation": "horizontal",
            "showValue": "auto",
            "legend": {"showLegend": False},
            "tooltip": {"mode": "single"}
        },
    }


def table(pid, title, x, y, w, h, sql):
    return {
        "id": pid,
        "type": "table",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "postgres", "uid": DS_UID},
        "targets": [target(sql, fmt="table")],
        "options": {"showHeader": True, "cellHeight": "sm"},
        "fieldConfig": {"defaults": {}, "overrides": []},
    }


SQL_LATEST_KURS = """
SELECT kurs_close AS value
FROM market_flow_joined
ORDER BY date DESC
LIMIT 1
"""

SQL_LATEST_CHANGE = """
SELECT kurs_change_pct AS value
FROM market_flow_joined
ORDER BY date DESC
LIMIT 1
"""

SQL_COMMODITY_PRESSURE = """
SELECT
  (
    COALESCE(change_pct_SIF, 0) * 0.40 +
    COALESCE(change_pct_BTC_USD, 0) * 0.35 +
    COALESCE(change_pct_GLD, 0) * 0.25
  ) AS value
FROM market_flow_joined
ORDER BY date DESC
LIMIT 1
"""

SQL_TOP_DRIVER = """
WITH corrs AS (
  SELECT 'Silver (SI=F)' AS commodity, CORR(kurs_change_pct, change_pct_SIF) AS r FROM market_flow_joined
  UNION ALL
  SELECT 'Bitcoin (BTC-USD)' AS commodity, CORR(kurs_change_pct, change_pct_BTC_USD) AS r FROM market_flow_joined
  UNION ALL
  SELECT 'Gold (GLD)' AS commodity, CORR(kurs_change_pct, change_pct_GLD) AS r FROM market_flow_joined
)
SELECT ROUND(r::numeric, 4) AS value
FROM corrs
ORDER BY ABS(r) DESC NULLS LAST
LIMIT 1
"""

SQL_INDEXED = """
WITH base AS (
  SELECT
    date::timestamp AS time,
    kurs_change_pct,
    change_pct_BTC_USD,
    change_pct_GLD,
    change_pct_SIF
  FROM market_flow_joined
),
series AS (
  SELECT time, 'EUR/USD' AS metric, kurs_change_pct AS pct FROM base
  UNION ALL SELECT time, 'BTC-USD', change_pct_BTC_USD FROM base
  UNION ALL SELECT time, 'GLD', change_pct_GLD FROM base
  UNION ALL SELECT time, 'SI=F', change_pct_SIF FROM base
)
SELECT
  time,
  metric,
  100 * EXP(SUM(LN(GREATEST(0.000001, 1 + COALESCE(pct,0) / 100))) OVER (PARTITION BY metric ORDER BY time)) AS value
FROM series
ORDER BY time, metric
"""

SQL_DAILY_CHANGE = """
SELECT date::timestamp AS time, 'EUR/USD' AS metric, kurs_change_pct AS value FROM market_flow_joined
UNION ALL SELECT date::timestamp AS time, 'BTC-USD' AS metric, change_pct_BTC_USD AS value FROM market_flow_joined
UNION ALL SELECT date::timestamp AS time, 'GLD' AS metric, change_pct_GLD AS value FROM market_flow_joined
UNION ALL SELECT date::timestamp AS time, 'SI=F' AS metric, change_pct_SIF AS value FROM market_flow_joined
ORDER BY time, metric
"""

SQL_CORR = """
WITH corrs AS (
  SELECT 'Silver (SI=F)' AS commodity, CORR(kurs_change_pct, change_pct_SIF) AS pearson_r FROM market_flow_joined
  UNION ALL
  SELECT 'Bitcoin (BTC-USD)' AS commodity, CORR(kurs_change_pct, change_pct_BTC_USD) AS pearson_r FROM market_flow_joined
  UNION ALL
  SELECT 'Gold (GLD)' AS commodity, CORR(kurs_change_pct, change_pct_GLD) AS pearson_r FROM market_flow_joined
)
SELECT commodity, ROUND(pearson_r::numeric, 4) AS pearson_r
FROM corrs
ORDER BY ABS(pearson_r) DESC NULLS LAST
"""

SQL_RECENT = """
SELECT
  date,
  ROUND(kurs_close::numeric, 5) AS kurs_close,
  ROUND(kurs_change_pct::numeric, 4) AS kurs_change_pct,
  ROUND(change_pct_BTC_USD::numeric, 4) AS btc_usd_change_pct,
  ROUND(change_pct_GLD::numeric, 4) AS gld_change_pct,
  ROUND(change_pct_SIF::numeric, 4) AS sif_change_pct,
  CASE
    WHEN GREATEST(ABS(COALESCE(change_pct_BTC_USD,0)), ABS(COALESCE(change_pct_GLD,0)), ABS(COALESCE(change_pct_SIF,0))) = ABS(COALESCE(change_pct_SIF,0)) THEN 'SI=F'
    WHEN GREATEST(ABS(COALESCE(change_pct_BTC_USD,0)), ABS(COALESCE(change_pct_GLD,0)), ABS(COALESCE(change_pct_SIF,0))) = ABS(COALESCE(change_pct_BTC_USD,0)) THEN 'BTC-USD'
    ELSE 'GLD'
  END AS dominant_commodity
FROM market_flow_joined
ORDER BY date DESC
LIMIT 20
"""

SQL_LATEST_COMMODITY = """
SELECT *
FROM (
  SELECT 'BTC-USD' AS symbol, close_BTC_USD AS close, change_pct_BTC_USD AS change_pct
  FROM market_flow_joined ORDER BY date DESC LIMIT 1
) btc

UNION ALL

SELECT *
FROM (
  SELECT 'GLD' AS symbol, close_GLD AS close, change_pct_GLD AS change_pct
  FROM market_flow_joined ORDER BY date DESC LIMIT 1
) gld

UNION ALL

SELECT *
FROM (
  SELECT 'SI=F' AS symbol, close_SIF AS close, change_pct_SIF AS change_pct
  FROM market_flow_joined ORDER BY date DESC LIMIT 1
) sif
"""

dashboard = {
    "uid": "ipbd-kurs-commodity-analysis",
    "title": "IPBD Kelompok 11 — Kurs × Commodity Analysis",
    "tags": ["ipbd", "kurs", "commodity", "rafah"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5m",
    "time": {"from": "now-60d", "to": "now"},
    "panels": [
        stat(1, "Latest EUR/USD", 0, 0, 6, 4, SQL_LATEST_KURS),
        stat(2, "Latest EUR/USD Change %", 6, 0, 6, 4, SQL_LATEST_CHANGE),
        stat(3, "Commodity Pressure", 12, 0, 6, 4, SQL_COMMODITY_PRESSURE),
        stat(4, "Top Commodity Correlation", 18, 0, 6, 4, SQL_TOP_DRIVER),

        timeseries(5, "EUR/USD vs Commodity Indexed Movement", 0, 4, 16, 9, SQL_INDEXED),
        bar(6, "Commodity Correlation vs EUR/USD", 16, 4, 8, 9, SQL_CORR),

        timeseries(7, "Daily Change: EUR/USD vs Commodities", 0, 13, 16, 9, SQL_DAILY_CHANGE),
        table(8, "Latest Commodity Snapshot", 16, 13, 8, 9, SQL_LATEST_COMMODITY),

        table(9, "Recent Kurs × Commodity Movement", 0, 22, 24, 9, SQL_RECENT),
    ],
}

payload = {
    "dashboard": dashboard,
    "folderUid": "",
    "overwrite": True,
    "message": "Create Kurs Commodity Analysis dashboard from market_flow_joined"
}

res = post("/api/dashboards/db", payload)
print("[OK] Dashboard created/updated:")
print(GRAFANA_URL + res.get("url", ""))
