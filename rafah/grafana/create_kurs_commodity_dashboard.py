import json
import os
import sys
import requests


GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3001").rstrip("/")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASS = os.getenv("GRAFANA_PASS", "admin")


def grafana_get(path):
    r = requests.get(
        f"{GRAFANA_URL}{path}",
        auth=(GRAFANA_USER, GRAFANA_PASS),
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def grafana_post(path, payload):
    r = requests.post(
        f"{GRAFANA_URL}{path}",
        auth=(GRAFANA_USER, GRAFANA_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def find_postgres_uid():
    datasources = grafana_get("/api/datasources")

    postgres = [d for d in datasources if d.get("type") == "postgres"]

    if not postgres:
        print("[ERROR] No PostgreSQL datasource found in Grafana.")
        print("Available datasources:")
        for d in datasources:
            print("-", d.get("name"), d.get("type"), d.get("uid"))
        sys.exit(1)

    ds = postgres[0]
    print(f"[OK] Using PostgreSQL datasource: {ds.get('name')} | uid={ds.get('uid')}")
    return ds.get("uid")


def target(sql, ref_id="A", fmt="time_series"):
    return {
        "refId": ref_id,
        "format": fmt,
        "rawSql": sql,
        "rawQuery": True,
        "datasource": {
            "type": "postgres",
            "uid": DS_UID,
        },
    }


def panel_timeseries(panel_id, title, x, y, w, h, sql, desc=""):
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": {
            "type": "postgres",
            "uid": DS_UID,
        },
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(sql)],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "smooth",
                    "lineWidth": 2,
                    "fillOpacity": 18,
                    "showPoints": "never",
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None}
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": True,
            },
            "tooltip": {
                "mode": "multi",
                "sort": "none",
            },
        },
    }


def panel_barchart(panel_id, title, x, y, w, h, sql, desc=""):
    return {
        "id": panel_id,
        "type": "barchart",
        "title": title,
        "description": desc,
        "datasource": {
            "type": "postgres",
            "uid": DS_UID,
        },
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "custom": {},
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None}
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "orientation": "horizontal",
            "xTickLabelRotation": 0,
            "xTickLabelSpacing": 0,
            "showValue": "auto",
            "stacking": "none",
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
                "showLegend": False,
            },
            "tooltip": {
                "mode": "single",
                "sort": "none",
            },
        },
    }


def panel_table(panel_id, title, x, y, w, h, sql, desc=""):
    return {
        "id": panel_id,
        "type": "table",
        "title": title,
        "description": desc,
        "datasource": {
            "type": "postgres",
            "uid": DS_UID,
        },
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "align": "auto",
                    "cellOptions": {
                        "type": "auto"
                    },
                    "inspect": False,
                },
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None}
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "showHeader": True,
            "cellHeight": "sm",
        },
    }


def panel_stat(panel_id, title, x, y, w, h, sql, desc=""):
    return {
        "id": panel_id,
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": {
            "type": "postgres",
            "uid": DS_UID,
        },
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(sql, fmt="table")],
        "fieldConfig": {
            "defaults": {
                "mappings": [],
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "red", "value": None},
                        {"color": "yellow", "value": 0},
                        {"color": "green", "value": 0.05}
                    ],
                },
            },
            "overrides": [],
        },
        "options": {
            "colorMode": "background",
            "graphMode": "area",
            "justifyMode": "center",
            "orientation": "auto",
            "reduceOptions": {
                "calcs": ["lastNotNull"],
                "fields": "",
                "values": False
            },
            "textMode": "auto",
        },
    }


DS_UID = find_postgres_uid()

SQL_LATEST_KURS = """
SELECT
  kurs_close::numeric AS value
FROM kurs_daily
ORDER BY trade_date DESC
LIMIT 1
"""

SQL_LATEST_COMMODITY_PRESSURE = """
WITH latest AS (
  SELECT DISTINCT ON (symbol)
    symbol,
    change_pct::numeric AS change_pct
  FROM commodity_daily
  WHERE symbol IN ('GLD', 'BTC-USD', 'SI=F')
  ORDER BY symbol, trade_date DESC
)
SELECT
  SUM(
    CASE
      WHEN symbol = 'SI=F' THEN change_pct * 0.40
      WHEN symbol = 'BTC-USD' THEN change_pct * 0.35
      WHEN symbol = 'GLD' THEN change_pct * 0.25
      ELSE 0
    END
  ) AS value
FROM latest
"""

SQL_EURUSD_VS_COMMODITY_INDEXED = """
WITH kurs AS (
  SELECT
    trade_date::timestamp AS time,
    'EUR/USD' AS metric,
    100 * EXP(SUM(LN(1 + (kurs_change_pct::numeric / 100))) OVER (ORDER BY trade_date)) AS value
  FROM kurs_daily
),
commodity AS (
  SELECT
    trade_date::timestamp AS time,
    symbol AS metric,
    100 * EXP(SUM(LN(1 + (change_pct::numeric / 100))) OVER (PARTITION BY symbol ORDER BY trade_date)) AS value
  FROM commodity_daily
  WHERE symbol IN ('GLD', 'BTC-USD', 'SI=F')
)
SELECT time, metric, value
FROM kurs
UNION ALL
SELECT time, metric, value
FROM commodity
ORDER BY time, metric
"""

SQL_DAILY_CHANGE_COMPARISON = """
SELECT
  trade_date::timestamp AS time,
  'EUR/USD' AS metric,
  kurs_change_pct::numeric AS value
FROM kurs_daily

UNION ALL

SELECT
  trade_date::timestamp AS time,
  symbol AS metric,
  change_pct::numeric AS value
FROM commodity_daily
WHERE symbol IN ('GLD', 'BTC-USD', 'SI=F')

ORDER BY time, metric
"""

SQL_CORRELATION = """
WITH joined AS (
  SELECT
    k.trade_date,
    k.kurs_change_pct::numeric AS kurs_change_pct,
    MAX(CASE WHEN c.symbol = 'GLD' THEN c.change_pct::numeric END) AS gld_change_pct,
    MAX(CASE WHEN c.symbol = 'BTC-USD' THEN c.change_pct::numeric END) AS btc_change_pct,
    MAX(CASE WHEN c.symbol = 'SI=F' THEN c.change_pct::numeric END) AS sif_change_pct
  FROM kurs_daily k
  LEFT JOIN commodity_daily c
    ON k.trade_date::date = c.trade_date::date
   AND c.symbol IN ('GLD', 'BTC-USD', 'SI=F')
  GROUP BY k.trade_date, k.kurs_change_pct
),
corrs AS (
  SELECT 'Gold (GLD)' AS commodity, CORR(kurs_change_pct, gld_change_pct) AS pearson_r FROM joined
  UNION ALL
  SELECT 'Bitcoin (BTC-USD)' AS commodity, CORR(kurs_change_pct, btc_change_pct) AS pearson_r FROM joined
  UNION ALL
  SELECT 'Silver (SI=F)' AS commodity, CORR(kurs_change_pct, sif_change_pct) AS pearson_r FROM joined
)
SELECT
  commodity,
  ROUND(pearson_r::numeric, 4) AS pearson_r
FROM corrs
ORDER BY ABS(pearson_r) DESC NULLS LAST
"""

SQL_RECENT_TABLE = """
WITH joined AS (
  SELECT
    k.trade_date,
    k.kurs_close::numeric AS kurs_close,
    k.kurs_change_pct::numeric AS kurs_change_pct,
    MAX(CASE WHEN c.symbol = 'GLD' THEN c.change_pct::numeric END) AS gld_change_pct,
    MAX(CASE WHEN c.symbol = 'BTC-USD' THEN c.change_pct::numeric END) AS btc_change_pct,
    MAX(CASE WHEN c.symbol = 'SI=F' THEN c.change_pct::numeric END) AS sif_change_pct
  FROM kurs_daily k
  LEFT JOIN commodity_daily c
    ON k.trade_date::date = c.trade_date::date
   AND c.symbol IN ('GLD', 'BTC-USD', 'SI=F')
  GROUP BY k.trade_date, k.kurs_close, k.kurs_change_pct
)
SELECT
  trade_date,
  ROUND(kurs_close, 5) AS kurs_close,
  ROUND(kurs_change_pct, 4) AS kurs_change_pct,
  ROUND(btc_change_pct, 4) AS btc_usd_change_pct,
  ROUND(gld_change_pct, 4) AS gld_change_pct,
  ROUND(sif_change_pct, 4) AS sif_change_pct,
  CASE
    WHEN GREATEST(ABS(COALESCE(btc_change_pct,0)), ABS(COALESCE(gld_change_pct,0)), ABS(COALESCE(sif_change_pct,0))) = ABS(COALESCE(sif_change_pct,0)) THEN 'SI=F'
    WHEN GREATEST(ABS(COALESCE(btc_change_pct,0)), ABS(COALESCE(gld_change_pct,0)), ABS(COALESCE(sif_change_pct,0))) = ABS(COALESCE(btc_change_pct,0)) THEN 'BTC-USD'
    ELSE 'GLD'
  END AS dominant_commodity
FROM joined
ORDER BY trade_date DESC
LIMIT 20
"""

SQL_COMMODITY_LATEST_TABLE = """
SELECT DISTINCT ON (symbol)
  symbol,
  commodity,
  trade_date,
  ROUND(close::numeric, 4) AS close,
  ROUND(change_pct::numeric, 4) AS change_pct,
  label
FROM commodity_daily
WHERE symbol IN ('GLD', 'BTC-USD', 'SI=F')
ORDER BY symbol, trade_date DESC
"""

SQL_COMMODITY_DAILY_COUNT = """
SELECT
  trade_date::timestamp AS time,
  symbol AS metric,
  COUNT(*) AS value
FROM commodity_daily
WHERE symbol IN ('GLD', 'BTC-USD', 'SI=F')
GROUP BY trade_date, symbol
ORDER BY trade_date, symbol
"""

dashboard = {
    "uid": "ipbd-kurs-commodity-analysis",
    "title": "IPBD Kelompok 11 — Kurs × Commodity Analysis",
    "tags": ["ipbd", "kurs", "commodity", "rafah"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5m",
    "time": {
        "from": "now-5y",
        "to": "now"
    },
    "panels": [
        panel_stat(1, "Latest EUR/USD", 0, 0, 6, 4, SQL_LATEST_KURS),
        panel_stat(2, "Commodity Pressure", 6, 0, 6, 4, SQL_LATEST_COMMODITY_PRESSURE),
        panel_table(3, "Latest Commodity Data", 12, 0, 12, 4, SQL_COMMODITY_LATEST_TABLE),

        panel_timeseries(
            4,
            "EUR/USD vs Commodity Indexed Movement",
            0,
            4,
            16,
            9,
            SQL_EURUSD_VS_COMMODITY_INDEXED,
            "All series are indexed to 100 so EUR/USD, Gold, Bitcoin, and Silver can be compared on the same scale."
        ),
        panel_barchart(
            5,
            "Commodity Correlation vs EUR/USD",
            16,
            4,
            8,
            9,
            SQL_CORRELATION,
            "Pearson correlation between daily commodity change and EUR/USD daily change."
        ),

        panel_timeseries(
            6,
            "Daily Change: EUR/USD vs Commodities",
            0,
            13,
            16,
            9,
            SQL_DAILY_CHANGE_COMPARISON,
            "Daily percentage movement comparison."
        ),
        panel_timeseries(
            7,
            "Commodity Records per Day",
            16,
            13,
            8,
            9,
            SQL_COMMODITY_DAILY_COUNT,
            "Count of commodity daily records by symbol."
        ),

        panel_table(
            8,
            "Recent Kurs × Commodity Movement",
            0,
            22,
            24,
            9,
            SQL_RECENT_TABLE,
            "Recent EUR/USD movement joined with commodity movement."
        ),
    ],
}

payload = {
    "dashboard": dashboard,
    "folderUid": "",
    "overwrite": True,
    "message": "Create Kurs Commodity Analysis dashboard"
}

result = grafana_post("/api/dashboards/db", payload)
print("[OK] Dashboard created/updated.")
print(result.get("url"))
print(f"{GRAFANA_URL}{result.get('url')}")
