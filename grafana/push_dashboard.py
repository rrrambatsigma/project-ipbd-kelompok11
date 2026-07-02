"""
push_dashboard.py
Push dashboard Grafana via API dengan query yang benar untuk PostgreSQL.
Solusi: gunakan format 'table' untuk semua panel, bukan 'time_series'.
Grafana versi terbaru lebih fleksibel dengan format table + visualization timeseries.
"""
import json
import requests

GRAFANA = "http://localhost:3001"
AUTH    = ("admin", "admin123")


def get_ds_uid() -> str:
    """Auto-detect UID datasource PostgreSQL dari Grafana API."""
    try:
        resp = requests.get(f"{GRAFANA}/api/datasources", auth=AUTH, timeout=5)
        if resp.status_code == 200:
            for ds in resp.json():
                if ds.get("type") == "grafana-postgresql-datasource":
                    print(f"[INFO] DS_UID auto-detected: {ds['uid']}")
                    return ds["uid"]
        print(f"[WARN] Gagal auto-detect DS_UID, pakai fallback.")
    except Exception as e:
        print(f"[WARN] Gagal konek Grafana API: {e}")
    # Fallback — user bisa ganti dengan UID manual
    return "PDF5DF95FFA6EAABB"


DS_UID = get_ds_uid()


def ds():
    return {"type": "grafana-postgresql-datasource", "uid": DS_UID}


dashboard = {
    "uid":           "ipbd-kurs-monitoring",
    "title":         "IPBD Kelompok 11 — EUR/USD Kurs & Sentimen",
    "tags":          ["ipbd", "kelompok11"],
    "timezone":      "browser",
    "schemaVersion": 36,
    "version":       10,
    "refresh":       "5m",
    "time":          {"from": "now-5y", "to": "now"},
    "panels":        []
}

y = 0

# ── Panel 1: EUR/USD Time Series ──────────────────────────────────────────
dashboard["panels"].append({
    "id": 1, "type": "timeseries",
    "title": "EUR/USD Close Price Harian (2021–2026)",
    "gridPos": {"h": 9, "w": 16, "x": 0, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#2196F3"},
            "custom": {
                "drawStyle": "line",
                "lineWidth": 2,
                "fillOpacity": 8,
                "spanNulls": True,
                "showPoints": "never"
            }
        },
        "overrides": []
    },
    "options": {
        "tooltip": {"mode": "single"},
        "legend": {"displayMode": "list", "placement": "bottom"}
    },
    "targets": [{
        "datasource": ds(),
        "rawQuery": True,
            "rawSql": (
                "SELECT\n"
                "  trade_date::timestamp AT TIME ZONE 'UTC' AS time,\n"
                "  close_price AS \"EUR/USD\"\n"
                "FROM kurs_daily\n"
                "WHERE symbol = 'EURUSD=X'\n"
                "ORDER BY trade_date ASC"
            ),
        "format": "time_series",
        "refId": "A"
    }]
})

# ── Panel 2: Distribusi Label ─────────────────────────────────────────────
dashboard["panels"].append({
    "id": 2, "type": "piechart",
    "title": "Distribusi Label Kurs",
    "gridPos": {"h": 9, "w": 8, "x": 16, "y": y},
    "datasource": ds(),
    "options": {
        "pieType": "pie",
        "tooltipOptions": {"mode": "single"},
        "legend": {"displayMode": "list", "placement": "right"}
    },
    "targets": [{
        "datasource": ds(),
        "rawQuery": True,
        "rawSql": (
            "SELECT label, COUNT(*) AS jumlah\n"
            "FROM kurs_daily\n"
            "WHERE symbol = 'EURUSD=X' AND label IS NOT NULL\n"
            "GROUP BY label ORDER BY label"
        ),
        "format": "table",
        "refId": "A"
    }]
})

y += 9

# ── Panel 3: Sentimen Harian ──────────────────────────────────────────────
dashboard["panels"].append({
    "id": 3, "type": "timeseries",
    "title": "Sentimen Harian Rambat (Net Score = pos_prob - neg_prob)",
    "gridPos": {"h": 8, "w": 16, "x": 0, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#4CAF50"},
            "custom": {
                "drawStyle": "bars",
                "fillOpacity": 70,
                "lineWidth": 1,
                "spanNulls": False
            },
            "thresholds": {
                "mode": "absolute",
                "steps": [
                    {"color": "#F44336", "value": None},
                    {"color": "#FFEB3B", "value": -0.05},
                    {"color": "#4CAF50", "value": 0.05}
                ]
            }
        },
        "overrides": []
    },
    "options": {
        "tooltip": {"mode": "single"},
        "legend": {"displayMode": "list", "placement": "bottom"}
    },
    "targets": [{
        "datasource": ds(),
        "rawQuery": True,
            "rawSql": (
                "SELECT\n"
                "  trade_date::timestamp AT TIME ZONE 'UTC' AS time,\n"
                "  avg_sentiment AS \"Net Sentiment\"\n"
                "FROM sentiment_daily\n"
                "ORDER BY trade_date ASC"
            ),
        "format": "time_series",
        "refId": "A"
    }]
})

# ── Panel 4: Total Artikel ────────────────────────────────────────────────
dashboard["panels"].append({
    "id": 4, "type": "timeseries",
    "title": "Total Artikel Berita per Hari",
    "gridPos": {"h": 8, "w": 8, "x": 16, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#FF9800"},
            "custom": {
                "drawStyle": "bars",
                "fillOpacity": 80,
                "lineWidth": 1,
                "spanNulls": False
            }
        },
        "overrides": []
    },
    "options": {"tooltip": {"mode": "single"}},
    "targets": [{
        "datasource": ds(),
        "rawQuery": True,
            "rawSql": (
                "SELECT\n"
                "  trade_date::timestamp AT TIME ZONE 'UTC' AS time,\n"
                "  total_news AS \"Artikel\"\n"
                "FROM sentiment_daily\n"
                "WHERE total_news > 0\n"
                "ORDER BY trade_date ASC"
            ),
        "format": "time_series",
        "refId": "A"
    }]
})

y += 8

# ── Panel 5: Tabel Korelasi ───────────────────────────────────────────────
dashboard["panels"].append({
    "id": 5, "type": "table",
    "title": "Korelasi Sentimen vs Kurs (30 Hari Terakhir)",
    "gridPos": {"h": 10, "w": 24, "x": 0, "y": y},
    "datasource": ds(),
    "options": {"showHeader": True, "sortBy": [{"displayName": "trade_date", "desc": True}]},
    "fieldConfig": {
        "defaults": {"unit": "none"},
        "overrides": [
            {
                "matcher": {"id": "byName", "options": "kurs_change_pct"},
                "properties": [{
                    "id": "thresholds",
                    "value": {"mode": "absolute", "steps": [
                        {"color": "#F44336", "value": None},
                        {"color": "#FFEB3B", "value": -0.3},
                        {"color": "#4CAF50", "value": 0.3}
                    ]}
                }, {"id": "custom.displayMode", "value": "color-background"}]
            },
            {
                "matcher": {"id": "byName", "options": "net_sentiment"},
                "properties": [{
                    "id": "thresholds",
                    "value": {"mode": "absolute", "steps": [
                        {"color": "#F44336", "value": None},
                        {"color": "#FFEB3B", "value": -0.1},
                        {"color": "#4CAF50", "value": 0.1}
                    ]}
                }, {"id": "custom.displayMode", "value": "color-background"}]
            }
        ]
    },
    "targets": [{
        "datasource": ds(),
        "rawQuery": True,
        "rawSql": (
            "SELECT\n"
            "  k.trade_date,\n"
            "  ROUND(k.close_price::numeric, 5)      AS kurs_close,\n"
            "  ROUND(k.price_change_pct::numeric, 4) AS kurs_change_pct,\n"
            "  k.label                               AS kurs_label,\n"
            "  ROUND(s.avg_sentiment::numeric, 4)    AS net_sentiment,\n"
            "  s.positive_count,\n"
            "  s.negative_count,\n"
            "  s.dominant_sentiment\n"
            "FROM kurs_daily k\n"
            "JOIN sentiment_daily s ON s.trade_date = k.trade_date\n"
            "WHERE k.symbol = 'EURUSD=X'\n"
            "ORDER BY k.trade_date DESC\n"
            "LIMIT 30"
        ),
        "format": "table",
        "refId": "A"
    }]
})

y += 10

# ── Panel 6–9: Stat Cards ─────────────────────────────────────────────────
stat_panels = [
    (6,  "Total Ticks (Real-time)", "SELECT COUNT(*) AS val FROM kurs_raw",
     "#2196F3", 0),
    (7,  "Total Windows (Silver)",   "SELECT COUNT(*) AS val FROM kurs_silver",
     "#4CAF50", 6),
    (8,  "Total Hari Kurs",
     "SELECT COUNT(*) AS val FROM kurs_daily WHERE symbol='EURUSD=X'",
     "#9C27B0", 12),
    (9,  "Total Artikel Sentimen",   "SELECT SUM(total_news) AS val FROM sentiment_daily",
     "#FF9800", 18),
]

for sid, title, sql, color, xpos in stat_panels:
    dashboard["panels"].append({
        "id": sid, "type": "stat",
        "title": title,
        "gridPos": {"h": 4, "w": 6, "x": xpos, "y": y},
        "datasource": ds(),
        "options": {
            "colorMode":   "background",
            "graphMode":   "none",
            "textMode":    "auto",
            "justifyMode": "center",
            "reduceOptions": {
                "calcs": ["last"], "fields": "", "values": False
            }
        },
        "fieldConfig": {
            "defaults": {
                "unit": "none",
                "color": {"mode": "fixed", "fixedColor": color},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": color, "value": None}]
                }
            },
            "overrides": []
        },
        "targets": [{
            "datasource": ds(),
            "rawQuery": True,
            "rawSql": sql,
            "format": "table",
            "refId": "A"
        }]
    })

# ── Push via Grafana API ───────────────────────────────────────────────────
payload = {
    "dashboard": dashboard,
    "overwrite": True,
    "message":   "Fix query: AT TIME ZONE UTC untuk time series"
}

resp = requests.post(
    f"{GRAFANA}/api/dashboards/db",
    auth=AUTH,
    json=payload,
    headers={"Content-Type": "application/json"}
)

print(f"HTTP {resp.status_code}")
result = resp.json()
print(f"Status: {result.get('status', '?')}")
if resp.status_code == 200:
    url = result.get("url", "")
    print(f"\n✅ Dashboard berhasil!")
    print(f"   Buka: http://localhost:3001{url}")
else:
    print(f"Error: {resp.text}")
