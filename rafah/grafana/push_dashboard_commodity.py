"""
rafah/grafana/push_dashboard_commodity.py — IPBD Kelompok 11 (RAFAH)
Push dashboard Grafana untuk monitoring komoditas GLD, BTC-USD, SI=F.

Jalankan setelah Grafana running:
    python3 rafah/grafana/push_dashboard_commodity.py
"""

import json
import requests

GRAFANA = "http://localhost:3001"
AUTH    = ("admin", "admin123")
DS_UID  = "PDF5DF95FFA6EAABB"   # UID datasource PostgreSQL


def ds():
    return {"type": "grafana-postgresql-datasource", "uid": DS_UID}


dashboard = {
    "uid":           "ipbd-commodity-monitoring",
    "title":         "IPBD Kelompok 11 — Commodity Monitor (Rafah)",
    "tags":          ["ipbd", "kelompok11", "commodity", "rafah"],
    "timezone":      "browser",
    "schemaVersion": 36,
    "version":       1,
    "refresh":       "5m",
    "time":          {"from": "now-5y", "to": "now"},
    "panels":        [],
}

y = 0

# ── Panel 1: GLD (Gold) Close Price ───────────────────────────────────────
dashboard["panels"].append({
    "id": 1, "type": "timeseries",
    "title": "Gold (GLD) — Harga Harian",
    "gridPos": {"h": 8, "w": 8, "x": 0, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#FFD700"},
            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10},
        }, "overrides": []
    },
    "options": {"tooltip": {"mode": "single"}, "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{
        "datasource": ds(), "rawQuery": True,
        "rawSql": (
            "SELECT trade_date AT TIME ZONE 'UTC' AS time, close_price AS \"Gold (GLD)\"\n"
            "FROM commodity_daily WHERE symbol='GLD'\n"
            "ORDER BY trade_date ASC"
        ),
        "format": "time_series", "refId": "A"
    }]
})

# ── Panel 2: BTC-USD Close Price ──────────────────────────────────────────
dashboard["panels"].append({
    "id": 2, "type": "timeseries",
    "title": "Bitcoin (BTC-USD) — Harga Harian",
    "gridPos": {"h": 8, "w": 8, "x": 8, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#F7931A"},
            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10},
        }, "overrides": []
    },
    "options": {"tooltip": {"mode": "single"}, "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{
        "datasource": ds(), "rawQuery": True,
        "rawSql": (
            "SELECT trade_date AT TIME ZONE 'UTC' AS time, close_price AS \"Bitcoin (BTC)\"\n"
            "FROM commodity_daily WHERE symbol='BTC-USD'\n"
            "ORDER BY trade_date ASC"
        ),
        "format": "time_series", "refId": "A"
    }]
})

# ── Panel 3: SI=F (Silver) Close Price ────────────────────────────────────
dashboard["panels"].append({
    "id": 3, "type": "timeseries",
    "title": "Silver (SI=F) — Harga Harian",
    "gridPos": {"h": 8, "w": 8, "x": 16, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "color": {"mode": "fixed", "fixedColor": "#C0C0C0"},
            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10},
        }, "overrides": []
    },
    "options": {"tooltip": {"mode": "single"}, "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [{
        "datasource": ds(), "rawQuery": True,
        "rawSql": (
            "SELECT trade_date AT TIME ZONE 'UTC' AS time, close_price AS \"Silver (SI=F)\"\n"
            "FROM commodity_daily WHERE symbol='SI=F'\n"
            "ORDER BY trade_date ASC"
        ),
        "format": "time_series", "refId": "A"
    }]
})

y += 8

# ── Panel 4: Perubahan Harga Harian 3 Komoditas ───────────────────────────
dashboard["panels"].append({
    "id": 4, "type": "timeseries",
    "title": "Perubahan Harga Harian (%) — Semua Komoditas",
    "gridPos": {"h": 8, "w": 16, "x": 0, "y": y},
    "datasource": ds(),
    "fieldConfig": {
        "defaults": {
            "unit": "none",
            "custom": {"drawStyle": "bars", "fillOpacity": 60, "lineWidth": 1},
        }, "overrides": []
    },
    "options": {"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}},
    "targets": [
        {
            "datasource": ds(), "rawQuery": True,
            "rawSql": (
                "SELECT trade_date AT TIME ZONE 'UTC' AS time, price_change_pct AS \"Gold\"\n"
                "FROM commodity_daily WHERE symbol='GLD' ORDER BY trade_date ASC"
            ),
            "format": "time_series", "refId": "A"
        },
        {
            "datasource": ds(), "rawQuery": True,
            "rawSql": (
                "SELECT trade_date AT TIME ZONE 'UTC' AS time, price_change_pct AS \"Bitcoin\"\n"
                "FROM commodity_daily WHERE symbol='BTC-USD' ORDER BY trade_date ASC"
            ),
            "format": "time_series", "refId": "B"
        },
        {
            "datasource": ds(), "rawQuery": True,
            "rawSql": (
                "SELECT trade_date AT TIME ZONE 'UTC' AS time, price_change_pct AS \"Silver\"\n"
                "FROM commodity_daily WHERE symbol='SI=F' ORDER BY trade_date ASC"
            ),
            "format": "time_series", "refId": "C"
        },
    ]
})

# ── Panel 5: Distribusi Label ─────────────────────────────────────────────
dashboard["panels"].append({
    "id": 5, "type": "barchart",
    "title": "Distribusi Label per Komoditas (Naik/Turun/Stabil)",
    "gridPos": {"h": 8, "w": 8, "x": 16, "y": y},
    "datasource": ds(),
    "options": {"barWidth": 0.6, "tooltip": {"mode": "single"}},
    "targets": [{
        "datasource": ds(), "rawQuery": True,
        "rawSql": (
            "SELECT commodity || ' ' || label AS kategori, COUNT(*) AS jumlah\n"
            "FROM commodity_daily WHERE label IS NOT NULL\n"
            "GROUP BY commodity, label ORDER BY commodity, label"
        ),
        "format": "table", "refId": "A"
    }]
})

y += 8

# ── Panel 6: Tabel Data Terkini ───────────────────────────────────────────
dashboard["panels"].append({
    "id": 6, "type": "table",
    "title": "Data Harian Terkini — 3 Komoditas",
    "gridPos": {"h": 10, "w": 24, "x": 0, "y": y},
    "datasource": ds(),
    "options": {"showHeader": True},
    "targets": [{
        "datasource": ds(), "rawQuery": True,
        "rawSql": (
            "SELECT\n"
            "  trade_date,\n"
            "  commodity,\n"
            "  symbol,\n"
            "  ROUND(close_price::numeric, 4)      AS close,\n"
            "  ROUND(price_change_pct::numeric, 4) AS change_pct,\n"
            "  ROUND(ma5::numeric, 4)              AS ma5,\n"
            "  ROUND(ma10::numeric, 4)             AS ma10,\n"
            "  ROUND(volatility::numeric, 6)       AS volatility,\n"
            "  label\n"
            "FROM commodity_daily\n"
            "ORDER BY trade_date DESC, commodity\n"
            "LIMIT 30"
        ),
        "format": "table", "refId": "A"
    }]
})

y += 10

# ── Panel 7–9: Stat Cards ─────────────────────────────────────────────────
stats = [
    (7,  "Total Tick Komoditas", "SELECT COUNT(*) AS val FROM commodity_raw",   "#FF9800", 0),
    (8,  "Total Windows Silver",  "SELECT COUNT(*) AS val FROM commodity_silver", "#F7931A", 6),
    (9,  "Total Hari Gold Layer", "SELECT COUNT(*) AS val FROM commodity_daily",  "#FFD700", 12),
]
for sid, title, sql, color, xpos in stats:
    dashboard["panels"].append({
        "id": sid, "type": "stat",
        "title": title,
        "gridPos": {"h": 4, "w": 6, "x": xpos, "y": y},
        "datasource": ds(),
        "options": {
            "colorMode": "background", "graphMode": "none",
            "textMode": "auto", "justifyMode": "center",
            "reduceOptions": {"calcs": ["last"], "fields": "", "values": False},
        },
        "fieldConfig": {
            "defaults": {
                "unit": "none",
                "color": {"mode": "fixed", "fixedColor": color},
            }, "overrides": []
        },
        "targets": [{
            "datasource": ds(), "rawQuery": True,
            "rawSql": sql, "format": "table", "refId": "A"
        }]
    })

# ── Push ke Grafana ───────────────────────────────────────────────────────

# Cek / buat folder IPBD Kelompok 11
resp = requests.get(f"{GRAFANA}/api/folders", auth=AUTH)
folders = resp.json() if resp.status_code == 200 else []
folder_uid = None
for f in folders:
    if "IPBD" in f.get("title", "") or "kelompok" in f.get("title", "").lower():
        folder_uid = f["uid"]
        break

payload = {
    "dashboard": dashboard,
    "overwrite":  True,
    "message":    "Dashboard commodity Rafah: GLD, BTC-USD, SI=F",
}
if folder_uid:
    payload["folderUid"] = folder_uid

resp = requests.post(
    f"{GRAFANA}/api/dashboards/db",
    auth=AUTH, json=payload,
    headers={"Content-Type": "application/json"},
)

print(f"HTTP {resp.status_code}")
result = resp.json()
if resp.status_code == 200:
    print(f"✅ Dashboard Commodity berhasil!")
    print(f"   Buka: http://localhost:3001{result.get('url','')}")
else:
    print(f"Error: {resp.text}")
