import axios from "axios";

const KURS_API = import.meta.env.VITE_KURS_API;
const NEWS_API = import.meta.env.VITE_NEWS_API;
const COMMODITY_API = import.meta.env.VITE_COMMODITY_API;
const USE_MOCK = import.meta.env.VITE_USE_MOCK === "true";

const api = axios.create({ timeout: 20000 });

function unwrap(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload.data)) return payload.data;
  if (Array.isArray(payload.results)) return payload.results;
  return payload;
}

async function safeGet(url, fallback = []) {
  try {
    const res = await api.get(url);
    return { ok: true, data: unwrap(res.data), error: null };
  } catch (err) {
    return {
      ok: false,
      data: fallback,
      error: err?.response?.data?.detail || err.message || "Request failed",
    };
  }
}

const mock = {
  kursDaily: [
    { trade_date: "2026-06-26", kurs_close: 1.14, kurs_change_pct: 0.0659, kurs_label: "menguat" },
    { trade_date: "2026-06-25", kurs_close: 1.14, kurs_change_pct: -0.222, kurs_label: "melemah" },
    { trade_date: "2026-06-24", kurs_close: 1.14, kurs_change_pct: -0.416, kurs_label: "melemah" },
  ],
  newsDaily: [
    { date: "2026-06-26", net_sentiment: -0.561, positive_count: 0, negative_count: 1, total_news: 1 },
    { date: "2026-06-25", net_sentiment: 0.219, positive_count: 4, negative_count: 1, total_news: 5 },
    { date: "2026-06-24", net_sentiment: -0.482, positive_count: 0, negative_count: 2, total_news: 2 },
  ],
  commodityDaily: [
    { trade_date: "2026-06-26", symbol: "GLD", commodity: "Gold", close: 365.84, change_pct: 0.21, label: "stabil" },
    { trade_date: "2026-06-26", symbol: "BTC-USD", commodity: "Bitcoin", close: 59478.33, change_pct: -0.53, label: "turun" },
    { trade_date: "2026-06-26", symbol: "SI=F", commodity: "Silver", close: 36.12, change_pct: 0.18, label: "stabil" },
  ],
};

export async function checkApiStatus() {
  const targets = [
    ["News", NEWS_API],
    ["Kurs", KURS_API],
    ["Commodity", COMMODITY_API],
  ];

  const checks = await Promise.all(
    targets.map(async ([name, baseUrl]) => {
      try {
        const res = await api.get(`${baseUrl}/`);
        return { name, baseUrl, ok: res.status < 500, status: res.status };
      } catch (err) {
        return { name, baseUrl, ok: false, status: null, error: err.message };
      }
    })
  );

  return checks;
}

export async function fetchKursDaily() {
  if (USE_MOCK) return { ok: true, data: mock.kursDaily, error: null };
  return safeGet(`${KURS_API}/kurs/daily`, []);
}

export async function fetchKursSummary() {
  if (USE_MOCK) return { ok: true, data: {}, error: null };
  return safeGet(`${KURS_API}/stats/summary`, {});
}

export async function fetchNewsDaily() {
  if (USE_MOCK) return { ok: true, data: mock.newsDaily, error: null };
  return safeGet(`${NEWS_API}/api/sentiment/daily`, []);
}

export async function fetchCommodityDaily() {
  if (USE_MOCK) return { ok: true, data: mock.commodityDaily, error: null };
  return safeGet(`${COMMODITY_API}/commodity/daily?limit=500`, []);
}

export async function fetchCommodityLatest() {
  if (USE_MOCK) return { ok: true, data: mock.commodityDaily, error: null };
  return safeGet(`${COMMODITY_API}/commodity/latest?limit=20`, []);
}

export async function fetchCommoditySummary() {
  if (USE_MOCK) return { ok: true, data: { total_days: 3 }, error: null };
  return safeGet(`${COMMODITY_API}/stats/summary`, {});
}

export async function fetchCommodityPredictions() {
  if (USE_MOCK) {
    return {
      ok: true,
      data: [
        { symbol: "GLD", prediction: "stabil", confidence: 75 },
        { symbol: "BTC-USD", prediction: "turun", confidence: 63 },
        { symbol: "SI=F", prediction: "stabil", confidence: 68 },
      ],
      error: null,
    };
  }

  const symbols = ["GLD", "BTC-USD", "SI=F"];
  const rows = [];

  for (const symbol of symbols) {
    const result = await safeGet(`${COMMODITY_API}/predict/${encodeURIComponent(symbol)}`, null);
    if (result.ok && result.data) {
      rows.push(result.data);
    } else {
      rows.push({ symbol, prediction: "unavailable", confidence: null, error: result.error });
    }
  }

  return { ok: true, data: rows, error: null };
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = lines[0].split(",");
  const firstName = headers[0] || "feature";
  const secondName = headers[1] || "value";

  return lines.slice(1).map((line) => {
    const parts = line.split(",");
    return {
      [firstName]: parts[0],
      [secondName]: Number(parts[1]),
    };
  });
}

export async function fetchMarketFlowReport() {
  try {
    const res = await fetch(`/market_flow_outputs/market_flow_model_report.json?t=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { ok: true, data: await res.json(), error: null };
  } catch (err) {
    return { ok: false, data: {}, error: err.message };
  }
}

export async function fetchMarketFlowCorrelation() {
  try {
    const res = await fetch(`/market_flow_outputs/correlation_vs_kurs_change.csv?t=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const rows = parseCsv(text).map((r) => ({
      feature: r.feature,
      pearson_r: r.pearson_r,
    }));
    return { ok: true, data: rows, error: null };
  } catch (err) {
    return { ok: false, data: [], error: err.message };
  }
}

export async function fetchMarketFlowFeatureImportance() {
  try {
    const res = await fetch(`/market_flow_outputs/feature_importance.csv?t=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const rows = parseCsv(text).map((r) => ({
      feature: r.feature,
      importance: r.importance,
    }));
    return { ok: true, data: rows, error: null };
  } catch (err) {
    return { ok: false, data: [], error: err.message };
  }
}
