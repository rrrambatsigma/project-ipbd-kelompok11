import { useEffect, useMemo, useState } from "react";
import {
  AreaChart, Area, BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import {
  Activity, AlertTriangle, BarChart3, Bell, Bitcoin, Database,
  Newspaper, RefreshCw, ShieldCheck, TrendingUp,
} from "lucide-react";
import {
  checkApiStatus,
  fetchKursDaily,
  fetchKursSummary,
  fetchNewsDaily,
  fetchCommodityDaily,
  fetchCommodityLatest,
  fetchCommoditySummary,
  fetchCommodityPredictions,
  fetchMarketFlowReport,
  fetchMarketFlowCorrelation,
  fetchMarketFlowFeatureImportance,
} from "./lib/api";
import "./App.css";

function asNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function dateOf(row) {
  return row.trade_date || row.date || row.window_start || row.event_time || "-";
}

function latestOf(rows) {
  return Array.isArray(rows) && rows.length ? rows[0] : null;
}

function normalizeKurs(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => ({
    date: dateOf(r),
    close: asNumber(r.kurs_close ?? r.close ?? r.close_price),
    change_pct: asNumber(r.kurs_change_pct ?? r.change_pct ?? r.price_change_pct),
    label: r.kurs_label ?? r.label ?? "-",
  })).reverse();
}

function normalizeNews(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => {
    const positive = asNumber(r.positive_count ?? r.positif ?? r.positive ?? 0);
    const negative = asNumber(r.negative_count ?? r.negatif ?? r.negative ?? 0);
    const avgPos = asNumber(r.avg_pos_prob ?? r.avg_positive ?? r.avg_pos ?? 0);
    const avgNeg = asNumber(r.avg_neg_prob ?? r.avg_negative ?? r.avg_neg ?? 0);

    let net = r.net_sentiment ?? r.avg_compound ?? r.compound ?? r.sentiment;
    if (net === undefined || net === null) {
      net = avgPos - avgNeg;
    }

    return {
      date: dateOf(r),
      net_sentiment: asNumber(net),
      positive_count: positive,
      negative_count: negative,
      avg_pos_prob: avgPos,
      avg_neg_prob: avgNeg,
      total_news: asNumber(r.total_news ?? r.article_count ?? r.count ?? positive + negative),
    };
  }).reverse();
}

function normalizeCommodity(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => ({
    date: dateOf(r),
    symbol: r.symbol,
    commodity: r.commodity,
    close: asNumber(r.close ?? r.close_price ?? r.price),
    change_pct: asNumber(r.change_pct ?? r.price_change_pct),
    label: r.label ?? "-",
    tick_count: asNumber(r.tick_count),
  })).reverse();
}

function StatusPill({ item }) {
  return (
    <div className={`status-pill ${item.ok ? "online" : "offline"}`}>
      <span className="dot" />
      <div>
        <b>{item.name}</b>
        <small>{item.ok ? "Online" : "Offline"} · {item.baseUrl}</small>
      </div>
    </div>
  );
}

function MetricCard({ title, value, subtitle, icon, tone = "blue" }) {
  return (
    <div className={`metric-card ${tone}`}>
      <div className="metric-icon">{icon}</div>
      <div>
        <p>{title}</p>
        <h2>{value}</h2>
        {subtitle && <span>{subtitle}</span>}
      </div>
    </div>
  );
}

function Panel({ title, subtitle, children, error }) {
  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <h3>{title}</h3>
          {subtitle && <p>{subtitle}</p>}
        </div>
        {error && <span className="panel-error"><AlertTriangle size={14} /> {String(error).slice(0, 80)}</span>}
      </div>
      {children}
    </section>
  );
}

function PredictionBadge({ prediction }) {
  const value = prediction || "unknown";
  const cls = value.includes("naik") || value.includes("menguat") ? "up"
    : value.includes("turun") || value.includes("melemah") ? "down"
    : "stable";

  return <span className={`prediction ${cls}`}>{value}</span>;
}

export default function App() {
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);
  const [apiStatus, setApiStatus] = useState([]);

  const [kurs, setKurs] = useState({ ok: true, data: [], error: null });
  const [kursSummary, setKursSummary] = useState({ ok: true, data: {}, error: null });
  const [news, setNews] = useState({ ok: true, data: [], error: null });
  const [commodity, setCommodity] = useState({ ok: true, data: [], error: null });
  const [commodityLatest, setCommodityLatest] = useState({ ok: true, data: [], error: null });
  const [commoditySummary, setCommoditySummary] = useState({ ok: true, data: {}, error: null });
  const [predictions, setPredictions] = useState({ ok: true, data: [], error: null });
  
  const [modelReport, setModelReport] = useState({ ok: true, data: {}, error: null });
  const [correlationRows, setCorrelationRows] = useState({ ok: true, data: [], error: null });
  const [featureImportanceRows, setFeatureImportanceRows] = useState({ ok: true, data: [], error: null });

  async function load() {
    setLoading(true);
    const [
      statusRes,
      kursRes,
      kursSummaryRes,
      newsRes,
      commodityRes,
      commodityLatestRes,
      commoditySummaryRes,
      predictionRes,
      modelReportRes,
      correlationRes,
      featureImportanceRes,
    ] = await Promise.all([
      checkApiStatus(),
      fetchKursDaily(),
      fetchKursSummary(),
      fetchNewsDaily(),
      fetchCommodityDaily(),
      fetchCommodityLatest(),
      fetchCommoditySummary(),
      fetchCommodityPredictions(),
      fetchMarketFlowReport(),
      fetchMarketFlowCorrelation(),
      fetchMarketFlowFeatureImportance(),
    ]);

    setApiStatus(statusRes);
    setKurs(kursRes);
    setKursSummary(kursSummaryRes);
    setNews(newsRes);
    setCommodity(commodityRes);
    setCommodityLatest(commodityLatestRes);
    setCommoditySummary(commoditySummaryRes);
    setPredictions(predictionRes);
    setModelReport(modelReportRes);
    setCorrelationRows(correlationRes);
    setFeatureImportanceRows(featureImportanceRes);
    setLastRefresh(new Date());
    setLoading(false);
  }

  useEffect(() => {
    load();
    const timer = setInterval(load, 30000);
    return () => clearInterval(timer);
  }, []);

  const kursRows = useMemo(() => normalizeKurs(kurs.data), [kurs]);
  const newsRows = useMemo(() => normalizeNews(news.data), [news]);
  const commodityRows = useMemo(() => normalizeCommodity(commodity.data), [commodity]);
  const latestCommodityRows = useMemo(() => normalizeCommodity(commodityLatest.data), [commodityLatest]);

  const latestKurs = latestOf([...kursRows].reverse());
  const latestNews = latestOf([...newsRows].reverse());

  const commodityBySymbol = useMemo(() => {
    const map = {};
    for (const row of commodityRows) {
      if (!map[row.symbol]) map[row.symbol] = [];
      map[row.symbol].push(row);
    }
    return map;
  }, [commodityRows]);

  const indexedCommodityRows = useMemo(() => {
    const rows = [];

    for (const [symbol, symbolRows] of Object.entries(commodityBySymbol)) {
      const sorted = [...symbolRows].sort((a, b) => String(a.date).localeCompare(String(b.date)));
      const base = sorted.find((r) => r.close > 0)?.close || 1;

      for (const row of sorted) {
        rows.push({
          ...row,
          indexed_close: (row.close / base) * 100,
        });
      }
    }

    return rows;
  }, [commodityBySymbol]);

  const indexedBySymbol = useMemo(() => {
    const map = {};
    for (const row of indexedCommodityRows) {
      if (!map[row.symbol]) map[row.symbol] = [];
      map[row.symbol].push(row);
    }
    return map;
  }, [indexedCommodityRows]);

  const commodityLatestGrouped = useMemo(() => {
    const sourceRows = latestCommodityRows.length ? latestCommodityRows : commodityRows;
    const map = new Map();

    for (const row of [...sourceRows].reverse()) {
      if (row.symbol && !map.has(row.symbol)) {
        map.set(row.symbol, row);
      }
    }

    return Array.from(map.values());
  }, [latestCommodityRows, commodityRows]);

  return (
    <main className="app-shell">
      <div className="hero">
        <div>
          <span className="eyebrow">IPBD Kelompok 11</span>
          <h1>Market Flow Intelligence Dashboard</h1>
          <p>Analisis integrasi kurs EUR/USD, sentimen berita, dan komoditas GLD · BTC-USD · SI=F.</p>
        </div>
        <button className="refresh-btn" onClick={load}>
          <RefreshCw size={18} className={loading ? "spin" : ""} />
          Refresh
        </button>
      </div>

      <section className="status-grid">
        {apiStatus.map((item) => <StatusPill key={item.name} item={item} />)}
      </section>

      <section className="metric-grid">
        <MetricCard
          title="Latest EUR/USD"
          value={latestKurs ? latestKurs.close.toFixed(4) : "-"}
          subtitle={latestKurs ? `${latestKurs.change_pct.toFixed(4)}% · ${latestKurs.label}` : "waiting for Kurs API"}
          icon={<TrendingUp size={22} />}
          tone="blue"
        />
        <MetricCard
          title="Latest Sentiment"
          value={latestNews ? latestNews.net_sentiment.toFixed(3) : "-"}
          subtitle={latestNews ? `positive ${latestNews.positive_count} · negative ${latestNews.negative_count}` : "waiting for News API"}
          icon={<Newspaper size={22} />}
          tone="green"
        />
        <MetricCard
          title="Commodity Rows"
          value={commodityRows.length || "-"}
          subtitle={commodity.ok ? "daily commodity records" : "endpoint error"}
          icon={<Bitcoin size={22} />}
          tone="orange"
        />
        <MetricCard
          title="Monitoring"
          value={apiStatus.every((x) => x.ok) ? "Healthy" : "Check"}
          subtitle={lastRefresh ? `last refresh ${lastRefresh.toLocaleTimeString()}` : "not refreshed yet"}
          icon={<ShieldCheck size={22} />}
          tone="purple"
        />
      </section>

      <section className="dashboard-grid">
        <Panel
          title="EUR/USD Daily Trend"
          subtitle="Jojo module — Kurs API"
          error={kurs.error}
        >
          <ResponsiveContainer width="100%" height={310}>
            <AreaChart data={kursRows}>
              <defs>
                <linearGradient id="kursGradient" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor="#38bdf8" stopOpacity={0.55} />
                  <stop offset="100%" stopColor="#38bdf8" stopOpacity={0.03} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Area type="monotone" dataKey="close" stroke="#38bdf8" fill="url(#kursGradient)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="News Sentiment Trend"
          subtitle="Rambat module — sentiment daily"
          error={news.error}
        >
          <ResponsiveContainer width="100%" height={310}>
            <LineChart data={newsRows}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Legend />
              <Line type="monotone" dataKey="net_sentiment" stroke="#22c55e" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="Commodity Indexed Price Movement"
          subtitle="Rafah module — normalized index, first value = 100"
          error={commodity.error}
        >
          <ResponsiveContainer width="100%" height={330}>
            <LineChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" type="category" allowDuplicatedCategory={false} tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Legend />
              <Line data={indexedBySymbol["GLD"] || []} type="monotone" dataKey="indexed_close" name="Gold GLD indexed" stroke="#facc15" strokeWidth={2} dot={false} />
              <Line data={indexedBySymbol["BTC-USD"] || []} type="monotone" dataKey="indexed_close" name="Bitcoin BTC-USD indexed" stroke="#fb923c" strokeWidth={2} dot={false} />
              <Line data={indexedBySymbol["SI=F"] || []} type="monotone" dataKey="indexed_close" name="Silver SI=F indexed" stroke="#cbd5e1" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="Commodity Prediction"
          subtitle="Model endpoint /predict/{symbol}"
          error={predictions.error}
        >
          <div className="prediction-grid">
            {(predictions.data || []).map((row) => (
              <div className="prediction-card" key={row.symbol}>
                <span>{row.symbol}</span>
                <PredictionBadge prediction={row.prediction} />
                <b>{row.confidence ? `${row.confidence}%` : row.error ? "unavailable" : "-"}</b>
              </div>
            ))}
          </div>

          <div className="mini-table">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Commodity</th>
                  <th>Close</th>
                  <th>Change %</th>
                  <th>Label</th>
                </tr>
              </thead>
              <tbody>
                {commodityLatestGrouped.map((row, idx) => (
                  <tr key={`${row.symbol}-${idx}`}>
                    <td>{row.symbol}</td>
                    <td>{row.commodity}</td>
                    <td>{row.close.toFixed(4)}</td>
                    <td className={row.change_pct >= 0 ? "pos" : "neg"}>{row.change_pct.toFixed(4)}</td>
                    <td><PredictionBadge prediction={row.label} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </section>

      <section className="dashboard-grid">
        <Panel
          title="Market Flow Correlation"
          subtitle="X = News/Sentiment + Commodity · Y = Kurs EUR/USD change"
          error={correlationRows.error}
        >
          <ResponsiveContainer width="100%" height={360}>
            <BarChart
              data={(correlationRows.data || []).filter((r) => r.feature !== "kurs_change_pct").slice(0, 10)}
              layout="vertical"
              margin={{ left: 120, right: 24 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="feature" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Bar dataKey="pearson_r" fill="#38bdf8" />
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="Model Feature Importance"
          subtitle="RandomForestRegressor feature contribution"
          error={featureImportanceRows.error}
        >
          <ResponsiveContainer width="100%" height={360}>
            <BarChart
              data={(featureImportanceRows.data || []).slice(0, 10)}
              layout="vertical"
              margin={{ left: 120, right: 24 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="feature" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Bar dataKey="importance" fill="#f97316" />
            </BarChart>
          </ResponsiveContainer>
        </Panel>
      </section>

      <section className="wide-panel">
        <div className="panel-head">
          <div>
            <h3>Market Flow Modelling Summary</h3>
            <p>Model output from rafah/modelling/market_flow_correlation.py</p>
          </div>
        </div>

        <div className="model-summary-grid">
          <div>
            <span>Rows Joined</span>
            <b>{modelReport.data?.rows_joined ?? "-"}</b>
          </div>
          <div>
            <span>Model Type</span>
            <b>{modelReport.data?.model_type ?? "-"}</b>
          </div>
          <div>
            <span>Target</span>
            <b>{modelReport.data?.target ?? "-"}</b>
          </div>
          <div>
            <span>MAE</span>
            <b>{modelReport.data?.mae !== undefined ? Number(modelReport.data.mae).toFixed(4) : "-"}</b>
          </div>
          <div>
            <span>R²</span>
            <b>{modelReport.data?.r2 !== undefined ? Number(modelReport.data.r2).toFixed(4) : "-"}</b>
          </div>
        </div>

        <p className="model-note">
          {modelReport.data?.note || "No modelling report loaded yet."}
        </p>
      </section>

      <section className="wide-panel">
        <div className="panel-head">
          <div>
            <h3>Integration Status & Alerting</h3>
            <p>Dashboard reads API health. Telegram alerting is handled by backend Python utility, not by React, to keep bot tokens private.</p>
          </div>
          <Bell size={22} />
        </div>

        <div className="alert-row">
          <div>
            <Activity size={18} />
            <span>API refresh interval: 30 seconds</span>
          </div>
          <div>
            <Database size={18} />
            <span>Commodity API: {import.meta.env.VITE_COMMODITY_API}</span>
          </div>
          <div>
            <BarChart3 size={18} />
            <span>Mode: {import.meta.env.VITE_USE_MOCK === "true" ? "Mock data" : "Backend API"}</span>
          </div>
        </div>
      </section>
    </main>
  );
}
