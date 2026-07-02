import { useEffect, useMemo, useState } from "react";
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Bell,
  Bitcoin,
  Database,
  GitCompareArrows,
  Newspaper,
  RefreshCw,
  ShieldCheck,
  Target,
  TrendingUp,
} from "lucide-react";
import {
  checkApiStatus,
  fetchKursDaily,
  fetchNewsDaily,
  fetchCommodityDaily,
  fetchCommodityLatest,
  fetchCommodityPredictions,
  fetchMarketFlowReport,
  fetchMarketFlowCorrelation,
  fetchMarketFlowFeatureImportance,
} from "./lib/api";
import BusinessInsight from "./BusinessInsight";
import "./App.css";

function asNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function formatNumber(value, digits = 4) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits);
}

function formatPercent(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${n.toFixed(digits)}%`;
}

function dateOf(row) {
  return row.trade_date || row.tanggal || row.date || row.window_start || row.event_time || "-";
}

function sortByDate(rows) {
  return [...rows].sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  if (lines.length < 2) return [];

  const headers = lines[0].split(",").map((h) => h.trim());

  return lines.slice(1).map((line) => {
    const parts = line.split(",");
    const row = {};

    headers.forEach((h, i) => {
      const raw = parts[i]?.trim();
      const num = Number(raw);
      row[h || "feature"] = Number.isFinite(num) && raw !== "" ? num : raw;
    });

    return row;
  });
}

async function fetchJoinedDataset() {
  try {
    const res = await fetch(`/market_flow_outputs/market_flow_joined_dataset.csv?t=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { ok: true, data: parseCsv(await res.text()), error: null };
  } catch (err) {
    return { ok: false, data: [], error: err.message };
  }
}

function normalizeKurs(rows) {
  return sortByDate((Array.isArray(rows) ? rows : []).map((r) => ({
    date: dateOf(r),
    close: asNumber(r.kurs_close ?? r.close ?? r.close_price),
    change_pct: asNumber(r.kurs_change_pct ?? r.change_pct ?? r.price_change_pct),
    label: r.kurs_label ?? r.label ?? "-",
  })));
}

function normalizeNews(rows) {
  return sortByDate((Array.isArray(rows) ? rows : []).map((r) => {
    const positive = asNumber(r.positive_count ?? r.positif ?? r.positive ?? 0);
    const negative = asNumber(r.negative_count ?? r.negatif ?? r.negative ?? 0);
    const avgPos = asNumber(r.avg_pos_prob ?? r.avg_positive ?? r.avg_pos ?? 0);
    const avgNeg = asNumber(r.avg_neg_prob ?? r.avg_negative ?? r.avg_neg ?? 0);

    let net = r.net_sentiment ?? r.avg_compound ?? r.compound ?? r.sentiment;
    if (net === undefined || net === null) net = avgPos - avgNeg;

    return {
      date: dateOf(r),
      net_sentiment: asNumber(net),
      positive_count: positive,
      negative_count: negative,
      avg_pos_prob: avgPos,
      avg_neg_prob: avgNeg,
      total_news: asNumber(r.total_news ?? r.article_count ?? r.count ?? positive + negative),
    };
  }));
}

function normalizeCommodity(rows) {
  return sortByDate((Array.isArray(rows) ? rows : []).map((r) => ({
    date: dateOf(r),
    symbol: r.symbol,
    commodity: r.commodity,
    close: asNumber(r.close ?? r.close_price ?? r.price),
    change_pct: asNumber(r.change_pct ?? r.price_change_pct),
    label: r.label ?? "-",
    tick_count: asNumber(r.tick_count),
  })));
}

function aggregateMonthlySentiment(rows) {
  const map = new Map();

  for (const row of rows) {
    if (!row.date || row.date === "-") continue;
    const month = String(row.date).slice(0, 7);

    if (!map.has(month)) {
      map.set(month, { month, sentiment_sum: 0, total_news: 0, rows: 0 });
    }

    const item = map.get(month);
    item.sentiment_sum += row.net_sentiment;
    item.total_news += row.total_news;
    item.rows += 1;
  }

  return Array.from(map.values()).map((r) => ({
    month: r.month,
    avg_sentiment: r.rows ? r.sentiment_sum / r.rows : 0,
    total_news: r.total_news,
  })).slice(-36);
}


function buildDriverTimeline(rows, driverKey) {
  if (!driverKey) return [];

  const clean = (Array.isArray(rows) ? rows : [])
    .map((r) => ({
      date: r.date,
      kurs: Number(r.kurs_change_pct),
      driver: Number(r[driverKey]),
    }))
    .filter((r) => Number.isFinite(r.kurs) && Number.isFinite(r.driver));

  if (!clean.length) return [];

  const mean = (arr, key) => arr.reduce((s, r) => s + r[key], 0) / arr.length;
  const std = (arr, key, m) => {
    const v = Math.sqrt(arr.reduce((s, r) => s + Math.pow(r[key] - m, 2), 0) / arr.length);
    return v || 1;
  };

  const kursMean = mean(clean, "kurs");
  const driverMean = mean(clean, "driver");
  const kursStd = std(clean, "kurs", kursMean);
  const driverStd = std(clean, "driver", driverMean);

  return clean.slice(-45).map((r) => ({
    date: r.date,
    kurs_change_pct: r.kurs,
    driver_value: r.driver,
    kurs_z: (r.kurs - kursMean) / kursStd,
    driver_z: (r.driver - driverMean) / driverStd,
  }));
}

function ServicePill({ item }) {
  return (
    <div className={`service-pill ${item.ok ? "online" : "offline"}`}>
      <span />
      <div>
        <b>{item.name}</b>
        <small>{item.ok ? "Online" : "Offline"} · {item.baseUrl}</small>
      </div>
    </div>
  );
}

function KpiCard({ icon, label, value, sub, tone = "blue" }) {
  return (
    <div className={`kpi-card ${tone}`}>
      <div className="kpi-icon">{icon}</div>
      <div>
        <span>{label}</span>
        <b>{value}</b>
        <small>{sub}</small>
      </div>
    </div>
  );
}

function Panel({ title, eyebrow, children, error, className = "" }) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-header">
        <div>
          {eyebrow && <span className="panel-eyebrow">{eyebrow}</span>}
          <h3>{title}</h3>
        </div>
        {error && (
          <div className="panel-error">
            <AlertTriangle size={14} />
            {String(error).slice(0, 80)}
          </div>
        )}
      </div>
      {children}
    </section>
  );
}

function PredictionBadge({ value }) {
  const text = String(value || "unknown").toLowerCase();

  const cls =
    text.includes("naik") || text.includes("menguat") || text.includes("up")
      ? "up"
      : text.includes("turun") || text.includes("melemah") || text.includes("down")
        ? "down"
        : "stable";

  return <span className={`badge ${cls}`}>{value || "-"}</span>;
}

export default function App() {
  const [view, setView] = useState("business");
  const [loading, setLoading] = useState(false);
  const [lastRefresh, setLastRefresh] = useState(null);

  const [apiStatus, setApiStatus] = useState([]);
  const [kurs, setKurs] = useState({ ok: true, data: [], error: null });
  const [news, setNews] = useState({ ok: true, data: [], error: null });
  const [commodity, setCommodity] = useState({ ok: true, data: [], error: null });
  const [commodityLatest, setCommodityLatest] = useState({ ok: true, data: [], error: null });
  const [predictions, setPredictions] = useState({ ok: true, data: [], error: null });

  const [modelReport, setModelReport] = useState({ ok: true, data: {}, error: null });
  const [correlationRows, setCorrelationRows] = useState({ ok: true, data: [], error: null });
  const [featureImportanceRows, setFeatureImportanceRows] = useState({ ok: true, data: [], error: null });
  const [joinedRows, setJoinedRows] = useState({ ok: true, data: [], error: null });

  async function load() {
    setLoading(true);

    const [
      statusRes,
      kursRes,
      newsRes,
      commodityRes,
      commodityLatestRes,
      predictionRes,
      modelReportRes,
      correlationRes,
      featureImportanceRes,
      joinedRes,
    ] = await Promise.all([
      checkApiStatus(),
      fetchKursDaily(),
      fetchNewsDaily(),
      fetchCommodityDaily(),
      fetchCommodityLatest(),
      fetchCommodityPredictions(),
      fetchMarketFlowReport(),
      fetchMarketFlowCorrelation(),
      fetchMarketFlowFeatureImportance(),
      fetchJoinedDataset(),
    ]);

    setApiStatus(statusRes);
    setKurs(kursRes);
    setNews(newsRes);
    setCommodity(commodityRes);
    setCommodityLatest(commodityLatestRes);
    setPredictions(predictionRes);
    setModelReport(modelReportRes);
    setCorrelationRows(correlationRes);
    setFeatureImportanceRows(featureImportanceRes);
    setJoinedRows(joinedRes);
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

  const latestKurs = kursRows.at(-1);
  const latestNews = newsRows.at(-1);

  const monthlyNews = useMemo(() => aggregateMonthlySentiment(newsRows), [newsRows]);

  const topDriver = useMemo(() => {
    return (correlationRows.data || [])
      .filter((r) => r.feature !== "kurs_change_pct")
      .filter((r) => Number.isFinite(Number(r.pearson_r)))
      .sort((a, b) => Math.abs(Number(b.pearson_r)) - Math.abs(Number(a.pearson_r)))[0];
  }, [correlationRows]);

  const driverType = useMemo(() => {
    const feature = String(topDriver?.feature || "").toLowerCase();

    if (
      feature.includes("sentiment") ||
      feature.includes("positive") ||
      feature.includes("negative") ||
      feature.includes("pos_prob") ||
      feature.includes("neg_prob") ||
      feature.includes("news") ||
      feature.includes("article")
    ) {
      return "news";
    }

    if (
      feature.includes("btc") ||
      feature.includes("gld") ||
      feature.includes("sif") ||
      feature.includes("si=f") ||
      feature.includes("commodity") ||
      feature.includes("volatility") ||
      feature.includes("change_pct") ||
      feature.includes("close_")
    ) {
      return "commodity";
    }

    if (feature.includes("kurs") || feature.includes("eur")) {
      return "kurs";
    }

    return "commodity";
  }, [topDriver]);


  const correlationTop = useMemo(() => {
    return (correlationRows.data || [])
      .filter((r) => r.feature !== "kurs_change_pct")
      .slice(0, 8);
  }, [correlationRows]);

  const featureTop = useMemo(() => {
    return (featureImportanceRows.data || []).slice(0, 8);
  }, [featureImportanceRows]);

  const scatterRows = useMemo(() => {
    if (!topDriver) return [];

    return (joinedRows.data || [])
      .map((r) => ({
        date: r.date,
        x: Number(r[topDriver.feature]),
        y: Number(r.kurs_change_pct),
      }))
      .filter((r) => Number.isFinite(r.x) && Number.isFinite(r.y));
  }, [joinedRows, topDriver]);

  const driverTimeline = useMemo(() => {
    return buildDriverTimeline(joinedRows.data || [], topDriver?.feature);
  }, [joinedRows, topDriver]);


  const commodityBySymbol = useMemo(() => {
    const map = {};
    for (const row of commodityRows) {
      if (!map[row.symbol]) map[row.symbol] = [];
      map[row.symbol].push(row);
    }
    return map;
  }, [commodityRows]);

  const indexedBySymbol = useMemo(() => {
    const map = {};

    for (const [symbol, rows] of Object.entries(commodityBySymbol)) {
      const sorted = sortByDate(rows).slice(-160);
      const base = sorted.find((r) => r.close > 0)?.close || 1;

      map[symbol] = sorted.map((r) => ({
        ...r,
        indexed_close: (r.close / base) * 100,
      }));
    }

    return map;
  }, [commodityBySymbol]);

  const latestCommodityGrouped = useMemo(() => {
    const source = latestCommodityRows.length ? latestCommodityRows : commodityRows;
    const map = new Map();

    for (const row of [...source].reverse()) {
      if (row.symbol && !map.has(row.symbol)) map.set(row.symbol, row);
    }

    return Array.from(map.values());
  }, [latestCommodityRows, commodityRows]);

  const modelR2 = modelReport.data?.r2;
  const modelNote = Number(modelR2) < 0
    ? "Exploratory only: data join masih kecil, jadi fokus pada korelasi dan feature importance."
    : "Model dapat digunakan sebagai indikasi awal hubungan X terhadap Y.";

  return (
    <main className="page">
      <header className="hero">
        <div>
          <span className="hero-chip">IPBD Kelompok 11</span>
          <h1>Market Flow Intelligence</h1>
</div>

        <button className="refresh-button" onClick={load}>
          <RefreshCw size={18} className={loading ? "spin" : ""} />
          Refresh
        </button>
      </header>

      <nav className="dashboard-nav">
        <button
          className={`nav-tab ${view === "business" ? "active" : ""}`}
          onClick={() => setView("business")}
        >
          Business Insight
        </button>
        <button
          className={`nav-tab ${view === "model" ? "active" : ""}`}
          onClick={() => setView("model")}
        >
          Model Analysis
        </button>
      </nav>

      {view === "business" ? (
        <BusinessInsight refreshKey={lastRefresh?.getTime() || 0} />
      ) : (
        <>

<section className="kpi-grid">
        <KpiCard
          icon={<TrendingUp size={21} />}
          label="Latest EUR/USD"
          value={latestKurs ? formatNumber(latestKurs.close, 4) : "-"}
          sub={latestKurs ? `${formatPercent(latestKurs.change_pct, 4)} · ${latestKurs.label}` : "Kurs API"}
          tone="blue"
        />

        <KpiCard
          icon={<Newspaper size={21} />}
          label="Latest Sentiment"
          value={latestNews ? formatNumber(latestNews.net_sentiment, 3) : "-"}
          sub={latestNews ? `positive ${latestNews.positive_count} · negative ${latestNews.negative_count}` : "News API"}
          tone="green"
        />

        <KpiCard
          icon={<GitCompareArrows size={21} />}
          label="Strongest X Driver"
          value={topDriver?.feature || "-"}
          sub={topDriver ? `Pearson r ${formatNumber(topDriver.pearson_r, 4)}` : "Model output"}
          tone="cyan"
        />

        <KpiCard
          icon={<Target size={21} />}
          label="Joined Days"
          value={modelReport.data?.rows_joined ?? "-"}
          sub="News + Commodity + EUR/USD"
          tone="purple"
        />
      </section>

      <section className="stakeholder-summary">
        <div className="stakeholder-left">
          <h2>EUR/USD market flow signal</h2>
          <p>
            This view summarizes the current relationship between market drivers and EUR/USD movement.
          </p>

          <div className="signal-grid">
            <div className="signal-card primary">
              <span>Main driver</span>
              <b>{topDriver?.feature || "-"}</b>
              <small>Pearson r {topDriver ? formatNumber(topDriver.pearson_r, 4) : "-"}</small>
            </div>

            <div className="signal-card">
              <span>Latest EUR/USD</span>
              <b>{latestKurs ? formatNumber(latestKurs.close, 4) : "-"}</b>
              <small>{latestKurs ? `${formatPercent(latestKurs.change_pct, 4)} · ${latestKurs.label}` : "No data"}</small>
            </div>

            <div className="signal-card">
              <span>Latest sentiment</span>
              <b>{latestNews ? formatNumber(latestNews.net_sentiment, 3) : "-"}</b>
              <small>{latestNews ? `+${latestNews.positive_count} / -${latestNews.negative_count}` : "No data"}</small>
            </div>

</div>
        </div>

        <div className="stakeholder-chart">
          <div className="chart-title-row">
            <div>
              <h3>Kurs movement vs strongest driver</h3>
              <p>Both series are normalized so the direction of movement can be compared.</p>
            </div>
          </div>

          <ResponsiveContainer width="100%" height={390}>
            <LineChart data={driverTimeline}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#020617", border: "1px solid #334155" }}
                formatter={(value) => formatNumber(value, 4)}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="kurs_z"
                name="EUR/USD movement"
                stroke="#38bdf8"
                strokeWidth={2}
                dot={false}
              />
              <Line
                type="monotone"
                dataKey="driver_z"
                name={topDriver?.feature || "Top driver"}
                stroke="#f97316"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="analysis-grid">
        <Panel title="Which drivers move with EUR/USD?" eyebrow="Driver relationship">
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={correlationTop} layout="vertical" margin={{ left: 120, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="feature" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Bar dataKey="pearson_r" fill="#38bdf8" radius={[0, 8, 8, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="What does the model rely on?" eyebrow="Feature importance">
          <ResponsiveContainer width="100%" height={340}>
            <BarChart data={featureTop} layout="vertical" margin={{ left: 120, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="feature" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Bar dataKey="importance" fill="#f97316" radius={[0, 8, 8, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </Panel>
      </section>

      <section className="context-grid context-grid-dynamic">
        <Panel
          title="EUR/USD Context"
          eyebrow="Kurs trend"
          error={kurs.error}
          className={driverType === "kurs" ? "featured-context" : "supporting-context"}
        >
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={kursRows.slice(-45)}>
              <defs>
                <linearGradient id="kursFill" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor="#38bdf8" stopOpacity={0.55} />
                  <stop offset="100%" stopColor="#38bdf8" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <YAxis tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Area type="monotone" dataKey="close" stroke="#38bdf8" fill="url(#kursFill)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="News Sentiment Context"
          eyebrow="Monthly average sentiment"
          error={news.error}
          className={driverType === "news" ? "featured-context" : "supporting-context"}
        >
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={monthlyNews}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="month" tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <YAxis tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Line type="monotone" dataKey="avg_sentiment" stroke="#22c55e" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel
          title="Commodity Context"
          eyebrow="Indexed movement, base = 100"
          error={commodity.error}
          className={driverType === "commodity" ? "featured-context" : "supporting-context"}
        >
          <ResponsiveContainer width="100%" height={300}>
            <LineChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" type="category" allowDuplicatedCategory={false} tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <YAxis tick={{ fill: "#cbd5e1", fontSize: 10 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} />
              <Legend />
              <Line data={indexedBySymbol["GLD"] || []} type="monotone" dataKey="indexed_close" name="GLD" stroke="#facc15" strokeWidth={2} dot={false} />
              <Line data={indexedBySymbol["BTC-USD"] || []} type="monotone" dataKey="indexed_close" name="BTC-USD" stroke="#fb923c" strokeWidth={2} dot={false} />
              <Line data={indexedBySymbol["SI=F"] || []} type="monotone" dataKey="indexed_close" name="SI=F" stroke="#cbd5e1" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>
      </section>

      <section className="stakeholder-prediction-section">
        <Panel title="Commodity prediction signals" eyebrow="Rafah model endpoint">
          <div className="prediction-row">
            {(predictions.data || []).map((row) => (
              <div className="prediction-card" key={row.symbol}>
                <span>{row.symbol}</span>
                <PredictionBadge value={row.prediction} />
                <b>{row.confidence ? `${row.confidence}%` : row.error ? "unavailable" : "-"}</b>
              </div>
            ))}
          </div>

          <div className="table-wrap">
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
                {latestCommodityGrouped.map((row) => (
                  <tr key={row.symbol}>
                    <td>{row.symbol}</td>
                    <td>{row.commodity}</td>
                    <td>{formatNumber(row.close, 4)}</td>
                    <td className={row.change_pct >= 0 ? "pos" : "neg"}>
                      {formatNumber(row.change_pct, 4)}
                    </td>
                    <td><PredictionBadge value={row.label} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </section>

        </>
      )}
    </main>
  );
}
