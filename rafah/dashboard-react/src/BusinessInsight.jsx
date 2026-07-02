import { useEffect, useMemo, useState } from "react";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Cell,
} from "recharts";
import {
  ArrowDownRight,
  ArrowRight,
  ArrowUpRight,
  Coins,
  Newspaper,
  Target,
  TrendingUp,
} from "lucide-react";

function num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function fmt(v, d = 3) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(d);
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
      const n = Number(raw);
      row[h || "feature"] = Number.isFinite(n) && raw !== "" ? n : raw;
    });

    return row;
  });
}

async function getJson(path) {
  const res = await fetch(`${path}${path.includes("?") ? "&" : "?"}t=${Date.now()}`);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  const data = await res.json();
  return Array.isArray(data) ? data : data.data || data;
}

async function getCsv(path) {
  const res = await fetch(`${path}?t=${Date.now()}`);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return parseCsv(await res.text());
}

function normalizeKurs(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => ({
    date: r.trade_date || r.date,
    close: num(r.kurs_close ?? r.close ?? r.close_price),
    change_pct: num(r.kurs_change_pct ?? r.change_pct ?? r.price_change_pct),
    label: r.kurs_label ?? r.label ?? "-",
  })).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function normalizeNews(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => {
    const positive = num(r.positive_count ?? r.positif ?? r.positive);
    const negative = num(r.negative_count ?? r.negatif ?? r.negative);
    const avgPos = num(r.avg_pos_prob ?? r.avg_positive ?? r.avg_pos);
    const avgNeg = num(r.avg_neg_prob ?? r.avg_negative ?? r.avg_neg);
    const net = num(r.net_sentiment ?? r.avg_compound ?? r.compound ?? r.sentiment ?? avgPos - avgNeg);

    return {
      date: r.trade_date || r.tanggal || r.date,
      net_sentiment: net,
      positive_count: positive,
      negative_count: negative,
    };
  }).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function normalizeCommodity(rows) {
  return (Array.isArray(rows) ? rows : []).map((r) => ({
    date: r.trade_date || r.date,
    symbol: r.symbol,
    commodity: r.commodity,
    close: num(r.close ?? r.close_price),
    change_pct: num(r.change_pct ?? r.price_change_pct),
    label: r.label ?? "-",
  })).sort((a, b) => String(a.date).localeCompare(String(b.date)));
}

function latestBySymbol(rows) {
  const map = new Map();

  for (const row of [...rows].reverse()) {
    if (row.symbol && !map.has(row.symbol)) {
      map.set(row.symbol, row);
    }
  }

  return Array.from(map.values());
}

function friendlyFeature(name) {
  const map = {
    change_pct_SIF: "Silver daily move",
    change_pct_BTC_USD: "Bitcoin daily move",
    change_pct_GLD: "Gold daily move",
    volatility_SIF: "Silver volatility",
    volatility_GLD: "Gold volatility",
    volatility_BTC_USD: "Bitcoin volatility",
    avg_pos_prob: "Positive news tone",
    avg_neg_prob: "Negative news tone",
    net_sentiment: "Net news sentiment",
    positive_count: "Positive news count",
    negative_count: "Negative news count",
    close_SIF: "Silver price level",
    close_GLD: "Gold price level",
    close_BTC_USD: "Bitcoin price level",
  };

  return map[name] || name;
}

function signalMeta(label) {
  const text = String(label || "").toLowerCase();

  if (text.includes("strength") || text.includes("naik") || text.includes("menguat")) {
    return {
      label: "Strengthening",
      tone: "up",
      icon: <ArrowUpRight size={30} />,
    };
  }

  if (text.includes("weak") || text.includes("turun") || text.includes("melemah")) {
    return {
      label: "Weakening",
      tone: "down",
      icon: <ArrowDownRight size={30} />,
    };
  }

  return {
    label: "Stable",
    tone: "stable",
    icon: <ArrowRight size={30} />,
  };
}

function BusinessMetric({ icon, label, value, sub, tone = "" }) {
  return (
    <div className={`business-metric ${tone}`}>
      <div className="business-metric-icon">{icon}</div>
      <span>{label}</span>
      <b>{value}</b>
      <small>{sub}</small>
    </div>
  );
}

export default function BusinessInsight() {
  const [state, setState] = useState({
    loading: true,
    error: null,
    kurs: [],
    news: [],
    commodity: [],
    predictions: [],
    correlation: [],
    report: {},
    modelPredictions: [],
    businessSignal: {},
  });

  useEffect(() => {
    async function load() {
      try {
        const [
          kurs,
          news,
          commodity,
          predictions,
          correlation,
          report,
          modelPredictions,
          businessSignal,
        ] = await Promise.all([
          getJson("/kurs-api/kurs/daily"),
          getJson("/news-api/api/sentiment/daily"),
          Promise.all([
            getJson("/commodity-api/commodity/daily?symbol=GLD&limit=500"),
            getJson("/commodity-api/commodity/daily?symbol=BTC-USD&limit=500"),
            getJson("/commodity-api/commodity/daily?symbol=SI=F&limit=500"),
          ]).then((x) => x.flat()),
          Promise.all([
            getJson("/commodity-api/predict/GLD").catch(() => ({ symbol: "GLD", prediction: "-" })),
            getJson("/commodity-api/predict/BTC-USD").catch(() => ({ symbol: "BTC-USD", prediction: "-" })),
            getJson("/commodity-api/predict/SI%3DF").catch(() => ({ symbol: "SI=F", prediction: "-" })),
          ]),
          getCsv("/market_flow_outputs/correlation_vs_kurs_change.csv"),
          getJson("/market_flow_outputs/market_flow_model_report.json"),
          getCsv("/market_flow_outputs/model_predictions_daily.csv"),
          getJson("/market_flow_outputs/business_latest_signal.json"),
        ]);

        setState({
          loading: false,
          error: null,
          kurs: normalizeKurs(kurs),
          news: normalizeNews(news),
          commodity: normalizeCommodity(commodity),
          predictions,
          correlation,
          report,
          modelPredictions,
          businessSignal,
        });
      } catch (err) {
        setState((s) => ({ ...s, loading: false, error: err.message }));
      }
    }

    load();
  }, []);

  const latestKurs = state.kurs.at(-1);
  const latestNews = state.news.at(-1);
  const latestCommodity = useMemo(() => latestBySymbol(state.commodity), [state.commodity]);

  const latestPrediction = state.report?.latest_prediction || state.businessSignal || {};
  const signal = signalMeta(latestPrediction.predicted_direction);
  const predictedChange = num(latestPrediction.predicted_change_pct);
  const confidence = num(latestPrediction.confidence);

  const topDrivers = useMemo(() => {
    return (state.correlation || [])
      .filter((r) => r.feature && r.feature !== "kurs_change_pct")
      .map((r) => ({
        feature: r.feature,
        label: friendlyFeature(r.feature),
        pearson_r: num(r.pearson_r),
        abs_r: Math.abs(num(r.pearson_r)),
      }))
      .sort((a, b) => b.abs_r - a.abs_r)
      .slice(0, 6);
  }, [state.correlation]);

  const mainDriver = topDrivers[0];

  const predictionTimeline = useMemo(() => {
    return (state.modelPredictions || []).map((r) => ({
      date: r.date,
      actual_change_pct: num(r.actual_change_pct),
      predicted_change_pct: num(r.predicted_change_pct),
    }));
  }, [state.modelPredictions]);

  const driverBars = topDrivers.map((d) => ({
    name: d.label,
    value: d.pearson_r,
  }));

  const newsScore = latestNews ? latestNews.net_sentiment : 0;
  const commodityScore = latestCommodity.reduce((s, r) => s + num(r.change_pct), 0) / Math.max(latestCommodity.length, 1);

  const impactBars = [
    { name: "News", value: newsScore },
    { name: "Commodity", value: commodityScore },
  ];

  if (state.loading) {
    return (
      <section className="business-page">
        <div className="business-loading">Loading business insight...</div>
      </section>
    );
  }

  return (
    <section className="business-page">
      {state.error && <div className="business-error">{state.error}</div>}

      <div className="business-hero-grid polished">
        <div className={`direction-card ${signal.tone}`}>
          <span>EUR/USD Prediction Signal</span>
          <div className="direction-main">
            {signal.icon}
            <b>{signal.label}</b>
          </div>
          <small>Predicted change: {fmt(predictedChange, 4)}%</small>
        </div>

        <BusinessMetric
          icon={<Target size={22} />}
          label="Main Driver"
          value={mainDriver?.label || "-"}
          sub={mainDriver ? `Correlation ${fmt(mainDriver.pearson_r, 4)}` : "-"}
          tone="blue"
        />

        <BusinessMetric
          icon={<TrendingUp size={22} />}
          label="Confidence"
          value={confidence ? `${fmt(confidence, 0)}%` : "-"}
          sub="Model prediction signal"
          tone="purple"
        />

        <BusinessMetric
          icon={<TrendingUp size={22} />}
          label="Latest EUR/USD"
          value={latestKurs ? fmt(latestKurs.close, 4) : "-"}
          sub={latestKurs ? `${fmt(latestKurs.change_pct, 4)}% · ${latestKurs.label}` : "-"}
          tone="cyan"
        />
      </div>

      <div className="business-main-grid polished">
        <div className="business-panel large">
          <div className="business-panel-head clean">
            <div>
              <span>Prediction view</span>
              <h3>Predicted vs Actual EUR/USD Movement</h3>
            </div>
          </div>

          <ResponsiveContainer width="100%" height={390}>
            <LineChart data={predictionTimeline}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis dataKey="date" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#020617", border: "1px solid #334155" }}
                formatter={(value) => `${fmt(value, 4)}%`}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="actual_change_pct"
                name="Actual EUR/USD Change"
                stroke="#38bdf8"
                strokeWidth={2.6}
                dot
              />
              <Line
                type="monotone"
                dataKey="predicted_change_pct"
                name="Predicted EUR/USD Change"
                stroke="#f97316"
                strokeWidth={2.6}
                dot
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="business-panel">
          <div className="business-panel-head clean">
            <div>
              <span>Current pressure</span>
              <h3>News vs Commodity</h3>
            </div>
          </div>

          <ResponsiveContainer width="100%" height={270}>
            <BarChart data={impactBars} layout="vertical" margin={{ left: 20, right: 24 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} width={95} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} formatter={(v) => fmt(v, 4)} />
              <Bar dataKey="value" radius={[0, 8, 8, 0]}>
                {impactBars.map((entry) => (
                  <Cell key={entry.name} fill={entry.value >= 0 ? "#22c55e" : "#ef4444"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>

          <div className="impact-summary polished">
            <div>
              <Newspaper size={18} />
              <span>News</span>
              <b className={newsScore >= 0 ? "pos" : "neg"}>{fmt(newsScore, 3)}</b>
            </div>
            <div>
              <Coins size={18} />
              <span>Commodity</span>
              <b className={commodityScore >= 0 ? "pos" : "neg"}>{fmt(commodityScore, 3)}</b>
            </div>
          </div>
        </div>
      </div>

      <div className="business-secondary-grid">
        <div className="business-panel">
          <div className="business-panel-head clean">
            <div>
              <span>Driver ranking</span>
              <h3>Top Market Drivers</h3>
            </div>
          </div>

          <ResponsiveContainer width="100%" height={330}>
            <BarChart data={driverBars} layout="vertical" margin={{ left: 135, right: 24 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis type="number" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <YAxis dataKey="name" type="category" tick={{ fill: "#cbd5e1", fontSize: 11 }} />
              <Tooltip contentStyle={{ background: "#020617", border: "1px solid #334155" }} formatter={(v) => fmt(v, 4)} />
              <Bar dataKey="value" fill="#38bdf8" radius={[0, 8, 8, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="business-panel">
          <div className="business-panel-head clean">
            <div>
              <span>Commodity watchlist</span>
              <h3>Latest Commodity Signals</h3>
            </div>
          </div>

          <div className="watchlist polished">
            {latestCommodity.map((row) => {
              const pred = state.predictions.find((p) => p.symbol === row.symbol);
              return (
                <div className="watch-row" key={row.symbol}>
                  <div>
                    <b>{row.symbol}</b>
                    <span>{row.commodity}</span>
                  </div>
                  <div>
                    <small>Close</small>
                    <b>{fmt(row.close, 4)}</b>
                  </div>
                  <div>
                    <small>Change</small>
                    <b className={row.change_pct >= 0 ? "pos" : "neg"}>{fmt(row.change_pct, 4)}%</b>
                  </div>
                  <div>
                    <small>Model</small>
                    <b>{pred?.prediction || row.label || "-"}</b>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </section>
  );
}
