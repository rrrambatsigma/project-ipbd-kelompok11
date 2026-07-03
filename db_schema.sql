-- =============================================================
--  IPBD KELOMPOK 11 — SHARED DATABASE SCHEMA
--  Database : kurs_eur_db
--  Host     : localhost:5433
--  User     : kursadmin  |  Password: kursadmin
-- =============================================================
--
--  PEMBAGIAN TUGAS:
--  ┌─────────┬────────────────────────────────────────────────┐
--  │  JOJO   │ kurs_raw, kurs_silver, kurs_daily              │
--  │  RAFAH  │ commodity_raw, commodity_silver, commodity_daily│
--  │  RAMBAT │ news_raw, news_clean, sentiment_daily          │
--  └─────────┴────────────────────────────────────────────────┘
--
--  Gold layer (modelling bersama): v_market_signals (VIEW)
--  View ini menggabungkan kurs_daily + commodity_daily + sentiment_daily
-- =============================================================


-- -------------------------------------------------------------
-- [JOJO] BRONZE: Tick kurs mentah yang sudah divalidasi
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kurs_raw (
    id          SERIAL           PRIMARY KEY,
    symbol      VARCHAR(20)      NOT NULL,          -- misal: EURUSD=X, EURIDR=X
    price       DOUBLE PRECISION NOT NULL,
    event_time  TIMESTAMP        NOT NULL,
    source      VARCHAR(50),                        -- Yahoo Finance
    ingested_at TIMESTAMP        DEFAULT NOW()
);

-- -------------------------------------------------------------
-- [JOJO] SILVER: Aggregasi per window 1 menit + fitur
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kurs_silver (
    id               SERIAL           PRIMARY KEY,
    symbol           VARCHAR(20)      NOT NULL,
    window_start     TIMESTAMP        NOT NULL,
    window_end       TIMESTAMP        NOT NULL,
    open_price       DOUBLE PRECISION,
    close_price      DOUBLE PRECISION,
    avg_price        DOUBLE PRECISION,
    volatility       DOUBLE PRECISION,
    tick_count       INTEGER,
    price_change     DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    label            VARCHAR(10),                   -- menguat / melemah / stabil
    source           VARCHAR(50),
    created_at       TIMESTAMP        DEFAULT NOW()
);

-- -------------------------------------------------------------
-- [JOJO] GOLD: Ringkasan harian kurs EUR/USD
--   → Dipakai sebagai input modelling bersama
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kurs_daily (
    id               SERIAL           PRIMARY KEY,
    trade_date       DATE             NOT NULL,
    symbol           VARCHAR(20)      NOT NULL,     -- EURUSD=X
    open_price       DOUBLE PRECISION,
    high_price       DOUBLE PRECISION,
    low_price        DOUBLE PRECISION,
    close_price      DOUBLE PRECISION,
    avg_price        DOUBLE PRECISION,
    volatility       DOUBLE PRECISION,
    price_change     DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    ma5              DOUBLE PRECISION,              -- moving average 5 hari
    ma10             DOUBLE PRECISION,              -- moving average 10 hari
    tick_count       INTEGER,
    label            VARCHAR(10),                   -- menguat / melemah / stabil
    updated_at       TIMESTAMP        DEFAULT NOW(),
    UNIQUE (trade_date, symbol)
);


-- =============================================================
-- [RAFAH] Buat tabel ini di database yang sama
-- =============================================================

-- BRONZE: Raw harga komoditas
CREATE TABLE IF NOT EXISTS commodity_raw (
    id          SERIAL           PRIMARY KEY,
    commodity   VARCHAR(30)      NOT NULL,          -- gold, wti, brent, natgas, copper
    symbol      VARCHAR(20)      NOT NULL,          -- GC=F, CL=F, BZ=F, NG=F, HG=F
    price       DOUBLE PRECISION NOT NULL,
    event_time  TIMESTAMP        NOT NULL,
    source      VARCHAR(50),
    ingested_at TIMESTAMP        DEFAULT NOW()
);

-- SILVER: Aggregasi per window komoditas
CREATE TABLE IF NOT EXISTS commodity_silver (
    id               SERIAL           PRIMARY KEY,
    commodity        VARCHAR(30)      NOT NULL,
    symbol           VARCHAR(20)      NOT NULL,
    window_start     TIMESTAMP        NOT NULL,
    window_end       TIMESTAMP        NOT NULL,
    open_price       DOUBLE PRECISION,
    close_price      DOUBLE PRECISION,
    avg_price        DOUBLE PRECISION,
    volatility       DOUBLE PRECISION,
    tick_count       INTEGER,
    price_change     DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    source           VARCHAR(50),
    created_at       TIMESTAMP        DEFAULT NOW()
);

-- GOLD: Ringkasan harian semua komoditas dalam satu baris (pivot)
--   → Dipakai sebagai input modelling bersama
CREATE TABLE IF NOT EXISTS commodity_daily (
    id               SERIAL           PRIMARY KEY,
    trade_date       DATE             NOT NULL      UNIQUE,
    -- WTI Crude Oil
    wti_open         DOUBLE PRECISION,
    wti_close        DOUBLE PRECISION,
    wti_change_pct   DOUBLE PRECISION,
    wti_ma5          DOUBLE PRECISION,
    -- Brent Crude Oil
    brent_open       DOUBLE PRECISION,
    brent_close      DOUBLE PRECISION,
    brent_change_pct DOUBLE PRECISION,
    brent_ma5        DOUBLE PRECISION,
    -- Gold
    gold_open        DOUBLE PRECISION,
    gold_close       DOUBLE PRECISION,
    gold_change_pct  DOUBLE PRECISION,
    gold_ma5         DOUBLE PRECISION,
    -- Natural Gas
    natgas_open      DOUBLE PRECISION,
    natgas_close     DOUBLE PRECISION,
    natgas_change_pct DOUBLE PRECISION,
    natgas_ma5       DOUBLE PRECISION,
    -- Copper
    copper_open      DOUBLE PRECISION,
    copper_close     DOUBLE PRECISION,
    copper_change_pct DOUBLE PRECISION,
    copper_ma5       DOUBLE PRECISION,
    updated_at       TIMESTAMP        DEFAULT NOW()
);


-- =============================================================
-- [RAMBAT] Buat tabel ini di database yang sama
-- =============================================================

-- BRONZE: Raw berita mentah
CREATE TABLE IF NOT EXISTS news_raw (
    id             SERIAL       PRIMARY KEY,
    news_id        VARCHAR(100) UNIQUE,             -- hash unik artikel
    title          TEXT,
    content        TEXT,
    source         VARCHAR(50),                     -- ecb / guardian / gdelt / newsapi
    published_at   TIMESTAMP,
    url            TEXT,
    language       VARCHAR(10),
    keyword_match  TEXT[],                          -- array keyword yang match
    ingestion_time TIMESTAMP    DEFAULT NOW()
);

-- SILVER: Berita bersih setelah NLP preprocessing
CREATE TABLE IF NOT EXISTS news_clean (
    id              SERIAL       PRIMARY KEY,
    news_id         VARCHAR(100) REFERENCES news_raw(news_id),
    title_clean     TEXT,
    content_clean   TEXT,
    tokens          TEXT[],
    sentiment_score DOUBLE PRECISION,               -- -1.0 s/d 1.0
    sentiment_label VARCHAR(10),                    -- positif / negatif / netral
    source          VARCHAR(50),
    source_tier     INTEGER,
    category        VARCHAR(50),
    published_at    TIMESTAMP,
    processed_at    TIMESTAMP    DEFAULT NOW()
);

-- GOLD: Aggregasi sentimen harian
--   → Dipakai sebagai input modelling bersama
CREATE TABLE IF NOT EXISTS sentiment_daily (
    id                   SERIAL           PRIMARY KEY,
    trade_date           DATE             NOT NULL UNIQUE,
    avg_sentiment        DOUBLE PRECISION,           -- rata-rata skor sentimen
    positive_count       INTEGER,                    -- jumlah berita positif
    negative_count       INTEGER,                    -- jumlah berita negatif
    neutral_count        INTEGER,
    total_news           INTEGER,
    sentiment_volatility DOUBLE PRECISION,           -- stddev skor sentimen
    dominant_sentiment   VARCHAR(10),                -- label dominan hari itu
    updated_at           TIMESTAMP        DEFAULT NOW()
);


-- =============================================================
-- GOLD LAYER BERSAMA: View untuk modelling & dashboard
-- Menggabungkan kurs + komoditas + sentimen per hari
-- =============================================================
CREATE OR REPLACE VIEW v_market_signals AS
SELECT
    k.trade_date,

    -- [JOJO] Kurs EUR/USD
    k.open_price                      AS kurs_open,
    k.close_price                     AS kurs_close,
    k.high_price                      AS kurs_high,
    k.low_price                       AS kurs_low,
    k.price_change_pct                AS kurs_change_pct,
    k.volatility                      AS kurs_volatility,
    k.ma5                             AS kurs_ma5,
    k.ma10                            AS kurs_ma10,
    k.label                           AS kurs_label,

    -- [RAFAH] Komoditas
    cd.wti_close,
    cd.wti_change_pct,
    cd.wti_ma5,
    cd.brent_close,
    cd.brent_change_pct,
    cd.gold_close,
    cd.gold_change_pct,
    cd.gold_ma5,
    cd.natgas_close,
    cd.natgas_change_pct,
    cd.copper_close,
    cd.copper_change_pct,

    -- [RAMBAT] Sentimen
    sd.avg_sentiment,
    sd.positive_count,
    sd.negative_count,
    sd.total_news,
    sd.sentiment_volatility,
    sd.dominant_sentiment

FROM kurs_daily k
LEFT JOIN commodity_daily  cd ON cd.trade_date = k.trade_date
LEFT JOIN sentiment_daily  sd ON sd.trade_date = k.trade_date
WHERE k.symbol = 'EURUSD=X'
ORDER BY k.trade_date DESC;

-- =============================================================
-- CARA CONNECT KE DATABASE INI:
--
--   Host     : localhost
--   Port     : 5433
--   Database : kurs_eur_db
--   User     : kursadmin
--   Password : kursadmin
--
-- Dari Python (psycopg2):
--   conn = psycopg2.connect(
--       host="localhost", port=5433,
--       dbname="kurs_eur_db",
--       user="kursadmin", password="kursadmin"
--   )
--
-- Dari terminal Docker:
--   docker exec -it postgres_kurs_eur psql -U kursadmin -d kurs_eur_db
--
-- Query untuk modelling:
--   SELECT * FROM v_market_signals;
-- =============================================================
