-- Crypto Scout tables and policies (TimescaleDB PG17)

-- =========================
-- CMC FEAR & GREED INDEX (FGI)
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.cmc_fgi (
    value INTEGER NOT NULL,
    value_classification TEXT NOT NULL CHECK (value_classification IN ('Extreme Fear','Fear','Neutral','Greed','Extreme Greed')),
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fgi_pkey PRIMARY KEY (update_time)
);

ALTER TABLE crypto_scout.cmc_fgi OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_cmc_fgi_update_time ON crypto_scout.cmc_fgi(update_time DESC);
SELECT public.create_hypertable('crypto_scout.cmc_fgi', 'update_time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

ALTER TABLE crypto_scout.cmc_fgi SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'update_time DESC'
);

SELECT public.add_compression_policy('crypto_scout.cmc_fgi', INTERVAL '30 days');
SELECT public.add_reorder_policy('crypto_scout.cmc_fgi', 'idx_cmc_fgi_update_time');

-- =========================
-- KLINE TABLES (1d/1w)
-- Schema is identical across intervals.
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.cmc_kline_1d (
    symbol TEXT NOT NULL,
    time_open TIMESTAMP WITH TIME ZONE NOT NULL,
    time_close TIMESTAMP WITH TIME ZONE NOT NULL,
    time_high TIMESTAMP WITH TIME ZONE NOT NULL,
    time_low  TIMESTAMP WITH TIME ZONE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION NOT NULL,
    circulating_supply BIGINT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT cmc_kline_1d_pkey PRIMARY KEY (symbol, timestamp)
);

ALTER TABLE crypto_scout.cmc_kline_1d OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_cmc_kline_1d_symbol_time ON crypto_scout.cmc_kline_1d(symbol, timestamp DESC);
SELECT public.create_hypertable('crypto_scout.cmc_kline_1d', 'timestamp', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

ALTER TABLE crypto_scout.cmc_kline_1d SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.cmc_kline_1d', INTERVAL '30 days');
SELECT public.add_reorder_policy('crypto_scout.cmc_kline_1d', 'idx_cmc_kline_1d_symbol_time');

CREATE TABLE IF NOT EXISTS crypto_scout.cmc_kline_1w (
    symbol TEXT NOT NULL,
    time_open TIMESTAMP WITH TIME ZONE NOT NULL,
    time_close TIMESTAMP WITH TIME ZONE NOT NULL,
    time_high TIMESTAMP WITH TIME ZONE NOT NULL,
    time_low  TIMESTAMP WITH TIME ZONE NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION NOT NULL,
    circulating_supply BIGINT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT cmc_kline_1w_pkey PRIMARY KEY (symbol, timestamp)
);

ALTER TABLE crypto_scout.cmc_kline_1w OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_cmc_kline_1w_symbol_time ON crypto_scout.cmc_kline_1w(symbol, timestamp DESC);
SELECT public.create_hypertable('crypto_scout.cmc_kline_1w', 'timestamp', chunk_time_interval => INTERVAL '3 months', if_not_exists => TRUE);

ALTER TABLE crypto_scout.cmc_kline_1w SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.cmc_kline_1w', INTERVAL '30 days');
SELECT public.add_reorder_policy('crypto_scout.cmc_kline_1w', 'idx_cmc_kline_1w_symbol_time');

-- =========================
-- BTC PRICE RISK (risk-to-price mapping)
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.btc_price_risk (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    risk DOUBLE PRECISION NOT NULL,
    price_of_price_risk BIGINT NOT NULL,
    price_of_composite_risk BIGINT NOT NULL,
    CONSTRAINT btc_price_risk_pkey PRIMARY KEY (timestamp, risk)
);

ALTER TABLE crypto_scout.btc_price_risk OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_btc_price_risk_timestamp ON crypto_scout.btc_price_risk(timestamp DESC);
SELECT public.create_hypertable('crypto_scout.btc_price_risk', 'timestamp', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

ALTER TABLE crypto_scout.btc_price_risk SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'risk',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.btc_price_risk', INTERVAL '30 days');
SELECT public.add_reorder_policy('crypto_scout.btc_price_risk', 'idx_btc_price_risk_timestamp');

-- =========================
-- BTC RISK PRICE (current risk assessment)
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.btc_risk_price (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    price BIGINT NOT NULL,
    price_risk DOUBLE PRECISION NOT NULL,
    composite_risk DOUBLE PRECISION NOT NULL,
    CONSTRAINT btc_risk_price_pkey PRIMARY KEY (timestamp)
);

ALTER TABLE crypto_scout.btc_risk_price OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_btc_risk_price_timestamp ON crypto_scout.btc_risk_price(timestamp DESC);
SELECT public.create_hypertable('crypto_scout.btc_risk_price', 'timestamp', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

ALTER TABLE crypto_scout.btc_risk_price SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.btc_risk_price', INTERVAL '30 days');
SELECT public.add_reorder_policy('crypto_scout.btc_risk_price', 'idx_btc_risk_price_timestamp');