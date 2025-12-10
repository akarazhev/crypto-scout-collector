-- Bybit Spot tables and policies (TimescaleDB PG17)

-- =========================
-- SPOT TICKERS
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_tickers (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    last_price DOUBLE PRECISION NOT NULL,
    high_price_24h DOUBLE PRECISION NOT NULL,
    low_price_24h DOUBLE PRECISION NOT NULL,
    prev_price_24h DOUBLE PRECISION NOT NULL,
    volume_24h DOUBLE PRECISION NOT NULL,
    turnover_24h DOUBLE PRECISION NOT NULL,
    price_24h_pcnt DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_tickers_pkey PRIMARY KEY (symbol, timestamp)
);

ALTER TABLE crypto_scout.bybit_spot_tickers OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_tickers_symbol_time ON crypto_scout.bybit_spot_tickers(symbol, timestamp DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_tickers', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE crypto_scout.bybit_spot_tickers SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.bybit_spot_tickers', interval '1 month');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_symbol_time');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_tickers', interval '365 days');

-- =========================
-- KLINE TABLES (1m/5m/15m/60m/240m/1d)
-- Schema is identical across intervals. Only confirmed klines should be inserted by the app.
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1m (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_1m_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_1m OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_1m_end_time ON crypto_scout.bybit_spot_kline_1m(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_1m_symbol_start_time ON crypto_scout.bybit_spot_kline_1m(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_1m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_5m (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_5m_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_5m OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_5m_end_time ON crypto_scout.bybit_spot_kline_5m(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_5m_symbol_start_time ON crypto_scout.bybit_spot_kline_5m(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_5m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_15m (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_15m_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_15m OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_15m_end_time ON crypto_scout.bybit_spot_kline_15m(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_15m_symbol_start_time ON crypto_scout.bybit_spot_kline_15m(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_15m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_60m (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_60m_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_60m OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_60m_end_time ON crypto_scout.bybit_spot_kline_60m(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_60m_symbol_start_time ON crypto_scout.bybit_spot_kline_60m(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_60m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_240m (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_240m_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_240m OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_240m_end_time ON crypto_scout.bybit_spot_kline_240m(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_240m_symbol_start_time ON crypto_scout.bybit_spot_kline_240m(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_240m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1d (
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DOUBLE PRECISION NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    high_price DOUBLE PRECISION NOT NULL,
    low_price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    turnover DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_kline_1d_pkey PRIMARY KEY (symbol, start_time)
);

ALTER TABLE crypto_scout.bybit_spot_kline_1d OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_1d_end_time ON crypto_scout.bybit_spot_kline_1d(symbol, end_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_kline_1d_symbol_start_time ON crypto_scout.bybit_spot_kline_1d(symbol, start_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_kline_1d', 'start_time', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

ALTER TABLE crypto_scout.bybit_spot_kline_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
ALTER TABLE crypto_scout.bybit_spot_kline_5m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
ALTER TABLE crypto_scout.bybit_spot_kline_15m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
ALTER TABLE crypto_scout.bybit_spot_kline_60m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
ALTER TABLE crypto_scout.bybit_spot_kline_240m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
ALTER TABLE crypto_scout.bybit_spot_kline_1d SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);

SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_1m', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_5m', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_15m', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_60m', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_240m', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_kline_1d', interval '1 month');

SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_1m', 'idx_bybit_spot_kline_1m_symbol_start_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_5m', 'idx_bybit_spot_kline_5m_symbol_start_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_15m', 'idx_bybit_spot_kline_15m_symbol_start_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_60m', 'idx_bybit_spot_kline_60m_symbol_start_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_240m', 'idx_bybit_spot_kline_240m_symbol_start_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_kline_1d', 'idx_bybit_spot_kline_1d_symbol_start_time');

SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_1m', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_5m', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_15m', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_60m', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_240m', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_kline_1d', interval '365 days');

-- =========================
-- PUBLIC TRADES (normalized: 1 row per trade)
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_public_trade (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    taker_side TEXT NOT NULL CHECK (taker_side IN ('Buy','Sell')),
    is_block_trade BOOLEAN NOT NULL,
    is_rpi BOOLEAN NOT NULL,
    CONSTRAINT bybit_spot_public_trade_pkey PRIMARY KEY (symbol, trade_time, id)
);

ALTER TABLE crypto_scout.bybit_spot_public_trade OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_public_trade_trade_time ON crypto_scout.bybit_spot_public_trade(trade_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_public_trade_symbol_time ON crypto_scout.bybit_spot_public_trade(symbol, trade_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_public_trade', 'trade_time', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);

ALTER TABLE crypto_scout.bybit_spot_public_trade SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'trade_time DESC, id DESC'
);

SELECT public.add_compression_policy('crypto_scout.bybit_spot_public_trade', interval '14 days');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_public_trade', 'idx_bybit_spot_public_trade_symbol_time');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_public_trade', interval '365 days');

-- =========================
-- ORDER BOOKS (1/50/200/1000)
-- Schema is identical across depths. (normalized: 1 row per level)
-- =========================

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_1 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_order_book_1_pkey PRIMARY KEY (symbol, engine_time, id)
);

ALTER TABLE crypto_scout.bybit_spot_order_book_1 OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1_engine_time ON crypto_scout.bybit_spot_order_book_1(engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1_symbol_time ON crypto_scout.bybit_spot_order_book_1(symbol, engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1_symbol_side_price ON crypto_scout.bybit_spot_order_book_1(symbol, side, price);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1_symbol_side_engine_time ON crypto_scout.bybit_spot_order_book_1(symbol, side, engine_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_order_book_1', 'engine_time', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_50 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_order_book_50_pkey PRIMARY KEY (symbol, engine_time, id)
);

ALTER TABLE crypto_scout.bybit_spot_order_book_50 OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_50_engine_time ON crypto_scout.bybit_spot_order_book_50(engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_50_symbol_time ON crypto_scout.bybit_spot_order_book_50(symbol, engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_50_symbol_side_price ON crypto_scout.bybit_spot_order_book_50(symbol, side, price);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_50_symbol_side_engine_time ON crypto_scout.bybit_spot_order_book_50(symbol, side, engine_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_order_book_50', 'engine_time', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_200 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_order_book_200_pkey PRIMARY KEY (symbol, engine_time, id)
);

ALTER TABLE crypto_scout.bybit_spot_order_book_200 OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_200_engine_time ON crypto_scout.bybit_spot_order_book_200(engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_200_symbol_time ON crypto_scout.bybit_spot_order_book_200(symbol, engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_200_symbol_side_price ON crypto_scout.bybit_spot_order_book_200(symbol, side, price);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_200_symbol_side_engine_time ON crypto_scout.bybit_spot_order_book_200(symbol, side, engine_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_order_book_200', 'engine_time', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_1000 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_spot_order_book_1000_pkey PRIMARY KEY (symbol, engine_time, id)
);

ALTER TABLE crypto_scout.bybit_spot_order_book_1000 OWNER TO crypto_scout_db;
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1000_engine_time ON crypto_scout.bybit_spot_order_book_1000(engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1000_symbol_time ON crypto_scout.bybit_spot_order_book_1000(symbol, engine_time DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1000_symbol_side_price ON crypto_scout.bybit_spot_order_book_1000(symbol, side, price);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_order_book_1000_symbol_side_engine_time ON crypto_scout.bybit_spot_order_book_1000(symbol, side, engine_time DESC);
SELECT public.create_hypertable('crypto_scout.bybit_spot_order_book_1000', 'engine_time', chunk_time_interval => INTERVAL '6 hours', if_not_exists => TRUE);

ALTER TABLE crypto_scout.bybit_spot_order_book_1 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
ALTER TABLE crypto_scout.bybit_spot_order_book_50 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
ALTER TABLE crypto_scout.bybit_spot_order_book_200 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
ALTER TABLE crypto_scout.bybit_spot_order_book_1000 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);

SELECT public.add_compression_policy('crypto_scout.bybit_spot_order_book_1', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_order_book_50', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_order_book_200', interval '14 days');
SELECT public.add_compression_policy('crypto_scout.bybit_spot_order_book_1000', interval '14 days');

SELECT public.add_reorder_policy('crypto_scout.bybit_spot_order_book_1', 'idx_bybit_spot_order_book_1_symbol_side_engine_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_order_book_50', 'idx_bybit_spot_order_book_50_symbol_side_engine_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_order_book_200', 'idx_bybit_spot_order_book_200_symbol_side_engine_time');
SELECT public.add_reorder_policy('crypto_scout.bybit_spot_order_book_1000', 'idx_bybit_spot_order_book_1000_symbol_side_engine_time');

SELECT public.add_retention_policy('crypto_scout.bybit_spot_order_book_1', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_order_book_50', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_order_book_200', interval '365 days');
SELECT public.add_retention_policy('crypto_scout.bybit_spot_order_book_1000', interval '365 days');