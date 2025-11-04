-- Ensure required extensions are installed in the target database
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create crypto_scout schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS crypto_scout;

-- Set the search path to include all necessary schemas
SET search_path TO public, crypto_scout;

-- Persist search_path defaults for the database and application role
ALTER DATABASE crypto_scout SET search_path TO public, crypto_scout;
ALTER ROLE crypto_scout_db IN DATABASE crypto_scout SET search_path TO public, crypto_scout;

-- Track last processed offsets per stream (external offset tracking)
CREATE TABLE IF NOT EXISTS crypto_scout.stream_offsets (
    stream TEXT PRIMARY KEY,
    "offset" BIGINT NOT NULL CHECK ("offset" >= 0),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Set ownership to application role
ALTER TABLE crypto_scout.stream_offsets OWNER TO crypto_scout_db;

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA crypto_scout TO crypto_scout_db;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crypto_scout TO crypto_scout_db;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crypto_scout TO crypto_scout_db;

-- Ensure future objects created by the application role have appropriate privileges
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO crypto_scout_db;
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT USAGE, SELECT ON SEQUENCES TO crypto_scout_db;

-- Bybit Spot tables and policies (TimescaleDB PG17)
-- Safe to run on initial bootstrap; idempotent DDL where possible

-- =========================
-- SPOT TICKERS
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_tickers (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    cross_sequence BIGINT NOT NULL,
    last_price NUMERIC(20, 2) NOT NULL,
    high_price_24h NUMERIC(20, 2) NOT NULL,
    low_price_24h NUMERIC(20, 2) NOT NULL,
    prev_price_24h NUMERIC(20, 2) NOT NULL,
    volume_24h NUMERIC(20, 8) NOT NULL,
    turnover_24h NUMERIC(20, 4) NOT NULL,
    price_24h_pcnt NUMERIC(10, 4) NOT NULL,
    usd_index_price NUMERIC(20, 6),
    CONSTRAINT bybit_spot_tickers_pkey PRIMARY KEY (id, timestamp)
);
alter table crypto_scout.bybit_spot_tickers OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_tickers_timestamp ON crypto_scout.bybit_spot_tickers(timestamp DESC);
create index IF NOT EXISTS idx_bybit_spot_tickers_symbol_timestamp ON crypto_scout.bybit_spot_tickers(symbol, timestamp DESC);
select public.create_hypertable('crypto_scout.bybit_spot_tickers', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_spot_tickers set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_timestamp');

-- =========================
-- KLINE TABLES (1m/5m/15m/60m/240m/1d)
-- Schema is identical across intervals. Only confirmed klines should be inserted by the app.
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_1m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_1m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_1m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_1m_start_time ON crypto_scout.bybit_spot_kline_1m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_1m_symbol_start ON crypto_scout.bybit_spot_kline_1m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_1m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_5m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_5m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_5m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_5m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_5m_start_time ON crypto_scout.bybit_spot_kline_5m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_5m_symbol_start ON crypto_scout.bybit_spot_kline_5m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_5m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_15m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_15m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_15m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_15m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_15m_start_time ON crypto_scout.bybit_spot_kline_15m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_15m_symbol_start ON crypto_scout.bybit_spot_kline_15m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_15m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_60m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_60m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_60m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_60m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_60m_start_time ON crypto_scout.bybit_spot_kline_60m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_60m_symbol_start ON crypto_scout.bybit_spot_kline_60m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_60m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_240m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_240m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_240m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_240m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_240m_start_time ON crypto_scout.bybit_spot_kline_240m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_240m_symbol_start ON crypto_scout.bybit_spot_kline_240m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_240m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1d (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_spot_kline_1d_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_spot_kline_1d_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_spot_kline_1d OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_1d_start_time ON crypto_scout.bybit_spot_kline_1d(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_1d_symbol_start ON crypto_scout.bybit_spot_kline_1d(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_1d', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for kline tables
alter table crypto_scout.bybit_spot_kline_1m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_spot_kline_5m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_spot_kline_15m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_spot_kline_60m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_spot_kline_240m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_spot_kline_1d set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);

-- Reorder policies for kline tables
select add_reorder_policy('crypto_scout.bybit_spot_kline_1m', 'idx_bybit_spot_kline_1m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_5m', 'idx_bybit_spot_kline_5m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_15m', 'idx_bybit_spot_kline_15m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_60m', 'idx_bybit_spot_kline_60m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_240m', 'idx_bybit_spot_kline_240m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_1d', 'idx_bybit_spot_kline_1d_start_time');

-- =========================
-- PUBLIC TRADES (normalized: 1 row per trade)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_public_trade (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_id TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    taker_side TEXT NOT NULL CHECK (taker_side IN ('Buy','Sell')),
    cross_sequence BIGINT NOT NULL,
    is_block_trade BOOLEAN NOT NULL,
    is_rpi BOOLEAN NOT NULL,
    CONSTRAINT bybit_spot_public_trade_pkey PRIMARY KEY (id, trade_time),
    CONSTRAINT bybit_spot_public_trade_symbol_tradeid_uniq UNIQUE (symbol, trade_id, trade_time)
);
alter table crypto_scout.bybit_spot_public_trade OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_public_trade_trade_time ON crypto_scout.bybit_spot_public_trade(trade_time DESC);
create index IF NOT EXISTS idx_bybit_spot_public_trade_symbol_time ON crypto_scout.bybit_spot_public_trade(symbol, trade_time DESC);
create index IF NOT EXISTS idx_bybit_spot_public_trade_seq ON crypto_scout.bybit_spot_public_trade(cross_sequence);
select public.create_hypertable('crypto_scout.bybit_spot_public_trade', 'trade_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_spot_public_trade set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'trade_time DESC, trade_id DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_spot_public_trade', 'idx_bybit_spot_public_trade_trade_time');

-- =========================
-- ORDER BOOKS (1/50/200/1000)
-- Schema is identical across depths. (normalized: 1 row per level)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_1 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_spot_order_book_1_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_spot_order_book_1 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_order_book_1_engine_time ON crypto_scout.bybit_spot_order_book_1(engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_1_symbol_time ON crypto_scout.bybit_spot_order_book_1(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_1_symbol_side_price ON crypto_scout.bybit_spot_order_book_1(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_spot_order_book_1', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_50 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_spot_order_book_50_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_spot_order_book_50 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_order_book_50_engine_time ON crypto_scout.bybit_spot_order_book_50(engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_50_symbol_time ON crypto_scout.bybit_spot_order_book_50(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_50_symbol_side_price ON crypto_scout.bybit_spot_order_book_50(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_spot_order_book_50', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_200 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_spot_order_book_200_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_spot_order_book_200 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_order_book_200_engine_time ON crypto_scout.bybit_spot_order_book_200(engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_200_symbol_time ON crypto_scout.bybit_spot_order_book_200(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_200_symbol_side_price ON crypto_scout.bybit_spot_order_book_200(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_spot_order_book_200', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_order_book_1000 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_spot_order_book_1000_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_spot_order_book_1000 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_order_book_1000_engine_time ON crypto_scout.bybit_spot_order_book_1000(engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_1000_symbol_time ON crypto_scout.bybit_spot_order_book_1000(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_spot_order_book_1000_symbol_side_price ON crypto_scout.bybit_spot_order_book_1000(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_spot_order_book_1000', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for order book tables
alter table crypto_scout.bybit_spot_order_book_1 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_spot_order_book_50 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_spot_order_book_200 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_spot_order_book_1000 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);

-- Reorder policies for order book tables
select add_reorder_policy('crypto_scout.bybit_spot_order_book_1', 'idx_bybit_spot_order_book_1_engine_time');
select add_reorder_policy('crypto_scout.bybit_spot_order_book_50', 'idx_bybit_spot_order_book_50_engine_time');
select add_reorder_policy('crypto_scout.bybit_spot_order_book_200', 'idx_bybit_spot_order_book_200_engine_time');
select add_reorder_policy('crypto_scout.bybit_spot_order_book_1000', 'idx_bybit_spot_order_book_1000_engine_time');

-- Bybit Linear (Perps/Futures) tables and policies (TimescaleDB PG17)
-- Safe to run on initial bootstrap; idempotent DDL where possible

-- =========================
-- LINEAR TICKERS
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_tickers (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    cross_sequence BIGINT NOT NULL,
    last_price NUMERIC(20, 2) NOT NULL,
    high_price_24h NUMERIC(20, 2) NOT NULL,
    low_price_24h NUMERIC(20, 2) NOT NULL,
    prev_price_24h NUMERIC(20, 2) NOT NULL,
    volume_24h NUMERIC(20, 8) NOT NULL,
    turnover_24h NUMERIC(20, 4) NOT NULL,
    price_24h_pcnt NUMERIC(10, 4) NOT NULL,
    usd_index_price NUMERIC(20, 6),
    CONSTRAINT bybit_linear_tickers_pkey PRIMARY KEY (id, timestamp)
);
alter table crypto_scout.bybit_linear_tickers OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_tickers_timestamp ON crypto_scout.bybit_linear_tickers(timestamp DESC);
create index IF NOT EXISTS idx_bybit_linear_tickers_symbol_timestamp ON crypto_scout.bybit_linear_tickers(symbol, timestamp DESC);
select public.create_hypertable('crypto_scout.bybit_linear_tickers', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_linear_tickers set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_linear_tickers', 'idx_bybit_linear_tickers_timestamp');

-- =========================
-- KLINE TABLES (1m/5m/15m/60m/240m/1d)
-- Schema is identical across intervals. Only confirmed klines should be inserted by the app.
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_1m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_1m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_1m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_1m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_1m_start_time ON crypto_scout.bybit_linear_kline_1m(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_1m_symbol_start ON crypto_scout.bybit_linear_kline_1m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_1m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_5m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_5m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_5m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_5m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_5m_start_time ON crypto_scout.bybit_linear_kline_5m(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_5m_symbol_start ON crypto_scout.bybit_linear_kline_5m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_5m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_15m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_15m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_15m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_15m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_15m_start_time ON crypto_scout.bybit_linear_kline_15m(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_15m_symbol_start ON crypto_scout.bybit_linear_kline_15m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_15m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_60m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_60m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_60m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_60m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_60m_start_time ON crypto_scout.bybit_linear_kline_60m(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_60m_symbol_start ON crypto_scout.bybit_linear_kline_60m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_60m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_240m (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_240m_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_240m_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_240m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_240m_start_time ON crypto_scout.bybit_linear_kline_240m(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_240m_symbol_start ON crypto_scout.bybit_linear_kline_240m(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_240m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_kline_1d (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    turnover NUMERIC(20, 8) NOT NULL,
    last_trade_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_linear_kline_1d_pkey PRIMARY KEY (id, start_time),
    CONSTRAINT bybit_linear_kline_1d_symbol_start_uniq UNIQUE (symbol, start_time)
);
alter table crypto_scout.bybit_linear_kline_1d OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_kline_1d_start_time ON crypto_scout.bybit_linear_kline_1d(start_time DESC);
create index IF NOT EXISTS idx_bybit_linear_kline_1d_symbol_start ON crypto_scout.bybit_linear_kline_1d(symbol, start_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_kline_1d', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for kline tables
alter table crypto_scout.bybit_linear_kline_1m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_linear_kline_5m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_linear_kline_15m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_linear_kline_60m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_linear_kline_240m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
alter table crypto_scout.bybit_linear_kline_1d set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);

-- Reorder policies for kline tables
select add_reorder_policy('crypto_scout.bybit_linear_kline_1m', 'idx_bybit_linear_kline_1m_start_time');
select add_reorder_policy('crypto_scout.bybit_linear_kline_5m', 'idx_bybit_linear_kline_5m_start_time');
select add_reorder_policy('crypto_scout.bybit_linear_kline_15m', 'idx_bybit_linear_kline_15m_start_time');
select add_reorder_policy('crypto_scout.bybit_linear_kline_60m', 'idx_bybit_linear_kline_60m_start_time');
select add_reorder_policy('crypto_scout.bybit_linear_kline_240m', 'idx_bybit_linear_kline_240m_start_time');
select add_reorder_policy('crypto_scout.bybit_linear_kline_1d', 'idx_bybit_linear_kline_1d_start_time');

-- =========================
-- PUBLIC TRADES (normalized: 1 row per trade)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_public_trade (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_id TEXT NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    taker_side TEXT NOT NULL CHECK (taker_side IN ('Buy','Sell')),
    tick_direction TEXT,
    cross_sequence BIGINT NOT NULL,
    is_block_trade BOOLEAN NOT NULL,
    is_rpi BOOLEAN NOT NULL,
    CONSTRAINT bybit_linear_public_trade_pkey PRIMARY KEY (id, trade_time),
    CONSTRAINT bybit_linear_public_trade_symbol_tradeid_uniq UNIQUE (symbol, trade_id, trade_time)
);
alter table crypto_scout.bybit_linear_public_trade OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_public_trade_trade_time ON crypto_scout.bybit_linear_public_trade(trade_time DESC);
create index IF NOT EXISTS idx_bybit_linear_public_trade_symbol_time ON crypto_scout.bybit_linear_public_trade(symbol, trade_time DESC);
create index IF NOT EXISTS idx_bybit_linear_public_trade_seq ON crypto_scout.bybit_linear_public_trade(cross_sequence);
select public.create_hypertable('crypto_scout.bybit_linear_public_trade', 'trade_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_linear_public_trade set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'trade_time DESC, trade_id DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_linear_public_trade', 'idx_bybit_linear_public_trade_trade_time');

-- =========================
-- ORDER BOOKS (1/50/200/1000)
-- Schema is identical across depths. (normalized: 1 row per level)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_order_book_1 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_linear_order_book_1_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_linear_order_book_1 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_order_book_1_engine_time ON crypto_scout.bybit_linear_order_book_1(engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_1_symbol_time ON crypto_scout.bybit_linear_order_book_1(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_1_symbol_side_price ON crypto_scout.bybit_linear_order_book_1(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_linear_order_book_1', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_order_book_50 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_linear_order_book_50_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_linear_order_book_50 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_order_book_50_engine_time ON crypto_scout.bybit_linear_order_book_50(engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_50_symbol_time ON crypto_scout.bybit_linear_order_book_50(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_50_symbol_side_price ON crypto_scout.bybit_linear_order_book_50(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_linear_order_book_50', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_order_book_200 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_linear_order_book_200_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_linear_order_book_200 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_order_book_200_engine_time ON crypto_scout.bybit_linear_order_book_200(engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_200_symbol_time ON crypto_scout.bybit_linear_order_book_200(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_200_symbol_side_price ON crypto_scout.bybit_linear_order_book_200(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_linear_order_book_200', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);


create TABLE IF NOT EXISTS crypto_scout.bybit_linear_order_book_1000 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    CONSTRAINT bybit_linear_order_book_1000_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_linear_order_book_1000 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_order_book_1000_engine_time ON crypto_scout.bybit_linear_order_book_1000(engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_1000_symbol_time ON crypto_scout.bybit_linear_order_book_1000(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_1000_symbol_side_price ON crypto_scout.bybit_linear_order_book_1000(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_linear_order_book_1000', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for order book tables
alter table crypto_scout.bybit_linear_order_book_1 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_linear_order_book_50 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_linear_order_book_200 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_linear_order_book_1000 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);

-- Reorder policies for order book tables
select add_reorder_policy('crypto_scout.bybit_linear_order_book_1', 'idx_bybit_linear_order_book_1_engine_time');
select add_reorder_policy('crypto_scout.bybit_linear_order_book_50', 'idx_bybit_linear_order_book_50_engine_time');
select add_reorder_policy('crypto_scout.bybit_linear_order_book_200', 'idx_bybit_linear_order_book_200_engine_time');
select add_reorder_policy('crypto_scout.bybit_linear_order_book_1000', 'idx_bybit_linear_order_book_1000_engine_time');

-- =========================
-- ALL LIQUIDATION (normalized)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_all_liqudation (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    position_side TEXT NOT NULL CHECK (position_side IN ('Buy','Sell')),
    executed_size NUMERIC(20, 8) NOT NULL,
    bankruptcy_price NUMERIC(20, 8) NOT NULL,
    CONSTRAINT bybit_linear_all_liqudation_pkey PRIMARY KEY (id, event_time)
);
alter table crypto_scout.bybit_linear_all_liqudation OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_all_liqudation_event_time ON crypto_scout.bybit_linear_all_liqudation(event_time DESC);
create index IF NOT EXISTS idx_bybit_linear_all_liqudation_symbol_time ON crypto_scout.bybit_linear_all_liqudation(symbol, event_time DESC);
select public.create_hypertable('crypto_scout.bybit_linear_all_liqudation', 'event_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_linear_all_liqudation set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, position_side',
    timescaledb.compress_orderby = 'event_time DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_linear_all_liqudation', 'idx_bybit_linear_all_liqudation_event_time');

-- =========================
-- BYBIT LAUNCH POOL (LPL)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_lpl (
    id BIGSERIAL,
    return_coin TEXT NOT NULL,
    return_coin_icon TEXT NOT NULL,
    description TEXT NOT NULL,
    website TEXT NOT NULL,
    whitepaper TEXT NOT NULL,
    rules TEXT NOT NULL,
    stake_begin_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stake_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_begin_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_lpl_pkey PRIMARY KEY (id, stake_begin_time)
);

alter table crypto_scout.bybit_lpl OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_lpl_stake_begin_time ON crypto_scout.bybit_lpl(stake_begin_time DESC);
select public.create_hypertable('crypto_scout.bybit_lpl', 'stake_begin_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_lpl set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'return_coin',
    timescaledb.compress_orderby = 'stake_begin_time DESC, id DESC'
);
select add_reorder_policy('crypto_scout.bybit_lpl', 'idx_bybit_lpl_stake_begin_time');

-- =========================
-- CMC FEAR & GREED INDEX (FGI)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.cmc_fgi (
    id BIGSERIAL,
    score INTEGER NOT NULL,
    name TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    btc_price NUMERIC(20, 2) NOT NULL,
    btc_volume NUMERIC(20, 2) NOT NULL,
    CONSTRAINT fgi_pkey PRIMARY KEY (id, timestamp)
);

alter table crypto_scout.cmc_fgi OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_cmc_fgi_timestamp ON crypto_scout.cmc_fgi(timestamp DESC);
select public.create_hypertable('crypto_scout.cmc_fgi', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.cmc_fgi set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'name',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);
select add_reorder_policy('crypto_scout.cmc_fgi', 'idx_cmc_fgi_timestamp');