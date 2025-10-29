-- Bybit Linear (Perps/Futures) tables and policies (TimescaleDB PG17)
-- Safe to run on initial bootstrap; idempotent DDL where possible

-- Ensure required extensions and schema
create EXTENSION IF NOT EXISTS timescaledb;
create SCHEMA IF NOT EXISTS crypto_scout;
SET search_path TO public, crypto_scout;

-- =========================
-- LINEAR TICKERS
-- =========================
-- Note: Schema mirrors spot tickers to keep analytics consistent across markets.
--       Adjustments can be applied if Bybit linear adds/removes fields.

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
select add_compression_policy('crypto_scout.bybit_linear_tickers', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_linear_tickers', 'idx_bybit_linear_tickers_timestamp');
select add_retention_policy('crypto_scout.bybit_linear_tickers', interval '180 days');

-- =========================
-- KLINE 60m (confirmed only)
-- =========================

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

alter table crypto_scout.bybit_linear_kline_60m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC, id DESC'
);
select add_compression_policy('crypto_scout.bybit_linear_kline_60m', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_linear_kline_60m', 'idx_bybit_linear_kline_60m_start_time');
select add_retention_policy('crypto_scout.bybit_linear_kline_60m', interval '730 days');

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
    timescaledb.compress_orderby = 'trade_time DESC, id DESC'
);
select add_compression_policy('crypto_scout.bybit_linear_public_trade', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_linear_public_trade', 'idx_bybit_linear_public_trade_trade_time');
select add_retention_policy('crypto_scout.bybit_linear_public_trade', interval '180 days');

-- =========================
-- ORDER BOOK 200 (normalized: 1 row per level)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_linear_order_book_200 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('Bid','Ask')),
    price NUMERIC(20, 8) NOT NULL,
    size NUMERIC(20, 8) NOT NULL,
    update_id BIGINT NOT NULL,
    cross_sequence BIGINT NOT NULL,
    is_snapshot BOOLEAN NOT NULL,
    CONSTRAINT bybit_linear_order_book_200_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_linear_order_book_200 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_linear_order_book_200_engine_time ON crypto_scout.bybit_linear_order_book_200(engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_200_symbol_time ON crypto_scout.bybit_linear_order_book_200(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_linear_order_book_200_symbol_side_price ON crypto_scout.bybit_linear_order_book_200(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_linear_order_book_200', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_linear_order_book_200 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
select add_compression_policy('crypto_scout.bybit_linear_order_book_200', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_linear_order_book_200', 'idx_bybit_linear_order_book_200_engine_time');
select add_retention_policy('crypto_scout.bybit_linear_order_book_200', interval '7 days');

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
select add_compression_policy('crypto_scout.bybit_linear_all_liqudation', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_linear_all_liqudation', 'idx_bybit_linear_all_liqudation_event_time');
select add_retention_policy('crypto_scout.bybit_linear_all_liqudation', interval '730 days');
