-- Bybit Spot tables and policies (TimescaleDB PG17)

-- =========================
-- SPOT TICKERS
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_tickers (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    last_price NUMERIC(20, 8) NOT NULL,
    high_price_24h NUMERIC(20, 8) NOT NULL,
    low_price_24h NUMERIC(20, 8) NOT NULL,
    prev_price_24h NUMERIC(20, 8) NOT NULL,
    volume_24h NUMERIC(20, 8) NOT NULL,
    turnover_24h NUMERIC(20, 8) NOT NULL,
    price_24h_pcnt NUMERIC(3, 4) NOT NULL,
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
select add_compression_policy('crypto_scout.bybit_spot_tickers', interval '7 days');
select add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_timestamp');
select add_retention_policy('crypto_scout.bybit_spot_tickers', interval '180 days');

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

-- Compression policies for kline tables
select add_compression_policy('crypto_scout.bybit_spot_kline_1m', interval '7 days');
select add_compression_policy('crypto_scout.bybit_spot_kline_5m', interval '7 days');
select add_compression_policy('crypto_scout.bybit_spot_kline_15m', interval '7 days');
select add_compression_policy('crypto_scout.bybit_spot_kline_60m', interval '7 days');
select add_compression_policy('crypto_scout.bybit_spot_kline_240m', interval '7 days');
select add_compression_policy('crypto_scout.bybit_spot_kline_1d', interval '7 days');

-- Reorder policies for kline tables
select add_reorder_policy('crypto_scout.bybit_spot_kline_1m', 'idx_bybit_spot_kline_1m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_5m', 'idx_bybit_spot_kline_5m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_15m', 'idx_bybit_spot_kline_15m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_60m', 'idx_bybit_spot_kline_60m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_240m', 'idx_bybit_spot_kline_240m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_1d', 'idx_bybit_spot_kline_1d_start_time');

-- Retention policies for kline tables
select add_retention_policy('crypto_scout.bybit_spot_kline_1m', interval '90 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_5m', interval '180 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_15m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_60m', interval '730 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_240m', interval '1095 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_1d', interval '1825 days');
