-- Bybit Spot tables and policies (TimescaleDB PG17)

-- =========================
-- SPOT TICKERS
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_tickers (
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
alter table crypto_scout.bybit_spot_tickers OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_tickers_timestamp ON crypto_scout.bybit_spot_tickers(timestamp DESC);
select public.create_hypertable('crypto_scout.bybit_spot_tickers', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_spot_tickers set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);
select add_compression_policy('crypto_scout.bybit_spot_tickers', interval '1 month');
select add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_timestamp');
select add_retention_policy('crypto_scout.bybit_spot_tickers', interval '365 days');

-- =========================
-- KLINE TABLES (1m/5m/15m/60m/240m/1d)
-- Schema is identical across intervals. Only confirmed klines should be inserted by the app.
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1m (
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
alter table crypto_scout.bybit_spot_kline_1m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_1m_start_time ON crypto_scout.bybit_spot_kline_1m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_1m_end_time ON crypto_scout.bybit_spot_kline_1m(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_1m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_5m (
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
alter table crypto_scout.bybit_spot_kline_5m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_5m_start_time ON crypto_scout.bybit_spot_kline_5m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_5m_end_time ON crypto_scout.bybit_spot_kline_5m(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_5m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_15m (
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
alter table crypto_scout.bybit_spot_kline_15m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_15m_start_time ON crypto_scout.bybit_spot_kline_15m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_15m_end_time ON crypto_scout.bybit_spot_kline_15m(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_15m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_60m (
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
alter table crypto_scout.bybit_spot_kline_60m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_60m_start_time ON crypto_scout.bybit_spot_kline_60m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_60m_end_time ON crypto_scout.bybit_spot_kline_60m(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_60m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_240m (
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
alter table crypto_scout.bybit_spot_kline_240m OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_240m_start_time ON crypto_scout.bybit_spot_kline_240m(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_240m_end_time ON crypto_scout.bybit_spot_kline_240m(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_240m', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_spot_kline_1d (
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
alter table crypto_scout.bybit_spot_kline_1d OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_spot_kline_1d_start_time ON crypto_scout.bybit_spot_kline_1d(start_time DESC);
create index IF NOT EXISTS idx_bybit_spot_kline_1d_end_time ON crypto_scout.bybit_spot_kline_1d(symbol, end_time DESC);
select public.create_hypertable('crypto_scout.bybit_spot_kline_1d', 'start_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for kline tables
alter table crypto_scout.bybit_spot_kline_1m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
alter table crypto_scout.bybit_spot_kline_5m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
alter table crypto_scout.bybit_spot_kline_15m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
alter table crypto_scout.bybit_spot_kline_60m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
alter table crypto_scout.bybit_spot_kline_240m set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);
alter table crypto_scout.bybit_spot_kline_1d set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'start_time DESC'
);

-- Compression policies for kline tables
select add_compression_policy('crypto_scout.bybit_spot_kline_1m', interval '1 month');
select add_compression_policy('crypto_scout.bybit_spot_kline_5m', interval '1 month');
select add_compression_policy('crypto_scout.bybit_spot_kline_15m', interval '1 month');
select add_compression_policy('crypto_scout.bybit_spot_kline_60m', interval '1 month');
select add_compression_policy('crypto_scout.bybit_spot_kline_240m', interval '1 month');
select add_compression_policy('crypto_scout.bybit_spot_kline_1d', interval '1 month');

-- Reorder policies for kline tables
select add_reorder_policy('crypto_scout.bybit_spot_kline_1m', 'idx_bybit_spot_kline_1m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_5m', 'idx_bybit_spot_kline_5m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_15m', 'idx_bybit_spot_kline_15m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_60m', 'idx_bybit_spot_kline_60m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_240m', 'idx_bybit_spot_kline_240m_start_time');
select add_reorder_policy('crypto_scout.bybit_spot_kline_1d', 'idx_bybit_spot_kline_1d_start_time');

-- Retention policies for kline tables
select add_retention_policy('crypto_scout.bybit_spot_kline_1m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_5m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_15m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_60m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_240m', interval '365 days');
select add_retention_policy('crypto_scout.bybit_spot_kline_1d', interval '365 days');
