-- CMC parser tables and policies (TimescaleDB PG17)

-- =========================
-- CMC FEAR & GREED INDEX (FGI)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.cmc_fgi (
    id BIGSERIAL,
    value INTEGER NOT NULL,
    value_classification TEXT NOT NULL,
    update_time TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT fgi_pkey PRIMARY KEY (id, update_time)
);

alter table crypto_scout.cmc_fgi OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_cmc_fgi_update_time ON crypto_scout.cmc_fgi(update_time DESC);
select public.create_hypertable('crypto_scout.cmc_fgi', 'update_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.cmc_fgi set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'value_classification',
    timescaledb.compress_orderby = 'update_time DESC, id DESC'
);
select add_reorder_policy('crypto_scout.cmc_fgi', 'idx_cmc_fgi_update_time');

-- =========================
-- KLINE TABLES (1d/1w)
-- Schema is identical across intervals.
-- =========================

create TABLE IF NOT EXISTS crypto_scout.cmc_kline_1d (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    time_open TIMESTAMP WITH TIME ZONE NOT NULL,
    time_close TIMESTAMP WITH TIME ZONE NOT NULL,
    time_high TIMESTAMP WITH TIME ZONE NOT NULL,
    time_low  TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(20, 10) NOT NULL,
    high NUMERIC(20, 10) NOT NULL,
    low NUMERIC(20, 10) NOT NULL,
    close NUMERIC(20, 10) NOT NULL,
    volume NUMERIC(20, 10) NOT NULL,
    market_cap NUMERIC(26, 10) NOT NULL,
    circulating_supply NUMERIC(28, 10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT cmc_kline_1d_pkey PRIMARY KEY (id, timestamp),
    CONSTRAINT cmc_kline_1d_symbol_close_uniq UNIQUE (symbol, timestamp)
);

alter table crypto_scout.cmc_kline_1d OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_cmc_kline_1d_timestamp ON crypto_scout.cmc_kline_1d(timestamp DESC);
create index IF NOT EXISTS idx_cmc_kline_1d_symbol_timestamp ON crypto_scout.cmc_kline_1d(symbol, timestamp DESC);
select public.create_hypertable('crypto_scout.cmc_kline_1d', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.cmc_kline_1d set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);

select add_reorder_policy('crypto_scout.cmc_kline_1d', 'idx_cmc_kline_1d_timestamp');