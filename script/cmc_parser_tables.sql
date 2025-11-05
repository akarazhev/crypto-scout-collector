-- CMC parser tables and policies (TimescaleDB PG17)
-- Safe to run on initial bootstrap; idempotent DDL where possible

-- Ensure required extensions and schema
create EXTENSION IF NOT EXISTS timescaledb;
create SCHEMA IF NOT EXISTS crypto_scout;
SET search_path TO public, crypto_scout;

-- =========================
-- CMC FEAR & GREED INDEX (FGI)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.cmc_fgi (
    id BIGSERIAL,
    score INTEGER NOT NULL,
    name TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    btc_price NUMERIC(20, 8) NOT NULL,
    btc_volume NUMERIC(20, 8) NOT NULL,
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
select add_compression_policy('crypto_scout.cmc_fgi', interval '7 days');
select add_reorder_policy('crypto_scout.cmc_fgi', 'idx_cmc_fgi_timestamp');
select add_retention_policy('crypto_scout.cmc_fgi', interval '730 days');
