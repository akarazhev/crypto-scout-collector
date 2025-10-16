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
    offset BIGINT NOT NULL CHECK (offset >= 0),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Set ownership to application role
ALTER TABLE crypto_scout.stream_offsets OWNER TO crypto_scout_db;

-- Create Fear & Greed Index table in crypto_scout schema
CREATE TABLE IF NOT EXISTS crypto_scout.cmc_fgi (
    id BIGSERIAL,
    score INTEGER NOT NULL,
    name TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    btc_price NUMERIC(20, 2) NOT NULL,
    btc_volume NUMERIC(20, 2) NOT NULL,
    -- For hypertables, primary key must include the partitioning column (timestamp)
    CONSTRAINT fgi_pkey PRIMARY KEY (id, timestamp)
);

-- Set ownership to application role
ALTER TABLE crypto_scout.cmc_fgi OWNER TO crypto_scout_db;
 
-- Create index for cmc_fgi table first (before hypertable conversion)
CREATE INDEX IF NOT EXISTS idx_cmc_fgi_timestamp ON crypto_scout.cmc_fgi(timestamp DESC);
 
-- Convert the cmc_fgi table to a hypertable partitioned by timestamp
-- Using 1-day chunks for optimal performance with daily data
SELECT public.create_hypertable('crypto_scout.cmc_fgi', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Create Bybit Spot Tickers table in crypto_scout schema
CREATE TABLE IF NOT EXISTS crypto_scout.bybit_spot_tickers (
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
    -- For hypertables, primary key must include the partitioning column (timestamp)
    CONSTRAINT bybit_spot_tickers_pkey PRIMARY KEY (id, timestamp)
);

-- Set ownership to application role
ALTER TABLE crypto_scout.bybit_spot_tickers OWNER TO crypto_scout_db;

-- Create indexes for bybit_spot_tickers table first (before hypertable conversion)
CREATE INDEX IF NOT EXISTS idx_bybit_spot_tickers_timestamp ON crypto_scout.bybit_spot_tickers(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_bybit_spot_tickers_symbol_timestamp ON crypto_scout.bybit_spot_tickers(symbol, timestamp DESC);

-- Convert the bybit_spot_tickers table to a hypertable partitioned by timestamp
-- Using 1-day chunks for optimal performance with time-series data
SELECT public.create_hypertable('crypto_scout.bybit_spot_tickers', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Create Bybit Launch Pool table in crypto_scout schema
CREATE TABLE IF NOT EXISTS crypto_scout.bybit_lpl (
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
    -- For hypertables, primary key must include the partitioning column (stake_begin_time)
    CONSTRAINT bybit_lpl_pkey PRIMARY KEY (id, stake_begin_time)
);

-- Set ownership to application role
ALTER TABLE crypto_scout.bybit_lpl OWNER TO crypto_scout_db;
 
-- Create index for bybit_lpl table first (before hypertable conversion)
CREATE INDEX IF NOT EXISTS idx_bybit_lpl_stake_begin_time ON crypto_scout.bybit_lpl(stake_begin_time DESC);
 
-- Convert the bybit_lpl table to a hypertable partitioned by stake_begin_time
-- Using 1-day chunks for optimal performance with time-series data
SELECT public.create_hypertable('crypto_scout.bybit_lpl', 'stake_begin_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Set up compression policy for bybit_spot_tickers
ALTER TABLE crypto_scout.bybit_spot_tickers SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);

-- Add compression for bybit_lpl table
ALTER TABLE crypto_scout.bybit_lpl SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'return_coin',
    timescaledb.compress_orderby = 'stake_begin_time DESC, id DESC'
);

-- Add compression for cmc_fgi table
ALTER TABLE crypto_scout.cmc_fgi SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'name',
    timescaledb.compress_orderby = 'timestamp DESC, id DESC'
);

-- Compress chunks that are older than 7 days
SELECT add_compression_policy('crypto_scout.bybit_spot_tickers', INTERVAL '7 days');
SELECT add_compression_policy('crypto_scout.bybit_lpl', INTERVAL '7 days');
SELECT add_compression_policy('crypto_scout.cmc_fgi', INTERVAL '7 days');

-- Reorder open chunks by timestamp to optimize range scans on hot data
SELECT add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_timestamp');
SELECT add_reorder_policy('crypto_scout.cmc_fgi', 'idx_cmc_fgi_timestamp');
SELECT add_reorder_policy('crypto_scout.bybit_lpl', 'idx_bybit_lpl_stake_begin_time');

-- Retention policies
SELECT add_retention_policy('crypto_scout.cmc_fgi', INTERVAL '730 days'); -- keep ~2 years
SELECT add_retention_policy('crypto_scout.bybit_spot_tickers', INTERVAL '180 days'); -- keep 6 months
SELECT add_retention_policy('crypto_scout.bybit_lpl', INTERVAL '730 days'); -- keep ~2 years

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA crypto_scout TO crypto_scout_db;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crypto_scout TO crypto_scout_db;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crypto_scout TO crypto_scout_db;

-- Ensure future objects created by the application role have appropriate privileges
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO crypto_scout_db;
ALTER DEFAULT PRIVILEGES FOR ROLE crypto_scout_db IN SCHEMA crypto_scout GRANT USAGE, SELECT ON SEQUENCES TO crypto_scout_db;