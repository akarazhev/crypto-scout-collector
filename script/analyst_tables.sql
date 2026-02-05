-- Analyst indicators table for cmc_kline_1w with foreign key reference

CREATE TABLE IF NOT EXISTS crypto_scout.cmc_kline_1w_indicators (
    symbol TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    -- Simple Moving Averages (sma suffix)
    sma_50 DOUBLE PRECISION,
    sma_100 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    -- Exponential Moving Averages (ema suffix)
    ema_50 DOUBLE PRECISION,
    ema_100 DOUBLE PRECISION,
    ema_200 DOUBLE PRECISION,
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT cmc_kline_1w_indicators_pkey PRIMARY KEY (symbol, timestamp),
    -- Foreign key reference to cmc_kline_1w
    CONSTRAINT cmc_kline_1w_indicators_fkey
        FOREIGN KEY (symbol, timestamp)
        REFERENCES crypto_scout.cmc_kline_1w(symbol, timestamp)
        ON DELETE CASCADE
);

ALTER TABLE crypto_scout.cmc_kline_1w_indicators OWNER TO crypto_scout_db;

CREATE INDEX IF NOT EXISTS idx_cmc_kline_1w_indicators_timestamp
    ON crypto_scout.cmc_kline_1w_indicators(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_cmc_kline_1w_indicators_symbol
    ON crypto_scout.cmc_kline_1w_indicators(symbol);

-- Convert to hypertable
SELECT public.create_hypertable('crypto_scout.cmc_kline_1w_indicators', 'timestamp',
    chunk_time_interval => INTERVAL '3 months', if_not_exists => TRUE);

-- Enable compression
ALTER TABLE crypto_scout.cmc_kline_1w_indicators SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT public.add_compression_policy('crypto_scout.cmc_kline_1w_indicators', INTERVAL '30 days');

-- View that joins kline data with indicators
CREATE OR REPLACE VIEW crypto_scout.cmc_kline_1w_with_indicators AS
SELECT
    k.symbol,
    k.timestamp,
    k.time_open,
    k.time_close,
    k.time_high,
    k.time_low,
    k.open,
    k.high,
    k.low,
    k.close,
    k.volume,
    k.market_cap,
    k.circulating_supply,
    i.sma_50,
    i.sma_100,
    i.sma_200,
    i.ema_50,
    i.ema_100,
    i.ema_200
FROM crypto_scout.cmc_kline_1w k
LEFT JOIN crypto_scout.cmc_kline_1w_indicators i
    ON k.symbol = i.symbol AND k.timestamp = i.timestamp;
