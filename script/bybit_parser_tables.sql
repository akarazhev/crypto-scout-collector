-- Bybit parser tables and policies (TimescaleDB PG17)

-- =========================
-- BYBIT LAUNCH POOL (LPL)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_lpl (
    return_coin TEXT NOT NULL,
    return_coin_icon TEXT NOT NULL,
    description TEXT NOT NULL,
    website TEXT NOT NULL,
    whitepaper TEXT NOT NULL,
    rules TEXT NOT NULL,
    stake_begin_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stake_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    trade_begin_time TIMESTAMP WITH TIME ZONE,
    CONSTRAINT bybit_lpl_pkey PRIMARY KEY (return_coin, stake_begin_time)
);

alter table crypto_scout.bybit_lpl OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_lpl_stake_end_time ON crypto_scout.bybit_lpl(stake_end_time DESC);
create index IF NOT EXISTS idx_bybit_lpl_trade_begin_time ON crypto_scout.bybit_lpl(trade_begin_time DESC);
select public.create_hypertable('crypto_scout.bybit_lpl', 'stake_begin_time', chunk_time_interval => INTERVAL '1 year', if_not_exists => TRUE);

alter table crypto_scout.bybit_lpl set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'return_coin',
    timescaledb.compress_orderby = 'stake_begin_time DESC'
);
select public.add_compression_policy('crypto_scout.bybit_lpl', interval '35 days');
