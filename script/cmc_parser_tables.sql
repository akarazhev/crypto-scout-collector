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
