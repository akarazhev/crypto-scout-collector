-- Bybit Ta Linear (Perps/Futures) tables and policies (TimescaleDB PG17)

-- =========================
-- TA PUBLIC TRADES (normalized: 1 row per trade)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_public_trade (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    trade_time TIMESTAMP WITH TIME ZONE NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    taker_side TEXT NOT NULL CHECK (taker_side IN ('Buy','Sell')),
    tick_direction TEXT,
    is_block_trade BOOLEAN NOT NULL,
    is_rpi BOOLEAN NOT NULL,
    CONSTRAINT bybit_ta_linear_public_trade_pkey PRIMARY KEY (id, trade_time)
);
alter table crypto_scout.bybit_ta_linear_public_trade OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_public_trade_trade_time ON crypto_scout.bybit_ta_linear_public_trade(trade_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_public_trade_symbol_time ON crypto_scout.bybit_ta_linear_public_trade(symbol, trade_time DESC);
select public.create_hypertable('crypto_scout.bybit_ta_linear_public_trade', 'trade_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_ta_linear_public_trade set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'trade_time DESC, id DESC'
);
select add_compression_policy('crypto_scout.bybit_ta_linear_public_trade', interval '1 month');
select add_reorder_policy('crypto_scout.bybit_ta_linear_public_trade', 'idx_bybit_ta_linear_public_trade_trade_time');
select add_retention_policy('crypto_scout.bybit_ta_linear_public_trade', interval '365 days');

-- =========================
-- TA ORDER BOOKS (1/50/200/1000)
-- Schema is identical across depths. (normalized: 1 row per level)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_order_book_1 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_ta_linear_order_book_1_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_ta_linear_order_book_1 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1_engine_time ON crypto_scout.bybit_ta_linear_order_book_1(engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1_symbol_time ON crypto_scout.bybit_ta_linear_order_book_1(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1_symbol_side_price ON crypto_scout.bybit_ta_linear_order_book_1(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_ta_linear_order_book_1', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_order_book_50 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_ta_linear_order_book_50_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_ta_linear_order_book_50 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_50_engine_time ON crypto_scout.bybit_ta_linear_order_book_50(engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_50_symbol_time ON crypto_scout.bybit_ta_linear_order_book_50(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_50_symbol_side_price ON crypto_scout.bybit_ta_linear_order_book_50(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_ta_linear_order_book_50', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_order_book_200 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_ta_linear_order_book_200_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_ta_linear_order_book_200 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_200_engine_time ON crypto_scout.bybit_ta_linear_order_book_200(engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_200_symbol_time ON crypto_scout.bybit_ta_linear_order_book_200(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_200_symbol_side_price ON crypto_scout.bybit_ta_linear_order_book_200(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_ta_linear_order_book_200', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_order_book_1000 (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    engine_time TIMESTAMP WITH TIME ZONE NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid','ask')),
    price DOUBLE PRECISION NOT NULL,
    size DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_ta_linear_order_book_1000_pkey PRIMARY KEY (id, engine_time)
);
alter table crypto_scout.bybit_ta_linear_order_book_1000 OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1000_engine_time ON crypto_scout.bybit_ta_linear_order_book_1000(engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1000_symbol_time ON crypto_scout.bybit_ta_linear_order_book_1000(symbol, engine_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_order_book_1000_symbol_side_price ON crypto_scout.bybit_ta_linear_order_book_1000(symbol, side, price);
select public.create_hypertable('crypto_scout.bybit_ta_linear_order_book_1000', 'engine_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Compression settings for order book tables
alter table crypto_scout.bybit_ta_linear_order_book_1 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_ta_linear_order_book_50 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_ta_linear_order_book_200 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);
alter table crypto_scout.bybit_ta_linear_order_book_1000 set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, side',
    timescaledb.compress_orderby = 'engine_time DESC, price DESC, id DESC'
);

-- Compression policies for order book tables
select add_compression_policy('crypto_scout.bybit_ta_linear_order_book_1', interval '1 month');
select add_compression_policy('crypto_scout.bybit_ta_linear_order_book_50', interval '1 month');
select add_compression_policy('crypto_scout.bybit_ta_linear_order_book_200', interval '1 month');
select add_compression_policy('crypto_scout.bybit_ta_linear_order_book_1000', interval '1 month');

-- Reorder policies for order book tables
select add_reorder_policy('crypto_scout.bybit_ta_linear_order_book_1', 'idx_bybit_ta_linear_order_book_1_engine_time');
select add_reorder_policy('crypto_scout.bybit_ta_linear_order_book_50', 'idx_bybit_ta_linear_order_book_50_engine_time');
select add_reorder_policy('crypto_scout.bybit_ta_linear_order_book_200', 'idx_bybit_ta_linear_order_book_200_engine_time');
select add_reorder_policy('crypto_scout.bybit_ta_linear_order_book_1000', 'idx_bybit_ta_linear_order_book_1000_engine_time');

-- Retention policies for order book tables
select add_retention_policy('crypto_scout.bybit_ta_linear_order_book_1', interval '365 days');
select add_retention_policy('crypto_scout.bybit_ta_linear_order_book_50', interval '365 days');
select add_retention_policy('crypto_scout.bybit_ta_linear_order_book_200', interval '365 days');
select add_retention_policy('crypto_scout.bybit_ta_linear_order_book_1000', interval '365 days');

-- =========================
-- TA ALL LIQUIDATION (normalized)
-- =========================

create TABLE IF NOT EXISTS crypto_scout.bybit_ta_linear_all_liqudation (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    position_side TEXT NOT NULL CHECK (position_side IN ('Buy','Sell')),
    executed_size DOUBLE PRECISION NOT NULL,
    bankruptcy_price DOUBLE PRECISION NOT NULL,
    CONSTRAINT bybit_ta_linear_all_liqudation_pkey PRIMARY KEY (id, event_time)
);
alter table crypto_scout.bybit_ta_linear_all_liqudation OWNER TO crypto_scout_db;
create index IF NOT EXISTS idx_bybit_ta_linear_all_liqudation_event_time ON crypto_scout.bybit_ta_linear_all_liqudation(event_time DESC);
create index IF NOT EXISTS idx_bybit_ta_linear_all_liqudation_symbol_time ON crypto_scout.bybit_ta_linear_all_liqudation(symbol, event_time DESC);
select public.create_hypertable('crypto_scout.bybit_ta_linear_all_liqudation', 'event_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

alter table crypto_scout.bybit_ta_linear_all_liqudation set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, position_side',
    timescaledb.compress_orderby = 'event_time DESC, id DESC'
);
select add_compression_policy('crypto_scout.bybit_ta_linear_all_liqudation', interval '1 month');
select add_reorder_policy('crypto_scout.bybit_ta_linear_all_liqudation', 'idx_bybit_ta_linear_all_liqudation_event_time');
select add_retention_policy('crypto_scout.bybit_ta_linear_all_liqudation', interval '365 days');
