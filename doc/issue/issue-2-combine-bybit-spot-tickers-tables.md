# Issue 2: Combine `bybit_spot_tickers_btc_usdt` and `bybit_spot_tickers_eth_usdt` tables into `bybit_spot_tickers`

In this `crypto-scout-collector-db` project we are going to combine two tables `bybit_spot_tickers_btc_usdt` and
`bybit_spot_tickers_eth_usdt` into one `bybit_spot_tickers` table.

## Roles

Take the following roles:

- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Do not hallucinate.

## Tasks

- As the expert database engineer review the current `init.sql` script implementation in `crypto-scout-collector-db` project and
  update it by combining the `bybit_spot_tickers_btc_usdt` and `bybit_spot_tickers_eth_usdt` tables into
  `bybit_spot_tickers` table.
- Define for the `bybit_spot_tickers` table indexes, retentions and compressions.
- Remove the `bybit_spot_tickers_btc_usdt` and `bybit_spot_tickers_eth_usdt` tables with related configuration of
  indexes, retentions and compressions.
- Recheck your proposal and make sure that they are correct and haven't missed any important points.
- Rely on the sample of the data section.
- Update the documentation `issue-2-combine-spot-tickers-tables.md` with your results in the resolution section.

## Sample of the data

```json
{
  "ts": 1760107467340,
  "cs": 87394187580,
  "data": {
    "symbol": "BTCUSDT",
    "lastPrice": "121349.1",
    "highPrice24h": "122537.6",
    "lowPrice24h": "119641.7",
    "prevPrice24h": "121414.7",
    "volume24h": "8892.609065",
    "turnover24h": "1077902055.75212816",
    "price24hPcnt": "-0.0005",
    "usdIndexPrice": "121401.852838"
  }
}
```

### Field mapping from sample JSON

- `ts`: number. The timestamp (ms) that the system generates the data
- `cs`: integer. Cross sequence
- `data`: array. Object:
- `symbol`: string. Symbol name
- `lastPrice`: string. Last price
- `highPrice24h`: string. The highest price in the last 24 hours
- `lowPrice24h`: string. The lowest price in the last 24 hours
- `prevPrice24h`: string. Percentage change of market price relative to 24h
- `volume24h`: string. Volume for 24h
- `turnover24h`: string. Turnover for 24h
- `price24hPcnt`: string. Percentage change of market price relative to 24h
- `usdIndexPrice`: string. USD index price: - used to calculate USD value of the assets in Unified account. -
  non-collateral margin coin returns

## Resolution

- **[change]** Merged `bybit_spot_tickers_btc_usdt` and `bybit_spot_tickers_eth_usdt` into a single hypertable
  `crypto_scout.bybit_spot_tickers` in `script/init.sql`.

### Table schema: `crypto_scout.bybit_spot_tickers`

- **Columns**
    - `id BIGSERIAL`
    - `symbol TEXT NOT NULL` (e.g., `BTCUSDT`, `ETHUSDT`)
    - `timestamp TIMESTAMPTZ NOT NULL`
    - `cross_sequence BIGINT NOT NULL`
    - `last_price NUMERIC(20, 2) NOT NULL`
    - `high_price_24h NUMERIC(20, 2) NOT NULL`
    - `low_price_24h NUMERIC(20, 2) NOT NULL`
    - `prev_price_24h NUMERIC(20, 2) NOT NULL`
    - `volume_24h NUMERIC(20, 8) NOT NULL`
    - `turnover_24h NUMERIC(20, 4) NOT NULL`
    - `price_24h_pcnt NUMERIC(10, 4) NOT NULL`
    - `usd_index_price NUMERIC(20, 6)`
- **Primary key**: `(id, timestamp)`
- **Hypertable**: partitioned by `timestamp` with 1-day chunks

### Indexes

- **Time index**: `idx_bybit_spot_tickers_timestamp` on `(timestamp DESC)`
- **Selective index**: `idx_bybit_spot_tickers_symbol_timestamp` on `(symbol, timestamp DESC)`
- **Reorder policy**: `add_reorder_policy('crypto_scout.bybit_spot_tickers', 'idx_bybit_spot_tickers_timestamp')`

### Compression

- **Settings** on `bybit_spot_tickers`:
    - `timescaledb.compress = on`
    - `timescaledb.compress_segmentby = 'symbol'`
    - `timescaledb.compress_orderby = 'timestamp DESC, id DESC'`
- **Policy**: `add_compression_policy('crypto_scout.bybit_spot_tickers', INTERVAL '7 days')`

### Retention

- **Policy**: `add_retention_policy('crypto_scout.bybit_spot_tickers', INTERVAL '180 days')`

### Removed old tables and policies

- Removed from `script/init.sql`: definitions, indexes, reorder, compression, and retention for
    - `crypto_scout.bybit_spot_tickers_btc_usdt`
    - `crypto_scout.bybit_spot_tickers_eth_usdt`

### Field mapping from sample JSON

- `ts` → `timestamp` (epoch millis → timestamptz)
- `cs` → `cross_sequence`
- `data.symbol` → `symbol`
- `data.lastPrice` → `last_price`
- `data.highPrice24h` → `high_price_24h`
- `data.lowPrice24h` → `low_price_24h`
- `data.prevPrice24h` → `prev_price_24h`
- `data.volume24h` → `volume_24h`
- `data.turnover24h` → `turnover_24h`
- `data.price24hPcnt` → `price_24h_pcnt`
- `data.usdIndexPrice` → `usd_index_price`

### Notes

- `script/init.sql` is for bootstrap. If the old per-pair tables exist in a running cluster, backfill into
  `bybit_spot_tickers` with the proper `symbol` and drop old tables afterward.
- Other docs (e.g., `doc/timescaledb-production-setup.md`, `README.md`) still reference the per-pair tables; update them
  separately to reflect the unified schema.