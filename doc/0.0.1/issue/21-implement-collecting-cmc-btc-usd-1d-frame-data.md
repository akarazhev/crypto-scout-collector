# Issue 21: Implement collecting `btc-usd` data for the `1d` timeframe that is provided by `CoinMarketCap`

In this `crypto-scout-collector` project let's implement the `SQL` table with read and save service functions. The
related tests should also be updated.

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.

## Conditions

- Change current implementations: @cmc_parser_tables.sql, @CmcParserRepository.java, @CmcParserCollector.java,
  @CmcParserCollectorTest.java, @CmcParserRepositoryTest.java, @StreamCollectorTest.java.
- Rely on sections: `BTC-USD` 1D data model in json, `BTC-USD` 1D sql schema.
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `Java 25`, `ActiveJ 6.0-rc2`, `amqp-client 5.26.0`,
  `stream-client 1.2.0`.
- Follow the current code style.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` implement the `cmc_kline_1d` sql table in @cmc_parser_tables.sql taking into account
  that one row represents the data for the day, so it might be `365` rows per year. Adjust indexes, compressions,
  policies.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.
- As the `expert java engineer` update the implementations @CmcParserRepository.java, @CmcParserCollector.java to
  reflect changes.
- As the `expert java engineer` update the implementations @CmcParserCollectorTest.java, @CmcParserRepositoryTest.java,
  @StreamCollectorTest.java to test changes.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.

## `BTC-USD` 1D data model in json

```json
{
  "id": 1,
  "name": "Bitcoin",
  "symbol": "BTC",
  "timeEnd": "1729900799",
  "quotes": [
    {
      "timeOpen": "2025-11-27T00:00:00.000Z",
      "timeClose": "2025-11-27T23:59:59.999Z",
      "timeHigh": "2025-11-27T09:38:00.000Z",
      "timeLow": "2025-11-27T00:16:00.000Z",
      "quote": {
        "name": "2781",
        "open": 90517.7657112842,
        "high": 91897.5753373039,
        "low": 90089.5162623831,
        "close": 91285.3723441706,
        "volume": 57040622844.6700000000,
        "marketCap": 1821561772280.6200000000,
        "circulatingSupply": 19954584,
        "timestamp": "2025-11-27T23:59:59.999Z"
      }
    }
  ]
}
```

## `BTC-USD` 1D sql schema

```postgres-sql
create TABLE IF NOT EXISTS crypto_scout.cmc_kline_1d (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    time_open TIMESTAMP WITH TIME ZONE NOT NULL,
    time_close TIMESTAMP WITH TIME ZONE NOT NULL,
    time_high TIMESTAMP WITH TIME ZONE NOT NULL,
    time_low  TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    market_cap NUMERIC(20, 8) NOT NULL,
    circulating_supply NUMERIC(20, 8) NOT NULL,
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
```