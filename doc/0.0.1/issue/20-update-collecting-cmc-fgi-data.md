# Issue 20: Update collecting `FGI` data that is provided by `CoinMarketCap`

In this `crypto-scout-collector` project we are going to update the `SQL` data model of the `FGI` that is provided by
`CoinMarketCap`. The related tests should also be updated. `FGI` stands for `Fear & Greed Index`.

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.

## Conditions

- Change current implementations: @cmc_parser_tables.sql, @CmcParserRepository.java, @CmcParserCollector.java,
  @CmcParserCollectorTest.java, @CmcParserRepositoryTest.java.
- Rely on sections: `FGI` data model in json, `FGI` sql schema
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `Java 25`, `ActiveJ 6.0-rc2`, `amqp-client 5.26.0`,
  `stream-client 1.2.0`.
- Follow the current code style.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` update the `FGI` sql table in @cmc_parser_tables.sql taking into account that one
  row represents the data for the day, so it might be `365` rows per year. Adjust indexes, compressions, policies.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.
- As the `expert java engineer` update the implementations @CmcParserRepository.java, @CmcParserCollector.java to
  reflect changes.
- As the `expert java engineer` update the implementations @CmcParserCollectorTest.java, @CmcParserRepositoryTest.java
  to test changes.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.

## `FGI` data model in json

```json
{
  "value": 20,
  "update_time": "2025-11-28T12:38:10.026Z",
  "value_classification": "Fear"
}
```

## `FGI` sql schema

```postgres-sql
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
```