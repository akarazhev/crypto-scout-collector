# Issue 23: Review and optimize the db table definitions

In this `crypto-scout-collector` project let's review and optimize the db table definitions for the `bybit` data models.
It must be optimal for performance to store and retrieve data efficiently and be production ready. Propose appropriate 
compressions, chunk time intervals, reorder and other policies.

## Roles

Take the following roles:

- Expert database engineer.

## Conditions

- Rely on the following table definitions: @bybit_linear_tables.sql, @bybit_parser_tables.sql, @bybit_spot_tables.sql, 
  @BYBIT_LINEAR_tables.sql, @BYBIT_SPOT_tables.sql.
- Use the @cmc_parser_tables.sql as the reference.
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `podman 5.6.2`, `podman-compose 1.5.0`, `timescale/timescaledb:latest-pg17`.
- Follow the current code style.
- Do not hallucinate.

## Tasks

- As the `expert database engineer` review and optimize the db table definitions for the `bybit` data models: 
  @bybit_linear_tables.sql, @bybit_parser_tables.sql, @bybit_spot_tables.sql, @BYBIT_LINEAR_tables.sql, 
  @BYBIT_SPOT_tables.sql. Propose appropriate compressions, chunk time intervals, reorder and other policies. 
  Replace data types with the most appropriate ones: `NUMERIC` to `DOUBLE PRECISION`. Optimize indexes.
- As the `expert database engineer` double-check your proposal and make sure that they are correct and haven't missed
  any important points.