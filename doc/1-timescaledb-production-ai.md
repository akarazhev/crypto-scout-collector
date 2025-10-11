# Context: Development of the `timescaledb` production ready service

In this `crypto-scout-collector-db` project we are going to use the `timescaledb` server in a container for saving data crypto
data. So you will need to review and update the `podman-compose.yml` file for the production.

## Roles

Take the following roles:

- Expert dev-opts engineer.

## Conditions

- Use the best practices and design patterns.
- Do not hallucinate.
- Use the latest technology stack: `timescale/timescaledb:latest-pg17`.

## Tasks

- As the expert dev-opts engineer review the current `TimescaleDB` service implementation in `podman-compose.yml` and
  update it to be ready for production.
- We are going to save a lot of data from `Bybit` and `CoinMarketCap`. Adjust the configuration to be ready for production.
- Configure it for high amount of the data, compact old values and appropriate retention policy.  
- Recheck your proposal and make sure that they are correct and haven't missed any important points.
- Write a report with your proposal and implementation into `doc/timescaledb-production-setup.md`.