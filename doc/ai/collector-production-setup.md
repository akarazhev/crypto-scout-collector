# crypto-scout-collector – Production Setup Report (v0.0.1)

## Executive summary

This report documents a production-ready setup for `crypto-scout-collector`, grounded in the source tree. The service
consumes crypto metrics from RabbitMQ Streams and persists time-series data into TimescaleDB. The repository includes a
compose definition for TimescaleDB with tuning and a backup sidecar, an initialization SQL that creates hypertables and
policies, and a container image definition for the collector.

- Database and backups: `podman-compose.yml` provisions TimescaleDB (`timescale/timescaledb:latest-pg17`) and a backup
  sidecar (`prodrigestivill/postgres-backup-local`) with env-driven scheduling and retention. Secrets are placed under
  `secret/`.
- Schema and policies: SQL is split by concern:
    - `script/init.sql` — core bootstrap (extensions, schema, search_path), `crypto_scout.stream_offsets`,
      grants/default privileges.
    - `script/bybit_spot_tables.sql` — Bybit Spot DDL and policies, including `crypto_scout.bybit_spot_tickers`.
    - `script/bybit_linear_tables.sql` — Bybit Linear (Perps/Futures) DDL and policies.
    - `script/crypto_scout_tables.sql` — CMC FGI, kline (1d/1w), and BTC risk tables.
- Data seed scripts: `script/btc_usd_daily_inserts.sql`, `script/btc_usd_weekly_inserts.sql`,
  `script/cmc_fgi_inserts.sql`, `script/alternative_fgi_inserts.sql` — historical data for initial bootstrap.
- Application: Java 25 (ActiveJ). Reads RabbitMQ Streams, batches inserts via HikariCP to TimescaleDB, exposes
  `GET /health`.
- Documentation: `README.md` updated to reflect Java 25, compose service names, and Docker base; this report added under
  `doc/0.0.1/`.

Spot data persistence:

- `com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository` now supports:
    - `saveTicker`
    - `saveKline15m`, `saveKline60m`, `saveKline240m`, `saveKline1d`
    - `savePublicTrade`, `saveOrderBook200`
- Klines and public trades are idempotent via `ON CONFLICT DO NOTHING` using unique keys:
    - Klines: `(symbol, start_time)`
    - Public trades: `(symbol, trade_time)`
    - Order book 200 ingests one row per level without upsert (append-only).

## Proposed short GitHub description

Event‑driven Java service that ingests crypto market metrics from RabbitMQ Streams into TimescaleDB, with automated
backups.

## Verified repository inventory

- Compose & containers
    - `podman-compose.yml`: TimescaleDB service `crypto-scout-collector-db` with tuning, healthcheck, and volumes;
      backup sidecar service `crypto-scout-collector-backup` writing to `./backups`. Network `crypto-scout`.
    - `Dockerfile`: Eclipse Temurin JRE 25, copies shaded jar `crypto-scout-collector-0.0.1.jar`, entrypoint
      `java -jar crypto-scout-collector.jar`.

- Initialization & schema
    - `script/init.sql`: installs `timescaledb` and `pg_stat_statements`; creates schema `crypto_scout`, sets
      `search_path`; defines `crypto_scout.stream_offsets`; grants and default privileges.
      Compose mounts it as `/docker-entrypoint-initdb.d/00-init.sql`.
    - `script/bybit_spot_tables.sql`: Bybit Spot tables and policies; compose mounts it as
      `/docker-entrypoint-initdb.d/01_bybit_spot_tables.sql`:
        - `crypto_scout.bybit_spot_tickers` (spot tickers)
        - `crypto_scout.bybit_spot_kline_1m` (confirmed klines 1m)
        - `crypto_scout.bybit_spot_kline_5m` (confirmed klines 5m)
        - `crypto_scout.bybit_spot_kline_15m` (confirmed klines 15m)
        - `crypto_scout.bybit_spot_kline_60m` (confirmed klines 60m)
        - `crypto_scout.bybit_spot_kline_240m` (confirmed klines 240m)
        - `crypto_scout.bybit_spot_kline_1d` (confirmed klines 1d)
        - `crypto_scout.bybit_spot_public_trade` (1 row per trade)
        - `crypto_scout.bybit_spot_order_book_200` (1 row per book level)
          Adds indexes, hypertables, compression (7‑day threshold), reorder, and retention policies.
    - `script/bybit_linear_tables.sql`: Bybit Linear tables; compose mounts it as
      `/docker-entrypoint-initdb.d/02_bybit_linear_tables.sql`:
        - `crypto_scout.bybit_linear_tickers`
        - `crypto_scout.bybit_linear_kline_60m` (confirmed klines)
        - `crypto_scout.bybit_linear_public_trade` (1 row per trade)
        - `crypto_scout.bybit_linear_order_book_200` (1 row per book level)
        - `crypto_scout.bybit_linear_all_liquidation` (all-liquidations stream)
          Adds indexes, hypertables, compression (7‑day threshold), reorder, and retention policies.
    - `script/crypto_scout_tables.sql`: CMC and risk analysis tables; compose mounts it as
      `/docker-entrypoint-initdb.d/03_crypto_scout_tables.sql`:
        - `crypto_scout.cmc_fgi` (Fear & Greed Index, PK: `update_time`)
        - `crypto_scout.cmc_kline_1d` (BTC/USD daily klines, PK: `(symbol, timestamp)`)
        - `crypto_scout.cmc_kline_1w` (BTC/USD weekly klines, PK: `(symbol, timestamp)`)
        - `crypto_scout.btc_price_risk` (risk-to-price mapping, PK: `(timestamp, risk)`)
        - `crypto_scout.btc_risk_price` (current risk assessment, PK: `timestamp`)
          Adds indexes, hypertables, compression (30‑day threshold), and reorder policies.
    - Data seed scripts (historical data, executed on fresh init):
        - `script/btc_usd_daily_inserts.sql` → `/docker-entrypoint-initdb.d/04_btc_usd_daily_inserts.sql`
        - `script/btc_usd_weekly_inserts.sql` → `/docker-entrypoint-initdb.d/05_btc_usd_weekly_inserts.sql`
        - `script/cmc_fgi_inserts.sql` → `/docker-entrypoint-initdb.d/06_cmc_fgi_inserts.sql`

- Secrets & configuration (gitignored examples)
    - `secret/timescaledb.env.example` and `secret/timescaledb.env`: DB name/user/password, telemetry off, optional
      `POSTGRES_INITDB_ARGS`, and tuning values. Loaded by the DB service.
    - `secret/postgres-backup.env.example` and `secret/postgres-backup.env`: host, DB credentials, backup
      schedule/retention, extra dump opts. Loaded by the backup sidecar.
    - `secret/README.md`: instructions to create env files and set permissions (`chmod 600`). Note: use the compose DB
      service name `crypto-scout-collector-db` for `POSTGRES_HOST`.

- Application
    - Entry point: `src/main/java/com/github/akarazhev/cryptoscout/Collector.java` (ActiveJ `Launcher`).
    - Modules: `module/CoreModule.java` (reactor/executor), `module/CollectorModule.java` (DI and eager `StreamService`,
      `AmqpConsumer`, `AmqpPublisher`), `module/WebModule.java` (`/health`).
    - Services: `collector/StreamService.java`, `collector/BybitStreamService.java`,
      `collector/CryptoScoutService.java`, `collector/DataService.java`, `collector/AmqpConsumer.java`,
      `collector/AmqpPublisher.java`, `collector/HealthService.java`, `collector/PayloadParser.java`.
    - Repos & datasource: `collector/db/CollectorDataSource.java`, `collector/db/*Repository.java`.
    - Config readers: `config/AmqpConfig.java`, `config/JdbcConfig.java`, `config/ServerConfig.java`; keys in
      `config/Constants.java`.
    - App defaults: `src/main/resources/application.properties` (server, AMQP, JDBC/Hikari); logging via SLF4J/Logback.
    - Build: `pom.xml` with shade plugin; main class `com.github.akarazhev.cryptoscout.Collector`.

## Architecture and data flow

```mermaid
flowchart LR
    subgraph RabbitMQ[ RabbitMQ Streams ]
        S1[crypto-scout-stream]
        S2[bybit-stream]
    end

    subgraph AMQP[ RabbitMQ AMQP ]
        Q1[collector-queue]
    end

    subgraph App[crypto-scout-collector (ActiveJ)]
        SS[StreamService]
        BS[BybitStreamService]
        CS[CryptoScoutService]
        DS[DataService]
        AC[AmqpConsumer]
        AP[AmqpPublisher]
        W[WebModule /health]
    end

    subgraph DB[(TimescaleDB)]
        T1[crypto_scout.cmc_fgi]
        T2[crypto_scout.bybit_spot_*]
        T3[crypto_scout.bybit_linear_*]
        T4[crypto_scout.cmc_kline_*]
    end

    S1 -->|Payload| SS --> CS --> T1 & T4
    S2 -->|Payload| SS --> BS --> T2 & T3
    Q1 -->|AMQP| AC --> DS
```

Runtime characteristics:

- Event loop and concurrency: single‑threaded `NioReactor` for orchestration; blocking DB ops offloaded to a
  virtual‑thread executor.
- Batching: collectors buffer messages and flush on size/time thresholds (`jdbc.bybit.batch-size`,
  `jdbc.bybit.flush-interval-ms`, etc.).
- Health: `/health` returns `ok` via `WebModule`.

## TimescaleDB, backups, and application container

- DB image: `timescale/timescaledb:latest-pg17`.
- Volumes: `./data/postgresql:/var/lib/postgresql/data`, and SQL mounted read-only to
  `/docker-entrypoint-initdb.d/` in lexical order:
    - `./script/init.sql` → `/docker-entrypoint-initdb.d/00-init.sql`
    - `./script/bybit_spot_tables.sql` → `/docker-entrypoint-initdb.d/01_bybit_spot_tables.sql`
    - `./script/bybit_linear_tables.sql` → `/docker-entrypoint-initdb.d/02_bybit_linear_tables.sql`
    - `./script/crypto_scout_tables.sql` → `/docker-entrypoint-initdb.d/03_crypto_scout_tables.sql`
    - `./script/btc_usd_daily_inserts.sql` → `/docker-entrypoint-initdb.d/04_btc_usd_daily_inserts.sql`
    - `./script/btc_usd_weekly_inserts.sql` → `/docker-entrypoint-initdb.d/05_btc_usd_weekly_inserts.sql`
    - `./script/cmc_fgi_inserts.sql` → `/docker-entrypoint-initdb.d/06_cmc_fgi_inserts.sql`
    - `./script/alternative_fgi_inserts.sql` → `/docker-entrypoint-initdb.d/07_alternative_fgi_inserts.sql`
- Tuning (from `podman-compose.yml`): preload `timescaledb,pg_stat_statements`, `shared_buffers`, WAL settings, timezone
  UTC, `timescaledb.telemetry_level=off`, `pg_stat_statements.track=all`, IO timing, etc.
- Healthcheck: `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB`.

Backups sidecar: `prodrigestivill/postgres-backup-local:latest` (service: `crypto-scout-collector-backup`).

- Env‑driven schedule (`SCHEDULE`, e.g., `@daily`) and retention (`BACKUP_KEEP_DAYS/WEEKS/MONTHS`).

Secrets management:

- `secret/timescaledb.env` is the env file loaded by the DB service.
- `secret/postgres-backup.env` is the env file loaded by the backup container.
- `secret/collector.env` is the env file loaded by the application container. By default, the application uses
  `src/main/resources/application.properties`. If you need runtime overrides, adjust compose to pass JVM `-D` flags or
  update `application.properties` and rebuild. `JAVA_TOOL_OPTIONS` should include only `-XX:+ExitOnOutOfMemoryError`.

  Generate strong credentials and `chmod 600` the files.

Application container: `crypto-scout-collector`

- Base image: Eclipse Temurin 25 JRE Alpine (pinned digest), OCI metadata labels present.
- Runs as non-root user/group `10001:10001`; workdir `/opt/crypto-scout`.
- Healthcheck: `curl -f http://localhost:8081/health` (interval 10s, timeout 3s, retries 5, start period 30s).
- Compose hardening:
    - `read_only: true`, `tmpfs: /tmp:rw,size=64m,mode=1777,nodev,nosuid`
    - `security_opt: [no-new-privileges=true]`, `cap_drop: [ALL]`
    - `cpus: "0.5"`, `mem_limit: 512m`, `pids_limit: 256`, `ulimits.noFile: 4096:4096`
    - `user: "10001:10001"`, `init: true`, `restart: unless-stopped`, `stop_signal: SIGTERM`, `stop_grace_period: 30s`
- Networking: joins the external network `crypto-scout-bridge` to reach the DB service by name
  `crypto-scout-collector-db`.
    - `crypto_scout.cmc_fgi` — primary key `(update_time)`.
    - `crypto_scout.cmc_kline_1d` — primary key `(symbol, timestamp)`; BTC/USD daily klines.
    - `crypto_scout.cmc_kline_1w` — primary key `(symbol, timestamp)`; BTC/USD weekly klines.
    - `crypto_scout.btc_price_risk` — primary key `(timestamp, risk)`; risk-to-price mapping.
    - `crypto_scout.btc_risk_price` — primary key `(timestamp)`; current risk assessment.
    - `crypto_scout.bybit_spot_tickers` — primary key `(id, timestamp)`; indexes on `(timestamp)` and
      `(symbol, timestamp)`.
    - `crypto_scout.stream_offsets` — primary key `(stream)`; stores last processed offset per stream.
    - `crypto_scout.bybit_linear_tickers` — primary key `(id, timestamp)`; symbol/timestamp indexes.
    - `crypto_scout.bybit_linear_kline_60m` — primary key `(id, start_time)`; unique `(symbol, start_time)`.
    - `crypto_scout.bybit_linear_public_trade` — primary key `(id, trade_time)`; unique `(symbol, trade_time)`.
    - `crypto_scout.bybit_linear_order_book_200` — primary key `(id, engine_time)`; indexes on `(symbol, engine_time)`
      and `(symbol, side, price)`.
    - `crypto_scout.bybit_linear_all_liquidation` — primary key `(id, event_time)`; indexes on `(symbol, event_time)`.
- External offset tracking for crypto-scout and Bybit streams:
    - `StreamService` starts consumers from DB offset and disables server-side tracking.
    - `CryptoScoutService`/`BybitStreamService` batch inserts and update offsets atomically.
- Compression policies (segmentby/orderby) for tables (compress chunks older than 7 days for Bybit, 30 days for
  CMC/risk).
- Reorder policies align with time‑descending indexes.
- Retention: 180 days for `bybit_spot_tickers`.
- Grants and default privileges for role `crypto_scout_db`.

Offset handling: DB-backed external offsets are used for all streams except any future ephemeral consumers.

## Application configuration keys

From `src/main/resources/application.properties`:

- Server
    - `server.port` (default `8081`).
- RabbitMQ
    - `amqp.rabbitmq.host` (default `localhost`)
    - `amqp.rabbitmq.username` (default `crypto_scout_mq`)
    - `amqp.rabbitmq.password` (empty by default)
    - `amqp.rabbitmq.port` (default `5672`)
    - `amqp.stream.port` (default `5552`)
    - `amqp.bybit.stream` (default `bybit-stream`)
    - `amqp.bybit.ta.stream` (default `bybit-ta-stream`)
    - `amqp.crypto.scout.stream` (default `crypto-scout-stream`)
    - `amqp.crypto.scout.exchange`, `amqp.collector.queue`, `amqp.chatbot.queue`, `amqp.analyst.queue`
- JDBC / HikariCP
    - `jdbc.datasource.url` (default `jdbc:postgresql://localhost:5432/crypto_scout`)
    - `jdbc.datasource.username` (default `crypto_scout_db`)
    - `jdbc.datasource.password` (empty by default)
    - batching and Hikari pool sizing/timeouts.

Container networking tip: if the app runs on the same compose network, use
`jdbc:postgresql://crypto-scout-collector-db:5432/crypto_scout` (host `crypto-scout-collector-db` is the DB service
name).

## Build, run, and deploy

- Build fat JAR
  ```bash
  mvn -q -DskipTests package
  ```

- Run locally
  ```bash
  java -jar target/crypto-scout-collector-0.0.1.jar
  curl -s http://localhost:8081/health  # -> ok
  ```

- Start DB + backups (Podman Compose)
  ```bash
  cp ./secret/timescaledb.env.example ./secret/timescaledb.env
  cp ./secret/postgres-backup.env.example ./secret/postgres-backup.env
  chmod 600 ./secret/*.env
  podman-compose -f podman-compose.yml up -d
  # optional: docker compose -f podman-compose.yml up -d
  ```

- Run the application container (Podman Compose)
  ```bash
  # Build fat JAR required by the app image
  mvn -q -DskipTests package

  # Prepare collector env
  cp ./secret/collector.env.example ./secret/collector.env
  chmod 600 ./secret/collector.env

  # Build the app image and bring up the full stack
  podman-compose -f podman-compose.yml build crypto-scout-collector
  podman-compose -f podman-compose.yml up -d

  # Verify health
  curl -s http://localhost:8081/health  # -> ok
  ```

- Build container image and run
  ```bash
  docker build -t crypto-scout-collector:0.0.1 .
  docker run --rm \
    --name crypto-scout-collector \
    --network <compose_network_name> \
    -p 8081:8081 \
    crypto-scout-collector:0.0.1
  ```

Ensure RabbitMQ Streams and TimescaleDB are reachable per configured host/ports.

## Offset handling

- **Crypto-scout stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
    - The consumer starts from `offset + 1` if present, otherwise from `first`.
    - On flush, `CryptoScoutService` inserts data and updates the max processed offset in a single transaction.
- **Bybit stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
    - The consumer starts from `offset + 1` if present, otherwise from `first`.
    - On flush, `BybitStreamService` inserts data and updates the max processed offset in a single transaction.

If your DB was initialized before this change, apply the `stream_offsets` DDL manually or re-initialize the data dir to
pick up `script/init.sql` changes.

## Operations

- Health: `GET /health` returns `ok`.
- Logs: SLF4J/Logback (console appender, INFO level).
- Shutdown: ActiveJ launcher runs until termination; repositories and datasource close on stop.

## Hardening checklist

- Rotate credentials in `secret/*.env`; avoid committing real secrets. Enforce `chmod 600`.
- Consider `POSTGRES_INITDB_ARGS=--auth=scram-sha-256` in `secret/timescaledb.env` before first init.
- Validate retention and compression windows in `script/init.sql` for your SLA/cost profile.
- Run DB on persistent storage with regular off‑site backup sync from `./backups`.
- Set non‑empty `amqp.rabbitmq.password` and `jdbc.datasource.password` before production use.

## Validation steps

1. Start DB and backups via compose; verify `pg_isready` passes and backup container is healthy.
2. Build and run the app; verify `/health` returns `ok`.
3. Publish a small test message to each RabbitMQ stream; verify rows appear in the corresponding hypertables.
4. Confirm backup artifacts are generated under `./backups` after the scheduled run.

## Changelog & implementation

- Updated `README.md` to reflect Java 25, compose service names (`crypto-scout-collector-db`,
  `crypto-scout-collector-backup`), and Docker base image; removed placeholder artifacts and restored
  Architecture/Database sections.
- Added this production setup report at `doc/0.0.1/collector-production-setup.md` including the proposed GitHub short
  description.
- Implemented external DB-backed offset tracking for streams:
    - New table `crypto_scout.stream_offsets`.
    - `StreamService` starts consumers from DB offset and disables server-side tracking.
    - `CryptoScoutService`/`BybitStreamService` batch inserts and update offsets atomically.
- Consolidated CMC and risk tables:
    - Created `script/crypto_scout_tables.sql` with `cmc_fgi`, `cmc_kline_1d`, `cmc_kline_1w`, `btc_price_risk`,
      `btc_risk_price`.
    - Added data seed scripts for historical data bootstrap.
    - Updated compose volumes and documentation to mount and describe the new scripts.

## References to source

- `podman-compose.yml`
- `script/init.sql`
- `script/bybit_spot_tables.sql`
- `script/bybit_linear_tables.sql`
- `script/crypto_scout_tables.sql`
- `script/btc_usd_daily_inserts.sql`
- `script/btc_usd_weekly_inserts.sql`
- `script/cmc_fgi_inserts.sql`
- `script/alternative_fgi_inserts.sql`
- `secret/*.env`, `secret/README.md`
- `src/main/resources/application.properties`
- `src/main/java/com/github/akarazhev/cryptoscout/*`
- `Dockerfile`, `pom.xml`
