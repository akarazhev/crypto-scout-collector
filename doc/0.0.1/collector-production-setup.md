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
    - `script/bybit_parser_tables.sql` — `crypto_scout.bybit_lpl` (Bybit Launch Pool) DDL and policies.
    - `script/bybit_linear_tables.sql` — Bybit Linear (Perps/Futures) DDL and policies.
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
  - Public trades: `(symbol, trade_id, trade_time)`
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
    - `script/bybit_spot_tables.sql`: Bybit Spot tables and policies; compose mounts it as
      `/docker-entrypoint-initdb.d/02-bybit_spot_tables.sql`:
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
    - `script/cmc_parser_tables.sql`: `crypto_scout.cmc_fgi`; compose mounts it as
      `/docker-entrypoint-initdb.d/03-cmc_parser_tables.sql`.
    - `script/bybit_parser_tables.sql`: `crypto_scout.bybit_lpl`; compose mounts it as
      `/docker-entrypoint-initdb.d/04-bybit_parser_tables.sql`.
    - `script/bybit_linear_tables.sql`: Bybit Linear tables; compose mounts it as
      `/docker-entrypoint-initdb.d/05-bybit_linear_tables.sql`:
        - `crypto_scout.bybit_linear_tickers`
        - `crypto_scout.bybit_linear_kline_60m` (confirmed klines)
        - `crypto_scout.bybit_linear_public_trade` (1 row per trade)
        - `crypto_scout.bybit_linear_order_book_200` (1 row per book level)
        - `crypto_scout.bybit_linear_all_liqudation` (all-liquidations stream)
          Adds indexes, hypertables, compression (7‑day threshold), reorder, and retention policies.

- Secrets & configuration (gitignored examples)
    - `secret/timescaledb.env.example` and `secret/timescaledb.env`: DB name/user/password, telemetry off, optional
      `POSTGRES_INITDB_ARGS`, and tuning values. Loaded by the DB service.
    - `secret/postgres-backup.env.example` and `secret/postgres-backup.env`: host, DB credentials, backup
      schedule/retention, extra dump opts. Loaded by the backup sidecar.
    - `secret/README.md`: instructions to create env files and set permissions (`chmod 600`). Note: use the compose DB
      service name `crypto-scout-collector-db` for `POSTGRES_HOST`.

- Application
    - Entry point: `src/main/java/com/github/akarazhev/cryptoscout/Collector.java` (ActiveJ `Launcher`).
    - Modules: `module/CoreModule.java` (reactor/executor), `module/CollectorModule.java` (DI and eager `AmqpConsumer`),
      `module/WebModule.java` (`/health`).
    - Collectors: `collector/AmqpConsumer.java`, `collector/CmcParserCollector.java`,
      `collector/BybitParserCollector.java`, `collector/BybitCryptoCollector.java`.
    - Repos & datasource: `collector/db/CollectorDataSource.java`, `collector/db/*Repository.java`.
    - Config readers: `config/AmqpConfig.java`, `config/JdbcConfig.java`, `config/ServerConfig.java`; keys in
      `config/Constants.java`.
    - App defaults: `src/main/resources/application.properties` (server, AMQP, JDBC/Hikari), logs via
      `src/main/resources/logback.xml`.
    - Build: `pom.xml` with shade plugin; main class `com.github.akarazhev.cryptoscout.Collector`.

## Architecture and data flow

```mermaid
flowchart LR
    subgraph RabbitMQ[ RabbitMQ Streams ]
        S1[cmc-parser-stream]
        S2[bybit-parser-stream]
        S3[bybit-crypto-stream]
    end

    subgraph App[crypto-scout-collector (ActiveJ)]
        A1[AmqpConsumer]
        A2[CmcParserCollector]
        A3[BybitParserCollector]
        A4[BybitCryptoCollector]
        W[WebModule /health]
    end

    subgraph DB[(TimescaleDB)]
        T1[crypto_scout.cmc_fgi]
        T2[crypto_scout.bybit_spot_tickers]
        T3[crypto_scout.bybit_lpl]
    end

    S1 -->|Payload.CMC| A1 --> A2 --> T1
    S2 -->|Payload.BYBIT (LPL)| A1 --> A3 --> T3
    S3 -->|Payload.BYBIT (Spot tickers)| A1 --> A4 --> T2
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
    - `./script/init.sql` → `/docker-entrypoint-initdb.d/init.sql`
    - `./script/bybit_spot_tables.sql` → `/docker-entrypoint-initdb.d/02-bybit_spot_tables.sql`
    - `./script/cmc_parser_tables.sql` → `/docker-entrypoint-initdb.d/03-cmc_parser_tables.sql`
    - `./script/bybit_parser_tables.sql` → `/docker-entrypoint-initdb.d/04-bybit_parser_tables.sql`
    - `./script/bybit_linear_tables.sql` → `/docker-entrypoint-initdb.d/05-bybit_linear_tables.sql`
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
- Healthcheck: `curl -f http://localhost:8083/health` (interval 10s, timeout 3s, retries 5, start period 30s).
- Compose hardening:
    - `read_only: true`, `tmpfs: /tmp:rw,size=64m,mode=1777,nodev,nosuid`
    - `security_opt: [no-new-privileges=true]`, `cap_drop: [ALL]`
    - `cpus: "1.00"`, `memory: 1G`, `pids_limit: 256`, `ulimits.noFile: 4096:4096`
    - `user: "10001:10001"`, `init: true`, `restart: unless-stopped`, `stop_signal: SIGTERM`, `stop_grace_period: 30s`
- Networking: joins the external network `crypto-scout-bridge` to reach the DB service by name
  `crypto-scout-collector-db`.
    - `crypto_scout.cmc_fgi` — primary key `(id, timestamp)`.
    - `crypto_scout.bybit_spot_tickers` — primary key `(id, timestamp)`; indexes on `(timestamp)` and
      `(symbol, timestamp)`.
    - `crypto_scout.bybit_lpl` — primary key `(id, stake_begin_time)`.
    - `crypto_scout.stream_offsets` — primary key `(stream)`; stores last processed offset per stream.
    - `crypto_scout.bybit_linear_tickers` — primary key `(id, timestamp)`; symbol/timestamp indexes.
    - `crypto_scout.bybit_linear_kline_60m` — primary key `(id, start_time)`; unique `(symbol, start_time)`.
    - `crypto_scout.bybit_linear_public_trade` — primary key `(id, trade_time)`; unique `(symbol, trade_id)`.
    - `crypto_scout.bybit_linear_order_book_200` — primary key `(id, engine_time)`; indexes on `(symbol, engine_time)`
      and `(symbol, side, price)`.
    - `crypto_scout.bybit_linear_all_liqudation` — primary key `(id, event_time)`; indexes on `(symbol, event_time)`.
- External offset tracking for CMC, Bybit metrics, and Bybit spot streams:
    - `AmqpConsumer` starts consumers from DB offset and disables server-side tracking.
    - `CmcParserCollector`/`BybitParserCollector`/`BybitCryptoCollector` batch inserts and update offsets atomically.
- Compression policies (segmentby/orderby) for all three tables (compress chunks older than 7 days).
- Reorder policies align with time‑descending indexes.
- Retention: ~2 years for `cmc_fgi` and `bybit_lpl`, 180 days for `bybit_spot_tickers`.
- Grants and default privileges for role `crypto_scout_db`.

Offset handling: DB-backed external offsets are used for all streams except any future ephemeral consumers.

## Application configuration keys

From `src/main/resources/application.properties`:

- Server
    - `server.port` (default `8083`).
- RabbitMQ
    - `amqp.rabbitmq.host` (default `localhost`)
    - `amqp.rabbitmq.username` (default `crypto_scout_mq`)
    - `amqp.rabbitmq.password` (empty by default)
    - `amqp.rabbitmq.port` (default `5672`)
    - `amqp.stream.port` (default `5552`)
    - `amqp.bybit.crypto.stream` (default `bybit-crypto-stream`)
    - `amqp.bybit.parser.stream` (default `bybit-parser-stream`)
    - `amqp.cmc.parser.stream` (default `cmc-parser-stream`)
    - `amqp.collector.exchange`, `amqp.collector.queue`
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
  curl -s http://localhost:8083/health  # -> ok
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
  curl -s http://localhost:8083/health  # -> ok
  ```

- Build container image and run
  ```bash
  docker build -t crypto-scout-collector:0.0.1 .
  docker run --rm \
    --name crypto-scout-collector \
    --network <compose_network_name> \
    -p 8083:8083 \
    crypto-scout-collector:0.0.1
  ```

Ensure RabbitMQ Streams and TimescaleDB are reachable per configured host/ports.

## Offset handling

- **CMC stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
    - The consumer starts from `offset + 1` if present, otherwise from `first`.
    - On flush, `CmcParserCollector` inserts data and updates the max processed offset in a single transaction.
- **Bybit metrics stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
    - The consumer starts from `offset + 1` if present, otherwise from `first`.
    - On flush, `BybitParserCollector` inserts data and updates the max processed offset in a single transaction.
- **Bybit spot stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
    - The consumer starts from `offset + 1` if present, otherwise from `first`.
    - On flush, `BybitCryptoCollector` inserts data and updates the max processed offset in a single transaction.

If your DB was initialized before this change, apply the `stream_offsets` DDL manually or re-initialize the data dir to
pick up `script/init.sql` changes.

## Operations

- Health: `GET /health` returns `ok`.
- Logs: `src/main/resources/logback.xml` (console appender, INFO level).
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
- Implemented external DB-backed offset tracking for CMC stream:
    - New table `crypto_scout.stream_offsets`.
    - `AmqpConsumer` starts CMC consumer from DB offset and disables server-side tracking.
    - `CmcParserCollector` batches inserts and updates offset atomically.
- Split bootstrap SQL:
    - Moved `crypto_scout.bybit_spot_tickers` from `script/init.sql` to `script/bybit_spot_tables.sql`.
    - Moved `crypto_scout.cmc_fgi` to `script/cmc_parser_tables.sql`.
    - Moved `crypto_scout.bybit_lpl` to `script/bybit_parser_tables.sql`.
    - Updated compose volumes and documentation to mount and describe the new scripts.

## References to source

- `podman-compose.yml`
- `script/init.sql`
- `script/bybit_spot_tables.sql`
- `script/cmc_parser_tables.sql`
- `script/bybit_parser_tables.sql`
- `script/bybit_linear_tables.sql`
- `secret/*.env`, `secret/README.md`
- `src/main/resources/application.properties`, `logback.xml`
- `src/main/java/com/github/akarazhev/cryptoscout/*`
- `Dockerfile`, `pom.xml`
