# crypto-scout-collector – Production Setup Report (v0.0.1)

## Executive summary

This report documents a production-ready setup for `crypto-scout-collector`, grounded in the source tree. The service
consumes crypto metrics from RabbitMQ Streams and persists time-series data into TimescaleDB. The repository includes a
compose definition for TimescaleDB with tuning and a backup sidecar, an initialization SQL that creates hypertables and
policies, and a container image definition for the collector.

- Database and backups: `podman-compose.yml` provisions TimescaleDB (`timescale/timescaledb:latest-pg17`) and a backup
  sidecar (`prodrigestivill/postgres-backup-local`) with env-driven scheduling and retention. Secrets are placed under
  `secret/`.
- Schema and policies: `script/init.sql` creates schema `crypto_scout`, hypertables, indexes, compression, reorder, and
  retention policies.
- Application: Java 25 (ActiveJ). Reads RabbitMQ Streams, batches inserts via HikariCP to TimescaleDB, exposes
  `GET /health`.
- Documentation: `README.md` updated to reflect Java 25, compose service names, and Docker base; this report added under
  `doc/0.0.1/`.

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
    - `script/init.sql`: installs `timescaledb` and `pg_stat_statements`; creates schema `crypto_scout`;
      creates/configures hypertables:
        - `crypto_scout.cmc_fgi`
        - `crypto_scout.bybit_spot_tickers`
        - `crypto_scout.bybit_lpl`
        - `crypto_scout.stream_offsets` (plain table for external offset tracking)
          Adds indexes, compression policies (7‑day threshold), reorder, and retention (180–730 days). Grants privileges
          to role `crypto_scout_db` and default privileges.

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
    - Collectors: `collector/AmqpConsumer.java`, `collector/MetricsCmcCollector.java`,
      `collector/MetricsBybitCollector.java`, `collector/CryptoBybitCollector.java`.
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
        S1[metrics-cmc-stream]
        S2[metrics-bybit-stream]
        S3[crypto-bybit-stream]
    end

    subgraph App[crypto-scout-collector (ActiveJ)]
        A1[AmqpConsumer]
        A2[MetricsCmcCollector]
        A3[MetricsBybitCollector]
        A4[CryptoBybitCollector]
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
- Volumes: `./data/postgresql:/var/lib/postgresql/data`, `./script/init.sql` mounted read-only to
  `/docker-entrypoint-initdb.d/init.sql`.
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
    - `cpus: "1.00"`, `memory: 1G`, `pids_limit: 256`, `ulimits.noFile: 4096:4096`
    - `user: "10001:10001"`, `init: true`, `restart: unless-stopped`, `stop_signal: SIGTERM`, `stop_grace_period: 30s`
- Networking: joins the external network `crypto-scout-bridge` to reach the DB service by name
  `crypto-scout-collector-db`.
  - `crypto_scout.cmc_fgi` — primary key `(id, timestamp)`.
  - `crypto_scout.bybit_spot_tickers` — primary key `(id, timestamp)`; indexes on `(timestamp)` and
    `(symbol, timestamp)`.
  - `crypto_scout.bybit_lpl` — primary key `(id, stake_begin_time)`.
  - `crypto_scout.stream_offsets` — primary key `(stream)`; stores last processed offset per stream.
    - External offset tracking for CMC stream: `AmqpConsumer` starts CMC consumer from DB offset and disables
      server-side tracking; `MetricsCmcCollector` batches inserts and updates offset atomically.
- Compression policies (segmentby/orderby) for all three tables (compress chunks older than 7 days).
- Reorder policies align with time‑descending indexes.
- Retention: ~2 years for `cmc_fgi` and `bybit_lpl`, 180 days for `bybit_spot_tickers`.
- Grants and default privileges for role `crypto_scout_db`.

Recommendation: validate chunk interval, compression, and retention windows vs. expected volume and query patterns
before rollout.

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
    - `amqp.crypto.bybit.stream` (default `crypto-bybit-stream`)
    - `amqp.metrics.bybit.stream` (default `metrics-bybit-stream`)
    - `amqp.metrics.cmc.stream` (default `metrics-cmc-stream`)
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

- **CMC stream:** external offset tracking stored in `crypto_scout.stream_offsets`.
  - The consumer starts from `offset + 1` if present, otherwise from `first`.
  - On flush, `MetricsCmcCollector` inserts data and updates the max processed offset in a single transaction.
- **Bybit streams:** continue with server-side offset tracking via manual acknowledgments.

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
  - `MetricsCmcCollector` batches inserts and updates offset atomically.

## References to source

- `podman-compose.yml`
- `script/init.sql`
- `secret/*.env`, `secret/README.md`
- `src/main/resources/application.properties`, `logback.xml`
- `src/main/java/com/github/akarazhev/cryptoscout/*`
- `Dockerfile`, `pom.xml`
