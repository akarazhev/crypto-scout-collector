---
name: podman-deployment
description: Podman Compose deployment patterns for crypto-scout-collector containerization with TimescaleDB and automated backups
license: MIT
compatibility: opencode
metadata:
  tools: podman
  services: timescaledb, rabbitmq
  domain: deployment
---

## What I Do

Guide containerized deployment of crypto-scout-collector with Podman, including TimescaleDB with automated backups and RabbitMQ Streams integration.

## Container Services

### crypto-scout-collector Container
- **Image**: `crypto-scout-collector:0.0.1`
- **Base**: `eclipse-temurin:25-jre-alpine`
- **User**: UID/GID `10001` (non-root)
- **Port**: `8081` (health endpoint)
- **Network**: `crypto-scout-bridge`
- **Dependencies**: TimescaleDB, RabbitMQ

### TimescaleDB Container
- **Image**: `timescale/timescaledb:latest-pg17`
- **Service**: `crypto-scout-collector-db`
- **Port**: `5432`
- **Data**: `./data/postgresql`
- **Init Scripts**: Mounted from `./script/` to `/docker-entrypoint-initdb.d/`
  - `00-init.sql` - extensions, schema, stream_offsets table
  - `01_bybit_spot_tables.sql` - spot market tables
  - `02_bybit_linear_tables.sql` - linear market tables
  - `03_crypto_scout_tables.sql` - CMC and risk tables
  - `04_btc_usd_daily_inserts.sql` - historical daily data
  - `05_btc_usd_weekly_inserts.sql` - historical weekly data
  - `06_cmc_fgi_inserts.sql` - CMC FGI historical data
  - `07_alternative_fgi_inserts.sql` - Alternative FGI data

### Backup Sidecar Container
- **Image**: `prodrigestivill/postgres-backup-local:latest`
- **Service**: `crypto-scout-collector-backup`
- **Output**: `./backups`
- **Schedule**: Configurable via env file

### RabbitMQ (external dependency)
- **Streams Port**: `5552`
- **AMQP Port**: `5672`
- **Management**: `15672`
- **Streams**: `bybit-stream`, `crypto-scout-stream`
- **Queues**: `collector-queue`, `chatbot-queue`

## Container Build & Run

```bash
# Build shaded JAR (required before building image)
mvn -q -DskipTests package

# Create network (once)
./script/network.sh

# Prepare secrets
cp secret/timescaledb.env.example secret/timescaledb.env
cp secret/postgres-backup.env.example secret/postgres-backup.env
cp secret/collector.env.example secret/collector.env
chmod 600 secret/*.env

# Edit secrets with your values
$EDITOR secret/timescaledb.env
$EDITOR secret/collector.env

# Build and start all services
podman-compose -f podman-compose.yml up -d

# Check health
curl -s http://localhost:8081/health

# View logs
podman logs -f crypto-scout-collector
```

## Environment Configuration

### Required Secrets

**secret/timescaledb.env**:
```env
POSTGRES_DB=crypto_scout
POSTGRES_USER=crypto_scout_db
POSTGRES_PASSWORD=your_secure_password
```

**secret/collector.env**:
```env
SERVER_PORT=8081
AMQP_RABBITMQ_HOST=host.containers.internal
AMQP_RABBITMQ_PORT=5672
AMQP_STREAM_PORT=5552
AMQP_RABBITMQ_USERNAME=crypto_scout_mq
AMQP_RABBITMQ_PASSWORD=your_mq_password
JDBC_DATASOURCE_URL=jdbc:postgresql://crypto-scout-collector-db:5432/crypto_scout
JDBC_DATASOURCE_USERNAME=crypto_scout_db
JDBC_DATASOURCE_PASSWORD=your_db_password
```

## Compose Hardening

The `podman-compose.yml` includes production hardening:
- `init: true` - proper signal handling
- `pids_limit: 256` - process limit
- `read_only` rootfs with `tmpfs: /tmp`
- `cap_drop: ALL` - drop all capabilities
- `security_opt: no-new-privileges=true`
- `cpus: 0.5`, `mem_limit: 256m`
- `restart: unless-stopped`
- healthcheck with `start_period: 30s`

## Database Initialization

### Fresh Installation
1. Ensure `./data/postgresql` is empty
2. Start containers - init scripts run automatically
3. Verify: `podman exec crypto-scout-collector-db psql -U crypto_scout_db -c "\dt crypto_scout.*"`

### Existing Database
For already-initialized databases, apply scripts manually:
```bash
podman exec -i crypto-scout-collector-db psql -U crypto_scout_db -d crypto_scout < script/bybit_spot_tables.sql
podman exec -i crypto-scout-collector-db psql -U crypto_scout_db -d crypto_scout < script/bybit_linear_tables.sql
podman exec -i crypto-scout-collector-db psql -U crypto_scout_db -d crypto_scout < script/crypto_scout_tables.sql
```

## Backup and Restore

### Automated Backups
Backups run on schedule defined in `secret/postgres-backup.env`:
```env
POSTGRES_BACKUP_SCHEDULE=@daily
POSTGRES_BACKUP_KEEP_DAYS=7
POSTGRES_BACKUP_KEEP_WEEKS=4
POSTGRES_BACKUP_KEEP_MONTHS=6
```

### Manual Restore
```bash
# From custom format dump
pg_restore -h localhost -p 5432 -U crypto_scout_db -d crypto_scout < backups/crypto_scout-YYYYMMDD.dump

# From SQL file
psql -h localhost -p 5432 -U crypto_scout_db -d crypto_scout < backups/crypto_scout-YYYYMMDD.sql
```

## Running the Service Locally

```bash
# Prerequisites: RabbitMQ and TimescaleDB running

# Set environment variables
export AMQP_RABBITMQ_PASSWORD=your_mq_password
export JDBC_DATASOURCE_PASSWORD=your_db_password

# Run the app
java -jar target/crypto-scout-collector-0.0.1.jar

# Health check
curl -s http://localhost:8081/health
```

## Troubleshooting

### Container not starting
- Verify Podman: `podman --version`
- Check logs: `podman logs crypto-scout-collector`
- Verify network: `podman network inspect crypto-scout-bridge`

### Database connection errors
- Check TimescaleDB health: `podman exec crypto-scout-collector-db pg_isready`
- Verify credentials in `secret/collector.env`
- Check network connectivity between containers

### RabbitMQ Streams not reachable
- Confirm port 5552 is accessible
- For host RabbitMQ: `AMQP_RABBITMQ_HOST=host.containers.internal`
- Verify Streams plugin is enabled

### Health check failing
- Check database connectivity
- Verify RabbitMQ credentials
- Review application logs: `podman logs crypto-scout-collector`

### Init scripts not applied
- Data directory must be empty on first run
- Re-initialize: `rm -rf ./data/postgresql` and restart

## When to Use Me

Use this skill when:
- Building and deploying the container image
- Configuring Podman Compose for production
- Setting up TimescaleDB with automated backups
- Troubleshooting container or connectivity issues
- Managing database initialization and migrations
- Setting up CI/CD pipelines
- Managing secrets and environment configuration
- Performing backup and restore operations
