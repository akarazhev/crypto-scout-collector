---
name: podman-deployment
description: Podman Compose deployment patterns for crypto-scout-collector containerization and TimescaleDB integration
license: MIT
compatibility: opencode
metadata:
  tools: podman
  services: rabbitmq
  domain: deployment
---

## What I Do

Guide containerized deployment of crypto-scout-collector with Podman, including TimescaleDB and RabbitMQ Streams integration.

## Container Services

### crypto-scout-collector Container
- **Image**: `crypto-scout-collector:0.0.1`
- **Base**: `eclipse-temurin:25-jre-alpine`
- **User**: UID/GID `10001` (non-root)
- **Port**: `8081` (internal, not exposed to host)
- **Network**: `crypto-scout-bridge`

### RabbitMQ (external dependency)
- **Streams Port**: `5552`
- **User/Password**: `crypto_scout_mq` / configured via env
- **Streams**: `bybit-stream`, `crypto-scout-stream`

## Container Build & Run

```bash
# Build shaded JAR
mvn clean package -DskipTests

# Build container image
podman build -t crypto-scout-client:0.0.1 .

# Create network (once)
podman network create crypto-scout-bridge

# Run with compose
podman-compose -f podman-compose.yml up -d

# Check health
podman inspect --format='{{.State.Health.Status}}' crypto-scout-parser-client
```

## Environment Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8081` | HTTP server port |
| `AMQP_RABBITMQ_HOST` | `localhost` | RabbitMQ host |
| `AMQP_STREAM_PORT` | `5552` | RabbitMQ Streams port |
| `AMQP_RABBITMQ_USERNAME` | `crypto_scout_mq` | RabbitMQ user |
| `AMQP_RABBITMQ_PASSWORD` | (empty) | RabbitMQ password |
| `BYBIT_STREAM_MODULE_ENABLED` | `false` | Enable Bybit modules |
| `CMC_PARSER_MODULE_ENABLED` | `true` | Enable CMC module |

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

## Secrets Management

Secrets are managed via env files:
- `secret/parser-client.env` - runtime secrets (gitignored)
- `secret/client.env.example` - template for secrets

```bash
cp secret/client.env.example secret/parser-client.env
$EDITOR secret/parser-client.env
```

## Running the Service

```bash
# Local run (after build)
java -jar target/crypto-scout-client-0.0.1.jar

# Health check
curl -fsS http://localhost:8081/health

# With compose
podman-compose -f podman-compose.yml up -d
podman logs -f crypto-scout-parser-client
```

## Troubleshooting

### Container not starting
- Verify Podman is installed: `podman --version`
- Check podman-compose: `podman-compose --version`
- Check logs: `podman logs crypto-scout-parser-client`

### RabbitMQ Streams not reachable
- Confirm port 5552 is accessible
- For host RabbitMQ: `AMQP_RABBITMQ_HOST=host.containers.internal`
- Verify Streams plugin is enabled on RabbitMQ

### Health check failing
- Check RabbitMQ connectivity
- Verify streams exist: `bybit-stream`, `crypto-scout-stream`
- Check credentials in env file

## When to Use Me

Use this skill when:
- Building and deploying the container image
- Configuring Podman Compose for production
- Troubleshooting container or connectivity issues
- Setting up CI/CD pipelines
- Managing secrets and environment configuration
