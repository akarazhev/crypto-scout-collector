---
name: java-microservice
description: Java 25 microservice development patterns for crypto-scout-collector including modules, AMQP consuming, and stream processing
license: MIT
compatibility: opencode
metadata:
  language: java
  framework: activej
  domain: microservice
---

## What I Do

Provide guidance for developing and maintaining the crypto-scout-collector microservice, a Java 25 Maven application that consumes crypto market data from RabbitMQ Streams and persists to TimescaleDB.

## Core Components

### Modules
Activej DI modules for separation of concerns:
```java
// CoreModule - reactor and executor (virtual threads)
// WebModule - HTTP server, clients, health routes, DNS
// ClientModule - AMQP publisher lifecycle
// BybitSpotModule - Spot WebSocket streams + consumers
// BybitLinearModule - Linear WebSocket streams + consumers
// CmcParserModule - CMC HTTP parser + consumer
```

### AmqpPublisher
Publishes structured events to RabbitMQ Streams:
```java
// Routes payloads to configured streams based on provider/source
amqpPublisher.publish(payload);  // Bybit data -> bybit-stream
amqpPublisher.publish(cmcPayload);  // CMC data -> crypto-scout-stream
```

### Bybit Stream Consumers
WebSocket consumers for market data:
```java
// AbstractBybitStreamConsumer base class provides common lifecycle
// BybitSpotBtcUsdtConsumer, BybitSpotEthUsdtConsumer
// BybitLinearBtcUsdtConsumer, BybitLinearEthUsdtConsumer
```

### CmcParserConsumer
HTTP-based data collection:
```java
// Retrieves Fear & Greed Index (API Pro Latest)
// Retrieves BTC/USD quotes (1D, 1W)
// Publishes to crypto-scout-stream
```

### Health Endpoints
```java
// GET /health - returns "ok" when ready, 503 otherwise
// Used for liveness and readiness checks
```

## Configuration

All settings via system properties or environment variables:

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8081` | HTTP server port |
| `amqp.rabbitmq.host` | `localhost` | RabbitMQ host |
| `amqp.stream.port` | `5552` | RabbitMQ Streams port |
| `amqp.rabbitmq.username` | `crypto_scout_mq` | RabbitMQ user |
| `amqp.bybit.stream` | `bybit-stream` | Bybit data stream name |
| `amqp.crypto.scout.stream` | `crypto-scout-stream` | CMC data stream name |
| `bybit.stream.module.enabled` | `false` | Enable Bybit modules |
| `cmc.parser.module.enabled` | `true` | Enable CMC module |

## When to Use Me

Use this skill when:
- Implementing new modules or consumers
- Understanding the microservice architecture
- Configuring AMQP publishing and streams
- Working with Bybit WebSocket or CMC HTTP APIs
- Adding health checks or observability features
