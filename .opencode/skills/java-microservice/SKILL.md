---
name: java-microservice
description: Java 25 microservice development patterns for crypto-scout-collector including stream consumption, batch processing, and repository patterns
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
ActiveJ DI modules for separation of concerns:
```java
// CoreModule - reactor and virtual-thread executor
// WebModule - HTTP server with /health endpoint
// CollectorModule - DI wiring for repositories and services
```

### StreamService
Consumes from RabbitMQ Streams with database-backed offset management:
```java
// Subscribes to: crypto-scout-stream, bybit-stream
// External offset tracking in crypto_scout.stream_offsets
// Dispatches to: CryptoScoutService, BybitStreamService, AnalystService
```

### BybitStreamService
Processes Bybit market data with batching:
```java
// Spot data: PMST source - klines, tickers, trades, order books
// Linear data: PML source - klines, tickers, trades, order books, liquidations
// Batching: Configurable batch size and flush interval
// Exactly-once: Transactional data+offset writes
```

### CryptoScoutService
Processes CMC data with batching:
```java
// Fear & Greed Index (FGI source)
// BTC/USD daily klines (BTC_USD_1D source)
// BTC/USD weekly klines (BTC_USD_1W source)
// Batching and transactional offset updates
```

### Repository Pattern
JDBC/HikariCP repositories for database access:
```java
// BybitSpotRepository - spot market data
// BybitLinearRepository - linear/perps market data
// CryptoScoutRepository - CMC FGI and klines
// StreamOffsetsRepository - offset management
// AnalystRepository - technical analysis data
```

### AmqpConsumer/AmqpPublisher
AMQP integration for control messages:
```java
// Consumer: receives data queries from collector-queue
// Publisher: sends responses to chatbot-queue
// Automatic reconnection with exponential backoff
```

### DataService
Handles AMQP request/response:
```java
// Processes incoming query messages
// Coordinates repository calls
// Formats and publishes responses
```

### Health Endpoint
```java
// GET /health - returns JSON status
// {"status":"UP","database":{"status":"UP"},"amqp":{"status":"UP"}}
// HTTP 200 when healthy, HTTP 503 when degraded
```

## Architecture Patterns

### Batching Service Pattern
```java
public final class ExampleService extends AbstractReactive implements ReactiveService {
    private final Queue<OffsetPayload<Map<String, Object>>> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    @Override
    public Promise<Void> start() {
        running.set(true);
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        return Promise.complete();
    }
    
    public Promise<Void> save(final Payload<Map<String, Object>> payload, final long offset) {
        buffer.add(OffsetPayload.of(payload, offset));
        if (buffer.size() >= batchSize) {
            return flush();
        }
        return Promise.complete();
    }
    
    private Promise<Void> flush() {
        // Drain buffer, batch insert, update offset transactionally
    }
}
```

### Repository Pattern
```java
public final class ExampleRepository {
    private final DataSource dataSource;
    
    public int saveBatch(final List<Map<String, Object>> items, final long offset) throws SQLException {
        try (final var conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Insert data batch
                final var count = insertBatch(conn, items);
                // Update offset
                upsertOffset(conn, stream, offset);
                conn.commit();
                return count;
            } catch (final SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }
}
```

## Configuration

All settings via system properties or environment variables:

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8081` | HTTP server port |
| `amqp.rabbitmq.host` | `localhost` | RabbitMQ host |
| `amqp.stream.port` | `5552` | RabbitMQ Streams port |
| `amqp.rabbitmq.username` | `crypto_scout_mq` | RabbitMQ user |
| `amqp.rabbitmq.password` | (empty) | RabbitMQ password (required) |
| `amqp.bybit.stream` | `bybit-stream` | Bybit data stream name |
| `amqp.crypto.scout.stream` | `crypto-scout-stream` | CMC data stream name |
| `jdbc.datasource.url` | `jdbc:postgresql://localhost:5432/crypto_scout` | Database URL |
| `jdbc.datasource.username` | `crypto_scout_db` | Database user |
| `jdbc.datasource.password` | (empty) | Database password (required) |
| `jdbc.bybit.batch-size` | `1000` | Bybit batch size (1-10000) |
| `jdbc.bybit.flush-interval-ms` | `1000` | Bybit flush interval |
| `jdbc.crypto.scout.batch-size` | `100` | CMC batch size (1-10000) |
| `jdbc.crypto.scout.flush-interval-ms` | `1000` | CMC flush interval |

## When to Use Me

Use this skill when:
- Implementing new stream consumers or services
- Creating new database repositories
- Understanding the microservice architecture
- Working with RabbitMQ Streams consumption
- Implementing batch processing patterns
- Adding health checks or observability features
- Configuring database connection pooling
- Understanding offset management patterns
