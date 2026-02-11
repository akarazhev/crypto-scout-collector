---
name: java-code-style
description: Java 25 code style conventions for crypto-scout-collector including naming, imports, error handling, repository patterns, and testing
license: MIT
compatibility: opencode
metadata:
  language: java
  version: "25"
  domain: style-guide
---

## What I Do

Enforce consistent code style across the crypto-scout-collector microservice following established Java 25 conventions.

## File Structure

```
1-23:   MIT License header
25:     Package declaration
26:     Blank line
27+:    Imports (java.* → third-party → static, blank lines between groups)
        Blank line
        Class/enum/interface declaration
```

## Import Organization

```java
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

import com.rabbitmq.stream.Environment;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.akarazhev.cryptoscout.config.Constants.DB.JDBC_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
```

## Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `StreamService`, `BybitStreamService`, `CryptoScoutRepository` |
| Methods | camelCase with verb | `saveKline1m`, `getFgi`, `flush`, `consumePayload` |
| Constants | UPPER_SNAKE_CASE | `BATCH_SIZE`, `FLUSH_INTERVAL_MS`, `JDBC_URL` |
| Parameters/locals | `final var` | `final var timeout`, `final var data`, `final var count` |
| Test classes | `<ClassName>Test` | `StreamServiceTest`, `BybitSpotRepositoryTest` |
| Test methods | `should<Subject><Action>` | `shouldSaveKlineDataToDatabase`, `shouldFlushBufferOnBatchSize` |

## Access Modifiers

### Utility Classes
```java
final class Constants {
    private Constants() {
        throw new UnsupportedOperationException();
    }
    
    static final String PATH_SEPARATOR = "/";
    
    final static class DB {
        private DB() {
            throw new UnsupportedOperationException();
        }
        
        static final String JDBC_URL = System.getProperty("jdbc.datasource.url", "...");
        static final int BATCH_SIZE = Integer.parseInt(System.getProperty("jdbc.batch.size", "1000"));
    }
}
```

### Factory Pattern (Services)
```java
public final class BybitStreamService extends AbstractReactive implements ReactiveService {
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final Queue<OffsetPayload<Map<String, Object>>> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    
    public static BybitStreamService create(final NioReactor reactor, final Executor executor,
                                            final StreamOffsetsRepository streamOffsetsRepository,
                                            final BybitSpotRepository bybitSpotRepository,
                                            final BybitLinearRepository bybitLinearRepository) {
        return new BybitStreamService(reactor, executor, streamOffsetsRepository, 
                bybitSpotRepository, bybitLinearRepository);
    }
    
    private BybitStreamService(final NioReactor reactor, final Executor executor, ...) {
        super(reactor);
        this.executor = executor;
        // ...
    }
}
```

### Repository Classes
```java
public final class BybitSpotRepository {
    private final DataSource dataSource;
    
    public BybitSpotRepository(final DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    public int saveKline1m(final List<Map<String, Object>> klines, final long offset) throws SQLException
    public List<Map<String, Object>> getKline1m(final String symbol, final OffsetDateTime from, 
                                                final OffsetDateTime to) throws SQLException
}
```

## Error Handling

```java
// Unchecked exceptions for invalid state
if (dataSource == null) {
    throw new IllegalStateException("DataSource not initialized");
}

// Try-with-resources for all closeable resources
try (final var conn = dataSource.getConnection();
     final var stmt = conn.prepareStatement(sql);
     final var rs = stmt.executeQuery()) {
    while (rs.next()) {
        // Process results
    }
} catch (final SQLException e) {
    throw new IllegalStateException("Database error", e);
}

// Interrupt handling
try {
    Thread.sleep(duration.toMillis());
} catch (final InterruptedException e) {
    Thread.currentThread().interrupt();
}

// Exception chaining
throw new IllegalStateException("Failed to process batch", e);
```

## Logging

```java
private static final Logger LOGGER = LoggerFactory.getLogger(BybitStreamService.class);

// Info with context
LOGGER.info("Save {} spot 1m klines (tx) and updated offset {}", count, maxOffset);

// Warn with exception
LOGGER.warn("Error closing stream consumer", ex);

// Error with context and exception
LOGGER.error("Failed to process message from {}", streamName, exception);

// Startup/shutdown
LOGGER.info("StreamService started");
```

## Database Patterns

### Batch Insert with Transaction
```java
public int saveKline1m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
    final var sql = "INSERT INTO crypto_scout.bybit_spot_kline_1m (...) VALUES (...)" +
                    " ON CONFLICT (...) DO UPDATE SET ...";
    
    try (final var conn = dataSource.getConnection()) {
        conn.setAutoCommit(false);
        try (final var stmt = conn.prepareStatement(sql)) {
            for (final var kline : klines) {
                stmt.setString(1, (String) kline.get("symbol"));
                // ... set parameters
                stmt.addBatch();
            }
            final var results = stmt.executeBatch();
            
            // Update offset in same transaction
            upsertOffset(conn, stream, offset);
            
            conn.commit();
            return Arrays.stream(results).sum();
        } catch (final SQLException e) {
            conn.rollback();
            throw e;
        }
    }
}
```

### Query with Mapping
```java
public List<Map<String, Object>> getKline1m(final String symbol, final OffsetDateTime from, 
                                            final OffsetDateTime to) throws SQLException {
    final var sql = "SELECT * FROM crypto_scout.bybit_spot_kline_1m " +
                    "WHERE symbol = ? AND open_time BETWEEN ? AND ? ORDER BY open_time";
    final var results = new ArrayList<Map<String, Object>>();
    
    try (final var conn = dataSource.getConnection();
         final var stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, symbol);
        stmt.setObject(2, from);
        stmt.setObject(3, to);
        
        try (final var rs = stmt.executeQuery()) {
            while (rs.next()) {
                final var row = new HashMap<String, Object>();
                row.put("symbol", rs.getString("symbol"));
                row.put("open_time", rs.getObject("open_time", OffsetDateTime.class));
                // ... map columns
                results.add(row);
            }
        }
    }
    return results;
}
```

## Testing (JUnit 6/Jupiter)

```java
final class BybitSpotRepositoryTest {
    
    @BeforeAll
    static void setUp() {
        PodmanCompose.up();
    }
    
    @AfterAll
    static void tearDown() {
        PodmanCompose.down();
    }
    
    @Test
    void shouldSaveKlineDataToDatabase() throws Exception {
        final var klines = List.of(
            Map.of("symbol", "BTCUSDT", "open", "50000.00", ...)
        );
        final var count = repository.saveKline1m(klines, 100L);
        assertEquals(1, count);
    }
    
    @Test
    void shouldRetrieveKlineDataByTimeRange() throws Exception {
        final var from = OffsetDateTime.now().minusDays(1);
        final var to = OffsetDateTime.now();
        final var results = repository.getKline1m("BTCUSDT", from, to);
        assertNotNull(results);
        assertFalse(results.isEmpty());
    }
}
```

## Configuration Pattern

```java
static final String VALUE = System.getProperty("property.key", "defaultValue");
static final int PORT = Integer.parseInt(System.getProperty("port.key", "5552"));
static final long INTERVAL = Long.parseLong(System.getProperty("interval.key", "1000"));
static final Duration TIMEOUT = Duration.ofMinutes(Long.getLong("timeout.key", 3L));

// Validation
static {
    if (PASSWORD == null || PASSWORD.isEmpty()) {
        throw new IllegalStateException("Property 'amqp.rabbitmq.password' must be set");
    }
    if (BATCH_SIZE < 1 || BATCH_SIZE > 10000) {
        throw new IllegalStateException("Batch size must be between 1 and 10000");
    }
}
```

## Stream Processing Patterns

### Consumer with External Offset
```java
private void updateOffset(final String stream, final SubscriptionContext context) {
    reactor.execute(() -> Promise.ofBlocking(executor, () -> streamOffsetsRepository.getOffset(stream))
        .then(saved -> {
            if (saved.isPresent()) {
                context.offsetSpecification(OffsetSpecification.offset(saved.getAsLong() + 1));
                LOGGER.info("Consumer starting from DB offset {}+1 for stream {}", saved.getAsLong(), stream);
            } else {
                context.offsetSpecification(OffsetSpecification.first());
                LOGGER.info("Consumer starting from first for stream {}", stream);
            }
            return Promise.complete();
        })
        .whenComplete((_, ex) -> {
            if (ex != null) {
                LOGGER.warn("Failed to load offset from DB, starting from first", ex);
                context.offsetSpecification(OffsetSpecification.first());
            }
        })
    );
}
```

### Batching with Concurrent Protection
```java
private final AtomicBoolean flushInProgress = new AtomicBoolean(false);

private void scheduledFlush() {
    if (!running.get() || flushInProgress.getAndSet(true)) {
        return;
    }
    flush().whenComplete((_, _) -> {
        flushInProgress.set(false);
        if (running.get()) {
            reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        }
    });
}
```

## When to Use Me

Use this skill when:
- Writing new Java classes or methods
- Reviewing code for style compliance
- Understanding project conventions
- Organizing imports and file structure
- Implementing error handling patterns
- Creating database repositories
- Implementing batch processing
- Writing unit and integration tests
