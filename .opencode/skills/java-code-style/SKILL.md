---
name: java-code-style
description: Java 25 code style conventions for crypto-scout-collector including naming, imports, error handling, and testing patterns
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
import java.time.Duration;

import com.rabbitmq.stream.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.akarazhev.cryptoscout.test.Constants.DB.JDBC_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
```

## Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Classes | PascalCase | `StreamTestPublisher`, `MockData` |
| Methods | camelCase with verb | `waitForDatabaseReady`, `deleteFromTables` |
| Constants | UPPER_SNAKE_CASE | `JDBC_URL`, `DB_USER` |
| Parameters/locals | `final var` | `final var timeout`, `final var data` |
| Test classes | `<ClassName>Test` | `MockBybitSpotDataTest` |
| Test methods | `should<Subject><Action>` | `shouldSpotKline1DataReturnMap` |

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
        
        static final String JDBC_URL = System.getProperty("test.db.jdbc.url", "...");
    }
}
```

### Factory Pattern
```java
public final class StreamTestPublisher extends AbstractReactive implements ReactiveService {
    private final Executor executor;
    private volatile Producer producer;
    
    public static StreamTestPublisher create(final NioReactor reactor, final Executor executor,
                                             final Environment environment, final String stream) {
        return new StreamTestPublisher(reactor, executor, environment, stream);
    }
    
    private StreamTestPublisher(final NioReactor reactor, final Executor executor,
                                final Environment environment, final String stream) {
        super(reactor);
        this.executor = executor;
        // ...
    }
}
```

## Error Handling

```java
// Unchecked exceptions for invalid state
if (is == null) {
    throw new IllegalStateException("Resource not found: " + path);
}

// Try-with-resources for all closeable resources
try (final var conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
     final var st = conn.createStatement();
     final var rs = st.executeQuery(SELECT_ONE)) {
    return rs.next();
} catch (final SQLException e) {
    return false;
}

// Interrupt handling
try {
    Thread.sleep(duration.toMillis());
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
}

// Exception chaining
throw new IllegalStateException("Failed to resolve compose file URI", e);
```

## Logging

```java
private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);

LOGGER.info("Connected to DB: {}", conn.getClientInfo());
LOGGER.warn("Error closing stream producer", ex);
LOGGER.error("Failed to start StreamTestPublisher", ex);
```

## Testing (JUnit 6/Jupiter)

```java
final class MockBybitSpotDataTest {
    
    @BeforeAll
    static void setUp() {
        PodmanCompose.up();
    }
    
    @AfterAll
    static void tearDown() {
        PodmanCompose.down();
    }
    
    @Test
    void shouldSpotKline1DataReturnMap() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        assertNotNull(data);
        assertFalse(data.isEmpty());
    }
}
```

## Configuration Pattern

```java
static final String VALUE = System.getProperty("property.key", "defaultValue");
static final int PORT = Integer.parseInt(System.getProperty("port.key", "5552"));
static final Duration TIMEOUT = Duration.ofMinutes(Long.getLong("timeout.key", 3L));
```

## When to Use Me

Use this skill when:
- Writing new Java classes or methods
- Reviewing code for style compliance
- Understanding project conventions
- Organizing imports and file structure
- Implementing error handling patterns
