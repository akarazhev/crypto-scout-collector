# AGENTS.md

This document provides guidelines for agentic coding contributors to this repository.

## Project Overview

Java 25 Maven microservice that collects crypto market data from Bybit and CoinMarketCap, then publishes structured events to RabbitMQ Streams. Built on ActiveJ for fully async I/O.

## Build, Test, and Lint Commands

### Build
```bash
mvn clean install
mvn -q -DskipTests install
```

### Run All Tests
```bash
mvn test
mvn -q test
```

### Run Single Test
```bash
mvn test -Dtest=MockBybitSpotDataTest
mvn test -Dtest=MockBybitSpotDataTest#shouldSpotKline1DataReturnMap
mvn -q test -Dtest=MockBybitSpotDataTest
```

### Run Tests with System Properties
```bash
mvn -q -Dpodman.compose.up.timeout.min=5 test
mvn -q -Dtest.db.jdbc.url=jdbc:postgresql://localhost:5432/crypto_scout test
```

### Clean
```bash
mvn clean
```

## Code Style Guidelines

### File Structure
- MIT License header at top (23 lines)
- Package declaration on line 25
- One blank line before imports
- Imports organized: java.*, third-party, then static imports (each group separated by blank line)
- One blank line after imports
- Class/enum/interface declaration
- No trailing whitespace

### Imports
```java
import java.io.IOException;
import java.nio.file.Path;

import com.rabbitmq.stream.Environment;
import org.slf4j.Logger;

import static com.github.akarazhev.cryptoscout.test.Constants.DB.JDBC_URL;
```

### Naming Conventions
- **Classes**: PascalCase (e.g., `StreamTestPublisher`, `MockData`)
- **Methods**: camelCase starting with lowercase verb (e.g., `waitForDatabaseReady`, `deleteFromTables`)
- **Constants**: UPPER_SNAKE_CASE in nested static classes (e.g., `JDBC_URL`, `DB_USER`)
- **Parameters and locals**: camelCase using `final var` (e.g., `final var timeout`, `final var data`)
- **Test classes**: `<ClassName>Test` suffix (e.g., `MockBybitSpotDataTest`)
- **Test methods**: `should<Subject><Action>` pattern (e.g., `shouldSpotKline1DataReturnMap`)

### Access Modifiers
- **Utility classes**: package-private with private constructor throwing `UnsupportedOperationException`
- **Nested constant classes**: `final static` with private constructor throwing `UnsupportedOperationException`
- **Factory methods**: `public static` named `create()`
- **Instance fields**: `private final` or `private volatile` for thread-safe lazy initialization
- **Static fields**: `private static final`
- **Methods**: `public`, `private`, or package-private as needed

### Type System
- Java 25 with `maven.compiler.release=25`
- Use `final var` for local variable type inference when type is obvious
- Explicit types when readability improves
- `Map<String, Object>` for JSON data structures

### Error Handling
- **Unchecked exceptions**: Use `IllegalStateException` for invalid state/conditions
- **Resource not found**: `IllegalStateException` with descriptive message
- **Try-with-resources**: Always for `Connection`, `Statement`, `ResultSet`, `InputStream`, `OutputStream`
- **Exception parameters**: `final Exception e` or `final Exception ex`
- **Interrupt handling**: `Thread.currentThread().interrupt()` in catch blocks for `InterruptedException`
- **Logging exceptions**: Include message and exception: `LOGGER.error("Failed to connect", e)`
- **Exception chaining**: Wrap with cause: `throw new IllegalStateException(msg, e)`
- **Return false on expected failures**: e.g., `canConnect()` returns `false` instead of throwing

### Method Design
- **Factory pattern**: Static `create()` methods returning new instances
- **Void methods**: Use for side effects
- **Boolean methods**: Return `true`/`false`, don't throw for expected failures
- **Duration parameters**: Use `java.time.Duration` instead of `long millis`
- **Varargs**: For optional lists of items (e.g., `deleteFromTables(String... tables)`)

### Testing (JUnit 6/Jupiter)
- **Test classes**: Package-private, no modifiers (e.g., `final class MockBybitSpotDataTest`)
- **Lifecycle methods**: `@BeforeAll static void setUp()`, `@AfterAll static void tearDown()`
- **Test methods**: `@Test void should...() throws Exception`
- **Assertions**: Import from `org.junit.jupiter.api.Assertions` using static imports
- **No test runners**: Use standard JUnit 5 patterns

### Logging
- **Logger field**: `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class)`
- **Log levels**: `info()` for important events, `warn()` for recoverable issues, `error()` for failures
- **Messages**: Descriptive, include context (e.g., `"Connected to DB: {}"`, `"Failed to start publisher"`)
- **Exceptions**: Pass as second parameter: `LOGGER.error("Description", exception)`

### Constants Organization
Group related constants in nested static classes:
```java
final class Constants {
    static final String PATH_SEPARATOR = "/";

    final static class DB {
        static final String JDBC_URL = System.getProperty("test.db.jdbc.url", "...");
        static final String DB_USER = System.getProperty("test.db.user", "...");
    }

    final static class PodmanCompose {
        static final Duration UP_TIMEOUT = Duration.ofMinutes(...);
    }
}
```

### Concurrency
- **Volatile fields**: For lazy-initialized singleton-style fields
- **Thread naming**: Provide names for background threads
- **Interruption**: Always restore interrupt status when catching `InterruptedException`
- **Daemon threads**: Set for background readers that shouldn't block JVM shutdown

### Resource Management
- **Try-with-resources**: Required for all closeable resources (SQL, streams, connections)
- **Null checks**: Throw `IllegalStateException` for null resources
- **Timeout handling**: Throw `IllegalStateException` with descriptive message including timeout value
- **Process management**: `destroyForcibly()` after timeout, preserve partial output in exception message

### Code Organization
- **Static imports**: From project's `Constants` class heavily used
- **Method length**: Keep reasonable, extract private helpers if too long
- **Static blocks**: Used for initialization that may throw (e.g., resource resolution in `PodmanCompose`)
- **Enum design**: Enums can have fields and methods (see `MockData.Source` and `MockData.Type`)

### System Properties
All configuration via system properties with defaults:
```java
static final String VALUE = System.getProperty("property.key", "defaultValue");
static final int PORT = Integer.parseInt(System.getProperty("port.key", "5552"));
static final Duration TIMEOUT = Duration.ofMinutes(Long.getLong("timeout.key", 3L));
```
