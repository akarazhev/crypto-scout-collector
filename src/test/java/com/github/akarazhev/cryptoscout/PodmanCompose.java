package com.github.akarazhev.cryptoscout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public final class PodmanCompose {
    private static final String PODMAN_COMPOSE_CMD = System.getProperty("podman.compose.cmd", "podman-compose");
    private static final String PODMAN_CMD = System.getProperty("podman.cmd", "podman");
    private static final String COMPOSE_FILE_NAME = "podman-compose.yml";
    private static final Path COMPOSE_DIR;
    private static final String DB_CONTAINER_NAME = "crypto-scout-collector-db";
    private static final String JDBC_URL = System.getProperty("test.db.jdbc.url", "jdbc:postgresql://localhost:5432/crypto_scout");
    private static final String DB_USER = System.getProperty("test.db.user", "crypto_scout_db");
    private static final String DB_PASSWORD = System.getProperty("test.db.password", "crypto_scout_db");
    private static final Duration UP_TIMEOUT = Duration.ofMinutes(Long.getLong("podman.compose.up.timeout.min", 3L));
    private static final Duration DOWN_TIMEOUT = Duration.ofMinutes(Long.getLong("podman.compose.down.timeout.min", 1L));
    private static final Duration READY_RETRY_INTERVAL = Duration.ofSeconds(Long.getLong("podman.compose.ready.interval.sec", 2L));

    static {
        final var resourceUrl = PodmanCompose.class.getClassLoader().getResource(COMPOSE_FILE_NAME);
        if (resourceUrl == null) {
            throw new IllegalStateException("Resource not found: " + COMPOSE_FILE_NAME);
        }

        try {
            final var composeFile = Paths.get(resourceUrl.toURI());
            if (!Files.exists(composeFile)) {
                throw new IllegalStateException("Compose file not found on disk: " + composeFile);
            }

            COMPOSE_DIR = composeFile.getParent();
            if (COMPOSE_DIR == null || !Files.isDirectory(COMPOSE_DIR)) {
                throw new IllegalStateException("Compose directory is invalid: " + COMPOSE_DIR);
            }
        } catch (final URISyntaxException e) {
            throw new IllegalStateException("Failed to resolve compose file URI", e);
        }
    }

    private PodmanCompose() {
        throw new UnsupportedOperationException();
    }

    public static void up() {
        // Start containers
        runCommand(COMPOSE_DIR, UP_TIMEOUT, PODMAN_COMPOSE_CMD, "-f", COMPOSE_FILE_NAME, "up", "-d");
        // Wait for DB readiness
        waitForDatabaseReady(UP_TIMEOUT);
    }

    public static void down() {
        // Stop and remove containers
        runCommand(COMPOSE_DIR, DOWN_TIMEOUT, PODMAN_COMPOSE_CMD, "-f", COMPOSE_FILE_NAME, "down");
        // Wait until DB container is removed
        waitForContainerRemoval(DB_CONTAINER_NAME, DOWN_TIMEOUT);
    }

    private static void waitForDatabaseReady(final Duration timeout) {
        final var deadline = System.nanoTime() + timeout.toNanos();
        final var loginTimeoutSec = (int) Math.max(1L, READY_RETRY_INTERVAL.getSeconds());
        DriverManager.setLoginTimeout(loginTimeoutSec);

        while (System.nanoTime() < deadline) {
            if (canConnect()) {
                return;
            }

            sleep(READY_RETRY_INTERVAL);
        }

        throw new IllegalStateException("Database was not ready within " + timeout.toSeconds() + " seconds");
    }

    private static boolean canConnect() {
        try (final var conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             final var st = conn.createStatement();
             final var rs = st.executeQuery("SELECT 1")) {
            return rs.next();
        } catch (final SQLException e) {
            return false;
        }
    }

    private static void waitForContainerRemoval(final String containerName, final Duration timeout) {
        final var deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (!isContainerPresent(containerName)) {
                return;
            }

            sleep(READY_RETRY_INTERVAL);
        }

        throw new IllegalStateException("Container still present after timeout: " + containerName);
    }

    private static boolean isContainerPresent(final String containerName) {
        final var out = runAndCapture(COMPOSE_DIR, Duration.ofSeconds(15), PODMAN_CMD,
                "ps", "-a", "--format", "{{.Names}}");
        final var lines = out.split("\\R");
        for (final String line : lines) {
            if (containerName.equals(line.trim())) {
                return true;
            }
        }

        return false;
    }

    private static void runCommand(final Path dir, final Duration timeout, final String... command) {
        final var output = runAndCapture(dir, timeout, command);
        // best-effort to show output on success in debug scenarios
        if (!output.isEmpty()) {
            System.out.println(output);
        }
    }

    private static String runAndCapture(final Path dir, final Duration timeout, final String... command) {
        final var cmd = Arrays.asList(command);
        final var pb = new ProcessBuilder(cmd);
        pb.directory(dir.toFile());
        pb.redirectErrorStream(true);
        try {
            final var p = pb.start();
            final var out = new StringBuilder();
            final var reader = new Thread(() -> {
                try (final var br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        out.append(line).append(System.lineSeparator());
                    }

                } catch (final IOException ignored) {
                }
            }, "podman-compose-output");
            reader.setDaemon(true);
            reader.start();

            final var finished = p.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                p.destroyForcibly();
                throw new IllegalStateException("Command timed out: " + String.join(" ", cmd) +
                        "\nPartial output:\n" + out);
            }

            final var exit = p.exitValue();
            if (exit != 0) {
                throw new IllegalStateException("Command failed (" + exit + "): " + String.join(" ", cmd) +
                        "\nOutput:\n" + out);
            }

            return out.toString();
        } catch (final IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new IllegalStateException("Failed to run command: " + String.join(" ", command), e);
        }
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
