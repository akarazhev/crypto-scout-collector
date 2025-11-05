package com.github.akarazhev.cryptoscout.config;

import com.github.akarazhev.jcryptolib.config.AppConfig;
import com.zaxxer.hikari.HikariConfig;

import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_CONNECTION_TIMEOUT_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_IDLE_TIMEOUT_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_MAX_LIFETIME_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_MAXIMUM_POOL_SIZE;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_MINIMUM_IDLE;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.HIKARI_REGISTER_MBEANS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_BYBIT_BATCH_SIZE;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_BYBIT_FLUSH_INTERVAL_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_CMC_BATCH_SIZE;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_CMC_FLUSH_INTERVAL_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_PASSWORD;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_URL;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_USERNAME;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.POOL_NAME;

public final class JdbcConfig {
    private JdbcConfig() {
        throw new UnsupportedOperationException();
    }

    public static String getUrl() {
        return AppConfig.getAsString(JDBC_URL);
    }

    public static String getUsername() {
        return AppConfig.getAsString(JDBC_USERNAME);
    }

    public static String getPassword() {
        return AppConfig.getAsString(JDBC_PASSWORD);
    }

    private static int getMaximumPoolSize() {
        return AppConfig.getAsInt(HIKARI_MAXIMUM_POOL_SIZE);
    }

    private static int getMinimumIdle() {
        return AppConfig.getAsInt(HIKARI_MINIMUM_IDLE);
    }

    private static long getConnectionTimeoutMs() {
        return AppConfig.getAsInt(HIKARI_CONNECTION_TIMEOUT_MS);
    }

    private static long getIdleTimeoutMs() {
        return AppConfig.getAsInt(HIKARI_IDLE_TIMEOUT_MS);
    }

    private static long getMaxLifetimeMs() {
        return AppConfig.getAsInt(HIKARI_MAX_LIFETIME_MS);
    }

    private static boolean getRegisterMbeans() {
        return AppConfig.getAsBoolean(HIKARI_REGISTER_MBEANS);
    }

    public static int getCmcBatchSize() {
        return AppConfig.getAsInt(JDBC_CMC_BATCH_SIZE);
    }

    public static long getCmcFlushIntervalMs() {
        return AppConfig.getAsInt(JDBC_CMC_FLUSH_INTERVAL_MS);
    }

    public static int getBybitBatchSize() {
        return AppConfig.getAsInt(JDBC_BYBIT_BATCH_SIZE);
    }

    public static long getBybitFlushIntervalMs() {
        return AppConfig.getAsInt(JDBC_BYBIT_FLUSH_INTERVAL_MS);
    }

    public static HikariConfig getHikariConfig() {
        final var config = new HikariConfig();
        config.setJdbcUrl(getUrl());
        config.setUsername(getUsername());
        config.setPassword(getPassword());
        config.setPoolName(POOL_NAME);
        config.setMaximumPoolSize(getMaximumPoolSize());
        config.setMinimumIdle(getMinimumIdle());
        config.setConnectionTimeout(getConnectionTimeoutMs());
        config.setIdleTimeout(getIdleTimeoutMs());
        config.setMaxLifetime(getMaxLifetimeMs());
        config.setRegisterMbeans(getRegisterMbeans());
        return config;
    }
}
