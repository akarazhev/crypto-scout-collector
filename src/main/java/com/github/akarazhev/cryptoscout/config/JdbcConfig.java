/*
 * MIT License
 *
 * Copyright (c) 2025 Andrey Karazhev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_CRYPTO_SCOUT_BATCH_SIZE;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_CRYPTO_SCOUT_FLUSH_INTERVAL_MS;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_PASSWORD;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_URL;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_USERNAME;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.POOL_NAME;

public final class JdbcConfig {
    private JdbcConfig() {
        throw new UnsupportedOperationException();
    }

    private static String getUrl() {
        return AppConfig.getAsString(JDBC_URL);
    }

    private static String getUsername() {
        return AppConfig.getAsString(JDBC_USERNAME);
    }

    private static String getPassword() {
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

    public static int getCryptoScoutBatchSize() {
        final var size = AppConfig.getAsInt(JDBC_CRYPTO_SCOUT_BATCH_SIZE);
        if (size < 1 || size > 10000) {
            throw new IllegalArgumentException(
                "jdbc.crypto.scout.batch-size must be between 1 and 10000, got: " + size);
        }
        return size;
    }

    public static long getCryptoScoutFlushIntervalMs() {
        return AppConfig.getAsInt(JDBC_CRYPTO_SCOUT_FLUSH_INTERVAL_MS);
    }

    public static int getBybitBatchSize() {
        final var size = AppConfig.getAsInt(JDBC_BYBIT_BATCH_SIZE);
        if (size < 1 || size > 10000) {
            throw new IllegalArgumentException(
                "jdbc.bybit.batch-size must be between 1 and 10000, got: " + size);
        }
        return size;
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
