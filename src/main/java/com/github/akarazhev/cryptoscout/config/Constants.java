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

final class Constants {
    private Constants() {
        throw new UnsupportedOperationException();
    }

    final static class AmqpConfig {
        private AmqpConfig() {
            throw new UnsupportedOperationException();
        }

        static final String CONNECTION_NAME = "crypto-scout-collector";
        static final String AMQP_RABBITMQ_HOST = "amqp.rabbitmq.host";
        static final String AMQP_RABBITMQ_USERNAME = "amqp.rabbitmq.username";
        static final String AMQP_RABBITMQ_PASSWORD = "amqp.rabbitmq.password";
        static final String AMQP_STREAM_PORT = "amqp.stream.port";
        static final String AMQP_CRYPTO_BYBIT_STREAM = "amqp.crypto.bybit.stream";
        static final String AMQP_METRICS_BYBIT_STREAM = "amqp.metrics.bybit.stream";
        static final String AMQP_METRICS_CMC_STREAM = "amqp.metrics.cmc.stream";
        static final String AMQP_CRYPTO_SCOUT_EXCHANGE = "amqp.crypto.scout.exchange";
        static final String AMQP_COLLECTOR_QUEUE = "amqp.collector.queue";
        static final String AMQP_RABBITMQ_PORT = "amqp.rabbitmq.port";
    }

    final static class JdbcConfig {
        private JdbcConfig() {
            throw new UnsupportedOperationException();
        }

        static final String POOL_NAME = "crypto-scout-collector-pool";
        static final String JDBC_URL = "jdbc.datasource.url";
        static final String JDBC_USERNAME = "jdbc.datasource.username";
        static final String JDBC_PASSWORD = "jdbc.datasource.password";
        static final String JDBC_CMC_BATCH_SIZE = "jdbc.cmc.batch-size";
        static final String JDBC_CMC_FLUSH_INTERVAL_MS = "jdbc.cmc.flush-interval-ms";
        static final String JDBC_BYBIT_BATCH_SIZE = "jdbc.bybit.batch-size";
        static final String JDBC_BYBIT_FLUSH_INTERVAL_MS = "jdbc.bybit.flush-interval-ms";
        // HikariCP pool configuration
        static final String HIKARI_MAXIMUM_POOL_SIZE = "jdbc.hikari.maximum-pool-size";
        static final String HIKARI_MINIMUM_IDLE = "jdbc.hikari.minimum-idle";
        static final String HIKARI_CONNECTION_TIMEOUT_MS = "jdbc.hikari.connection-timeout-ms";
        static final String HIKARI_IDLE_TIMEOUT_MS = "jdbc.hikari.idle-timeout-ms";
        static final String HIKARI_MAX_LIFETIME_MS = "jdbc.hikari.max-lifetime-ms";
        static final String HIKARI_REGISTER_MBEANS = "jdbc.hikari.register-mbeans";
    }

    final static class ServerConfig {
        private ServerConfig() {
            throw new UnsupportedOperationException();
        }

        static final String SERVER_PORT = "server.port";
    }
}
