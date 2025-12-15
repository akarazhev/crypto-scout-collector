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

package com.github.akarazhev.cryptoscout.collector;

import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.rabbitmq.client.ConnectionFactory;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.HEALTH_CHECK_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.AMQP;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.CONNECTION_TIMEOUT_SECONDS;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.DATABASE;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.ERROR;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.STATUS;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.STATUS_DOWN;
import static com.github.akarazhev.cryptoscout.collector.Constants.Health.STATUS_UP;

public final class HealthService extends AbstractReactive {
    private final static Logger LOGGER = LoggerFactory.getLogger(HealthService.class);
    private final Executor executor;
    private final CollectorDataSource dataSource;
    private final ConnectionFactory connectionFactory;

    public static HealthService create(final NioReactor reactor, final Executor executor,
                                       final CollectorDataSource dataSource) {
        return new HealthService(reactor, executor, dataSource);
    }

    private HealthService(final NioReactor reactor, final Executor executor,
                          final CollectorDataSource dataSource) {
        super(reactor);
        this.executor = executor;
        this.dataSource = dataSource;
        this.connectionFactory = AmqpConfig.getConnectionFactory();
    }

    public Promise<Map<String, Object>> checkHealth() {
        return Promise.ofBlocking(executor, () -> {
            final var health = new LinkedHashMap<String, Object>();
            health.put(STATUS, STATUS_UP);

            final var dbHealth = checkDatabase();
            health.put(DATABASE, dbHealth);

            final var amqpHealth = checkAmqp();
            health.put(AMQP, amqpHealth);

            if (!STATUS_UP.equals(dbHealth.get(STATUS)) || !STATUS_UP.equals(amqpHealth.get(STATUS))) {
                health.put(STATUS, STATUS_DOWN);
            }

            return health;
        });
    }

    private Map<String, Object> checkDatabase() {
        final var dbHealth = new LinkedHashMap<String, Object>();
        try (final var conn = dataSource.getDataSource().getConnection()) {
            if (conn.isValid(CONNECTION_TIMEOUT_SECONDS)) {
                dbHealth.put(STATUS, STATUS_UP);
            } else {
                dbHealth.put(STATUS, STATUS_DOWN);
                dbHealth.put(ERROR, "Connection not valid");
            }
        } catch (final Exception ex) {
            LOGGER.debug("Database health check failed", ex);
            dbHealth.put(STATUS, STATUS_DOWN);
            dbHealth.put(ERROR, ex.getMessage());
        }

        return dbHealth;
    }

    private Map<String, Object> checkAmqp() {
        final var amqpHealth = new LinkedHashMap<String, Object>();
        try (final var connection = connectionFactory.newConnection(HEALTH_CHECK_CLIENT_NAME)) {
            if (connection.isOpen()) {
                amqpHealth.put(STATUS, STATUS_UP);
            } else {
                amqpHealth.put(STATUS, STATUS_DOWN);
                amqpHealth.put(ERROR, "Connection not open");
            }
        } catch (final Exception ex) {
            LOGGER.debug("AMQP health check failed", ex);
            amqpHealth.put(STATUS, STATUS_DOWN);
            amqpHealth.put(ERROR, ex.getMessage());
        }

        return amqpHealth;
    }
}
