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

import com.github.akarazhev.jcryptolib.stream.Message;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.CONTENT_TYPE_JSON;
import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.DELIVERY_MODE_PERSISTENT;

public final class AmqpPublisher extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpPublisher.class);
    private final Executor executor;
    private final ConnectionFactory connectionFactory;
    private final String clientName;
    private final String queue;
    private volatile Connection connection;
    private volatile Channel channel;

    public static AmqpPublisher create(final NioReactor reactor, final Executor executor,
                                       final ConnectionFactory connectionFactory, final String clientName,
                                       final String queue) {
        return new AmqpPublisher(reactor, executor, connectionFactory, clientName, queue);
    }

    private AmqpPublisher(final NioReactor reactor, final Executor executor,
                          final ConnectionFactory connectionFactory, final String clientName,
                          final String queue) {
        super(reactor);
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.clientName = clientName;
        this.queue = queue;
    }

    @Override
    public Promise<Void> start() {
        return Promise.ofBlocking(executor, () -> {
            try {
                connection = connectionFactory.newConnection(clientName);
                channel = connection.createChannel();
                channel.confirmSelect();
                // Ensure the queue exists (will throw if it doesn't)
                channel.queueDeclarePassive(queue);
                LOGGER.info("AmqpPublisher started for queue: {}", queue);
            } catch (final Exception ex) {
                LOGGER.error("Failed to start AmqpPublisher for queue: {}", queue, ex);
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public Promise<Void> stop() {
        return Promise.ofBlocking(executor, () -> {
            try {
                if (channel != null) {
                    channel.close();
                    channel = null;
                }
            } catch (final Exception ex) {
                LOGGER.warn("Error closing AMQP channel for queue: {}", queue, ex);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                } catch (final Exception ex) {
                    LOGGER.warn("Error closing AMQP connection for queue: {}", queue, ex);
                }
            }
        });
    }

    public Promise<Void> publish(final String exchange, final String routingKey, final Message<?> message) {
        return Promise.ofBlocking(executor, () -> {
            if (channel == null || !channel.isOpen()) {
                throw new IllegalStateException("AMQP channel is not open. Call start() before publish().");
            }

            try {
                final var props = new AMQP.BasicProperties.Builder()
                        .contentType(CONTENT_TYPE_JSON)
                        .deliveryMode(DELIVERY_MODE_PERSISTENT)
                        .build();
                channel.basicPublish(exchange, routingKey, props, JsonUtils.object2Bytes(message));
                channel.waitForConfirmsOrDie();
            } catch (final Exception ex) {
                LOGGER.error("Failed to publish payload to AMQP queue {}: {}", queue, ex.getMessage(), ex);
                throw new RuntimeException(ex);
            }
        });
    }
}
