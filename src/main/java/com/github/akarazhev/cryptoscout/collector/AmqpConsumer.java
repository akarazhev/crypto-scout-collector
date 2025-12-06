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

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public final class AmqpConsumer extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);
    private final Executor executor;
    private final ConnectionFactory connectionFactory;
    private final String clientName;
    private final String queue;
    private final Consumer<byte[]> messageHandler;
    private volatile Connection connection;
    private volatile Channel channel;
    private volatile String consumerTag;

    public static AmqpConsumer create(final NioReactor reactor, final Executor executor,
                                      final ConnectionFactory connectionFactory, final String clientName,
                                      final String queue, final Consumer<byte[]> messageHandler) {
        return new AmqpConsumer(reactor, executor, connectionFactory, clientName, queue, messageHandler);
    }

    private AmqpConsumer(final NioReactor reactor, final Executor executor,
                         final ConnectionFactory connectionFactory, final String clientName,
                         final String queue, final Consumer<byte[]> messageHandler) {
        super(reactor);
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.clientName = clientName;
        this.queue = queue;
        this.messageHandler = messageHandler;
    }

    @Override
    public Promise<Void> start() {
        return Promise.ofBlocking(executor, () -> {
            try {
                connection = connectionFactory.newConnection(clientName);
                channel = connection.createChannel();
                channel.basicQos(1);
                channel.queueDeclarePassive(queue);

                final DeliverCallback deliver = (_, delivery) -> {
                    try {
                        final var body = delivery.getBody();
                        reactor.execute(() -> Promise.ofBlocking(executor, () -> messageHandler.accept(body)));
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } catch (final Exception e) {
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        } catch (final IOException ex) {
                            LOGGER.debug("Error nacking AMQP message", ex);
                        }

                        LOGGER.error("Failed to process message", e);
                    }
                };

                final CancelCallback cancel =
                        tag -> LOGGER.debug("AMQP consumer cancelled: {}", tag);
                consumerTag = channel.basicConsume(queue, false, deliver, cancel);
                LOGGER.info("AmqpConsumer started for queue: {}", queue);
            } catch (final Exception ex) {
                LOGGER.error("Failed to start AmqpConsumer for queue: {}", queue, ex);
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public Promise<Void> stop() {
        return Promise.ofBlocking(executor, () -> {
            try {
                if (channel != null) {
                    try {
                        if (consumerTag != null) {
                            channel.basicCancel(consumerTag);
                            consumerTag = null;
                        }
                    } catch (final Exception ex) {
                        LOGGER.debug("Error cancelling AMQP consumer on stop", ex);
                    }

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
}
