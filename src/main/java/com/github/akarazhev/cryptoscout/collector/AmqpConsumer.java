/*
 * MIT License
 *
 * Copyright (c) 2026 Andrey Karazhev
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
import com.rabbitmq.client.ShutdownListener;
import io.activej.async.service.ReactiveService;
import io.activej.datastream.supplier.AbstractStreamSupplier;
import io.activej.datastream.supplier.StreamSupplier;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.PREFETCH_COUNT;
import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.RECONNECT_DELAY_MS;
import static com.github.akarazhev.cryptoscout.collector.Constants.Amqp.MAX_RECONNECT_ATTEMPTS;

public final class AmqpConsumer extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final InternalStreamSupplier streamSupplier = new InternalStreamSupplier();
    private final Executor executor;
    private final ConnectionFactory connectionFactory;
    private final String clientName;
    private final String queue;
    private volatile Connection connection;
    private volatile Channel channel;
    private volatile String consumerTag;

    public static AmqpConsumer create(final NioReactor reactor, final Executor executor,
                                      final ConnectionFactory connectionFactory, final String clientName,
                                      final String queue) {
        return new AmqpConsumer(reactor, executor, connectionFactory, clientName, queue);
    }

    private AmqpConsumer(final NioReactor reactor, final Executor executor,
                         final ConnectionFactory connectionFactory, final String clientName,
                         final String queue) {
        super(reactor);
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.clientName = clientName;
        this.queue = queue;
    }

    public StreamSupplier<byte[]> getStreamSupplier() {
        return streamSupplier;
    }

    @Override
    public Promise<Void> start() {
        running.set(true);
        return Promise.ofBlocking(executor, this::connect);
    }

    private void connect() {
        try {
            connection = connectionFactory.newConnection(clientName);
            channel = connection.createChannel();
            channel.basicQos(PREFETCH_COUNT);
            channel.queueDeclarePassive(queue);

            final ShutdownListener shutdownListener = cause -> {
                if (running.get() && !cause.isInitiatedByApplication()) {
                    LOGGER.warn("AMQP connection lost for queue: {}. Attempting reconnection...", queue);
                    reactor.execute(this::scheduleReconnect);
                }
            };

            connection.addShutdownListener(shutdownListener);
            final DeliverCallback deliver = (_, delivery) -> {
                try {
                    final var body = delivery.getBody();
                    reactor.execute(() -> streamSupplier.push(body));
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
    }

    private void scheduleReconnect() {
        if (!running.get()) {
            return;
        }

        reactor.delayBackground(RECONNECT_DELAY_MS, () -> {
            if (!running.get()) {
                return;
            }

            Promise.ofBlocking(executor, () -> {
                for (var attempt = 1; attempt <= MAX_RECONNECT_ATTEMPTS; attempt++) {
                    try {
                        LOGGER.info("Reconnection attempt {} for queue: {}", attempt, queue);
                        connect();
                        LOGGER.info("Successfully reconnected to queue: {}", queue);
                        return;
                    } catch (final Exception ex) {
                        LOGGER.warn("Reconnection attempt {} failed for queue: {}", attempt, queue, ex);
                        if (attempt < MAX_RECONNECT_ATTEMPTS) {
                            Thread.sleep(RECONNECT_DELAY_MS);
                        }
                    }
                }

                LOGGER.error("Failed to reconnect after {} attempts for queue: {}", MAX_RECONNECT_ATTEMPTS, queue);
            });
        });
    }

    @Override
    public Promise<Void> stop() {
        running.set(false);
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
        }).then(streamSupplier::complete);
    }

    private static final class InternalStreamSupplier extends AbstractStreamSupplier<byte[]> {
        private final SettablePromise<Void> completion = new SettablePromise<>();

        private void push(final byte[] data) {
            if (!isEndOfStream()) {
                send(data);
            }
        }

        private Promise<Void> complete() {
            if (!isEndOfStream()) {
                sendEndOfStream();
            }

            return completion;
        }

        @Override
        protected void onAcknowledge() {
            completion.set(null);
        }

        @Override
        protected void onError(final Exception e) {
            completion.setException(e);
        }
    }
}
