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

import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.jcryptolib.stream.Command;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.concurrent.Executor;

public final class DataCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataCollector.class);
    private final Executor executor;
    private final BybitCryptoCollector bybitCryptoCollector;
    private final BybitTaCryptoCollector bybitTaCryptoCollector;
    private final BybitParserCollector bybitParserCollector;
    private final CmcParserCollector cmcParserCollector;
    private volatile Connection connection;
    private volatile Channel channel;
    private volatile String consumerTag;

    public static DataCollector create(final NioReactor reactor, final Executor executor,
                                       final BybitCryptoCollector bybitCryptoCollector,
                                       final BybitTaCryptoCollector bybitTaCryptoCollector,
                                       final BybitParserCollector bybitParserCollector,
                                       final CmcParserCollector cmcParserCollector) {
        return new DataCollector(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector, bybitParserCollector,
                cmcParserCollector);
    }

    private DataCollector(final NioReactor reactor, final Executor executor,
                          final BybitCryptoCollector bybitCryptoCollector,
                          final BybitTaCryptoCollector bybitTaCryptoCollector,
                          final BybitParserCollector bybitParserCollector,
                          final CmcParserCollector cmcParserCollector) {
        super(reactor);
        this.executor = executor;
        this.bybitCryptoCollector = bybitCryptoCollector;
        this.bybitTaCryptoCollector = bybitTaCryptoCollector;
        this.bybitParserCollector = bybitParserCollector;
        this.cmcParserCollector = cmcParserCollector;
    }

    @Override
    public Promise<Void> start() {
        return Promise.ofBlocking(executor, () -> {
            try {
                final var queue = AmqpConfig.getAmqpCollectorQueue();
                connection = AmqpConfig.getConnection();
                channel = connection.createChannel();
                channel.basicQos(1);
                channel.queueDeclarePassive(queue);

                final DeliverCallback deliver = (_, delivery) -> {
                    try {
                        final var body = delivery.getBody();
                        reactor.execute(() -> Promise.ofBlocking(executor, () -> {
                            @SuppressWarnings("unchecked") final var command =
                                    (Command<OffsetDateTime[]>) JsonUtils.bytes2Object(body, Command.class);
                            process(command);
                        }));
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } catch (final Exception e) {
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        } catch (final IOException ex) {
                            LOGGER.debug("Error nacking AMQP message", ex);
                        }

                        LOGGER.error("Failed to parse/process command message", e);
                    }
                };

                final CancelCallback cancel =
                        tag -> LOGGER.debug("AMQP data collector consumer cancelled: {}", tag);
                consumerTag = channel.basicConsume(queue, false, deliver, cancel);
                LOGGER.info("DataCollector started consuming queue: {}", queue);
            } catch (final Exception ex) {
                LOGGER.error("Failed to start DataCollector", ex);
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
                        LOGGER.debug("Error cancelling AMQP data collector consumer on stop", ex);
                    }

                    channel.close();
                    channel = null;
                }
            } catch (final Exception ex) {
                LOGGER.warn("Error closing AMQP data collector channel", ex);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                } catch (final Exception ex) {
                    LOGGER.warn("Error closing AMQP data collector connection", ex);
                }
            }
        });
    }

    private Promise<Void> process(final Command<OffsetDateTime[]> command) {
        LOGGER.info("Received command: id={}, from={}, size={}", command.id(), command.from(), command.size());
        return Promise.complete();
    }
}
