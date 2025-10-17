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

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.SubscriptionListener;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;

public final class AmqpConsumer extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpConsumer.class);
    private final Executor executor;
    private final CryptoBybitCollector cryptoBybitCollector;
    private final MetricsBybitCollector metricsBybitCollector;
    private final MetricsCmcCollector metricsCmcCollector;
    private volatile Environment environment;
    private volatile Consumer metricsCmcConsumer;
    private volatile Consumer metricsBybitConsumer;
    private volatile Consumer streamBybitConsumer;

    private enum StreamType {CMC, BYBIT, BYBIT_STREAM}

    public static AmqpConsumer create(final NioReactor reactor, final Executor executor,
                                      final CryptoBybitCollector cryptoBybitCollector,
                                      final MetricsBybitCollector metricsBybitCollector,
                                      final MetricsCmcCollector metricsCmcCollector) {
        return new AmqpConsumer(reactor, executor, cryptoBybitCollector, metricsBybitCollector, metricsCmcCollector);
    }

    private AmqpConsumer(final NioReactor reactor, final Executor executor,
                         final CryptoBybitCollector cryptoBybitCollector,
                         final MetricsBybitCollector metricsBybitCollector,
                         final MetricsCmcCollector metricsCmcCollector) {
        super(reactor);
        this.executor = executor;
        this.cryptoBybitCollector = cryptoBybitCollector;
        this.metricsBybitCollector = metricsBybitCollector;
        this.metricsCmcCollector = metricsCmcCollector;
    }

    @Override
    public Promise<?> start() {
        return Promise.ofBlocking(executor, () -> {
            try {
                final var cmcStream = AmqpConfig.getAmqpMetricsCmcStream();
                environment = AmqpConfig.getEnvironment();
                metricsCmcConsumer = environment.consumerBuilder()
                        .stream(cmcStream)
                        .noTrackingStrategy()
                        .subscriptionListener(context ->
                                updateOffset(cmcStream, context))
                        .messageHandler((context, message) ->
                                consumePayload(StreamType.CMC, context, message))
                        .build();
                metricsBybitConsumer = environment.consumerBuilder()
                        .name(AmqpConfig.getAmqpMetricsBybitStream())
                        .stream(AmqpConfig.getAmqpMetricsBybitStream())
                        .offset(OffsetSpecification.first())
                        .manualTrackingStrategy()
                        .builder()
                        .messageHandler((context, message) ->
                                consumePayload(StreamType.BYBIT, context, message))
                        .build();
                streamBybitConsumer = environment.consumerBuilder()
                        .name(AmqpConfig.getAmqpCryptoBybitStream())
                        .stream(AmqpConfig.getAmqpCryptoBybitStream())
                        .offset(OffsetSpecification.first())
                        .manualTrackingStrategy()
                        .builder()
                        .messageHandler((context, message) ->
                                consumePayload(StreamType.BYBIT_STREAM, context, message))
                        .build();
                LOGGER.info("AmqpConsumer started");
            } catch (final Exception ex) {
                LOGGER.error("Failed to start AmqpConsumer", ex);
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public Promise<?> stop() {
        return Promise.ofBlocking(executor, () -> {
            closeConsumer(metricsCmcConsumer);
            metricsCmcConsumer = null;
            closeConsumer(metricsBybitConsumer);
            metricsBybitConsumer = null;
            closeConsumer(streamBybitConsumer);
            streamBybitConsumer = null;
            closeEnvironment();
            LOGGER.info("AmqpConsumer stopped");
        });
    }

    private void updateOffset(final String stream, final SubscriptionListener.SubscriptionContext context) {
        reactor.execute(() ->
                Promise.ofBlocking(executor, () -> metricsCmcCollector.getStreamOffset(stream))
                        .then(saved -> {
                            if (saved.isPresent()) {
                                context.offsetSpecification(OffsetSpecification.offset(saved.getAsLong() + 1));
                                LOGGER.info("CMC consumer starting from DB offset {}+1 for stream {}", saved.getAsLong(),
                                        stream);
                            } else {
                                context.offsetSpecification(OffsetSpecification.first());
                                LOGGER.info("CMC consumer starting from first for stream {}", stream);
                            }

                            return Promise.complete();
                        })
                        .whenComplete((_, ex) -> {
                            if (ex != null) {
                                LOGGER.warn("Failed to load CMC offset from DB, starting from first", ex);
                                context.offsetSpecification(OffsetSpecification.first());
                            }
                        })
        );
//        try {
//            final var saved = metricsCmcCollector.getStreamOffset(stream);
//            if (saved.isPresent()) {
//                context.offsetSpecification(OffsetSpecification.offset(saved.getAsLong() + 1));
//                LOGGER.info("CMC consumer starting from DB offset {}+1 for stream {}", saved.getAsLong(), stream);
//            } else {
//                context.offsetSpecification(OffsetSpecification.first());
//                LOGGER.info("CMC consumer starting from first for stream {}", stream);
//            }
//        } catch (final Exception ex) {
//            LOGGER.warn("Failed to load CMC offset from DB, starting from first", ex);
//            context.offsetSpecification(OffsetSpecification.first());
//        }
    }

    @SuppressWarnings("unchecked")
    private void consumePayload(final StreamType type, final MessageHandler.Context context, final Message message) {
        reactor.execute(() ->
                Promise.ofBlocking(executor, () -> JsonUtils.bytes2Object(message.getBodyAsBinary(), Payload.class))
                        .then(payload -> switch (type) {
                            case CMC -> metricsCmcCollector.save(payload, context.offset());
                            case BYBIT -> metricsBybitCollector.save(payload);
                            case BYBIT_STREAM -> cryptoBybitCollector.save(payload);
                        })
                        .whenComplete((_, ex) -> {
                            if (ex == null) {
                                if (type != StreamType.CMC) {
                                    context.storeOffset();
                                }
                            } else {
                                LOGGER.error("Failed to process stream message from {}", type, ex);
                            }
                        })
        );
    }

    private void closeConsumer(final Consumer consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (final Exception ex) {
            LOGGER.warn("Error closing stream consumer", ex);
        }
    }

    private void closeEnvironment() {
        try {
            if (environment != null) {
                environment.close();
                environment = null;
            }
        } catch (final Exception ex) {
            LOGGER.warn("Error closing stream environment", ex);
        }
    }
}
