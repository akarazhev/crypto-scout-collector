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

import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
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
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final BybitCryptoCollector bybitCryptoCollector;
    private final BybitParserCollector bybitParserCollector;
    private final MetricsCmcCollector metricsCmcCollector;
    private volatile Environment environment;
    private volatile Consumer metricsCmcConsumer;
    private volatile Consumer metricsBybitConsumer;
    private volatile Consumer streamBybitConsumer;

    private enum StreamType {CMC, BYBIT, BYBIT_STREAM}

    public static AmqpConsumer create(final NioReactor reactor, final Executor executor,
                                      final StreamOffsetsRepository streamOffsetsRepository,
                                      final BybitCryptoCollector bybitCryptoCollector,
                                      final BybitParserCollector bybitParserCollector,
                                      final MetricsCmcCollector metricsCmcCollector) {
        return new AmqpConsumer(reactor, executor, streamOffsetsRepository, bybitCryptoCollector, bybitParserCollector,
                metricsCmcCollector);
    }

    private AmqpConsumer(final NioReactor reactor, final Executor executor,
                         final StreamOffsetsRepository streamOffsetsRepository,
                         final BybitCryptoCollector bybitCryptoCollector,
                         final BybitParserCollector bybitParserCollector,
                         final MetricsCmcCollector metricsCmcCollector) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.bybitCryptoCollector = bybitCryptoCollector;
        this.bybitParserCollector = bybitParserCollector;
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
                        .subscriptionListener(c -> updateOffset(cmcStream, c))
                        .messageHandler((c, m) -> consumePayload(StreamType.CMC, c, m))
                        .build();
                final var bybitParserStream = AmqpConfig.getAmqpBybitParserStream();
                metricsBybitConsumer = environment.consumerBuilder()
                        .stream(bybitParserStream)
                        .noTrackingStrategy()
                        .subscriptionListener(c -> updateOffset(bybitParserStream, c))
                        .messageHandler((c, m) -> consumePayload(StreamType.BYBIT, c, m))
                        .build();
                final var bybitCryptoStream = AmqpConfig.getAmqpBybitCryptoStream();
                streamBybitConsumer = environment.consumerBuilder()
                        .stream(bybitCryptoStream)
                        .noTrackingStrategy()
                        .subscriptionListener(c -> updateOffset(bybitCryptoStream, c))
                        .messageHandler((c, m) -> consumePayload(StreamType.BYBIT_STREAM, c, m))
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
        reactor.execute(() -> Promise.ofBlocking(executor, () -> streamOffsetsRepository.getOffset(stream))
                .then(saved -> {
                    if (saved.isPresent()) {
                        context.offsetSpecification(OffsetSpecification.offset(saved.getAsLong() + 1));
                        LOGGER.info("Consumer starting from DB offset {}+1 for stream {}", saved.getAsLong(), stream);
                    } else {
                        context.offsetSpecification(OffsetSpecification.first());
                        LOGGER.info("Consumer starting from first for stream {}", stream);
                    }

                    return Promise.complete();
                })
                .whenComplete((_, ex) -> {
                    if (ex != null) {
                        LOGGER.warn("Failed to load offset from DB, starting from first", ex);
                        context.offsetSpecification(OffsetSpecification.first());
                    }
                })
        );
    }

    @SuppressWarnings("unchecked")
    private void consumePayload(final StreamType type, final MessageHandler.Context context, final Message message) {
        reactor.execute(() -> Promise.ofBlocking(executor, () ->
                        JsonUtils.bytes2Object(message.getBodyAsBinary(), Payload.class))
                .then(payload -> switch (type) {
                    case CMC -> metricsCmcCollector.save(payload, context.offset());
                    case BYBIT -> bybitParserCollector.save(payload, context.offset());
                    case BYBIT_STREAM -> bybitCryptoCollector.save(payload, context.offset());
                })
                .whenComplete((_, ex) -> {
                    if (ex != null) {
                        LOGGER.error("Failed to process stream message from {}: {}", type.name(), ex);
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
