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

import com.github.akarazhev.cryptoscout.collector.db.MetricsCmcRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public final class MetricsCmcCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(MetricsCmcCollector.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final MetricsCmcRepository metricsCmcRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload> buffer = new ConcurrentLinkedQueue<>();

    public static MetricsCmcCollector create(final NioReactor reactor, final Executor executor,
                                             final StreamOffsetsRepository streamOffsetsRepository,
                                             final MetricsCmcRepository metricsCmcRepository) {
        return new MetricsCmcCollector(reactor, executor, streamOffsetsRepository, metricsCmcRepository);
    }

    private MetricsCmcCollector(final NioReactor reactor, final Executor executor,
                                final StreamOffsetsRepository streamOffsetsRepository,
                                final MetricsCmcRepository metricsCmcRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.metricsCmcRepository = metricsCmcRepository;
        this.batchSize = JdbcConfig.getCmcBatchSize();
        this.flushIntervalMs = JdbcConfig.getCmcFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpMetricsCmcStream();
    }

    @Override
    public Promise<?> start() {
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        LOGGER.info("MetricsCmcCollector started");
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        final var promise = flush();
        LOGGER.info("MetricsCmcCollector stopped");
        return promise;
    }

    public Promise<?> save(final Payload<Map<String, Object>> payload, final long offset) {
        if (!Provider.CMC.equals(payload.getProvider())) {
            LOGGER.warn("Invalid payload: {}", payload);
            return Promise.complete();
        }

        buffer.add(OffsetPayload.of(offset, payload));
        if (buffer.size() >= batchSize) {
            return flush();
        }

        return Promise.complete();
    }

    private void scheduledFlush() {
        flush().whenComplete((_, _) ->
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush));
    }

    private Promise<?> flush() {
        if (buffer.isEmpty()) {
            return Promise.complete();
        }

        final var snapshot = new ArrayList<OffsetPayload>();
        while (true) {
            final var item = buffer.poll();
            if (item == null) {
                break;
            }

            snapshot.add(item);
        }

        if (snapshot.isEmpty()) {
            return Promise.complete();
        }

        return Promise.ofBlocking(executor, () -> {
            final var fgis = new ArrayList<Map<String, Object>>();
            long maxOffset = -1L;
            for (final var msg : snapshot) {
                final var payload = msg.payload();
                final var source = payload.getSource();
                if (Source.FGI.equals(source)) {
                    fgis.add(payload.getData());
                }

                if (msg.offset() > maxOffset) {
                    maxOffset = msg.offset();
                }
            }

            if (!fgis.isEmpty()) {
                if (maxOffset >= 0) {
                    LOGGER.info("Inserted {} FGI points (tx) and updated offset {}",
                            metricsCmcRepository.insertFgi(fgis, maxOffset), maxOffset);
                }
            } else if (maxOffset >= 0) {
                // No data to insert but we still may want to advance offset in rare cases
                streamOffsetsRepository.upsertOffset(stream, maxOffset);
                LOGGER.debug("Upserted CMC stream offset {} (no data batch)", maxOffset);
            }

            return null;
        });
    }
}
