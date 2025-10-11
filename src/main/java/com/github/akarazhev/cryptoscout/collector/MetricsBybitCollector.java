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

import com.github.akarazhev.cryptoscout.collector.db.MetricsBybitRepository;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
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

public final class MetricsBybitCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(MetricsBybitCollector.class);
    private final Executor executor;
    private final MetricsBybitRepository metricsBybitRepository;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<Payload<Map<String, Object>>> buffer = new ConcurrentLinkedQueue<>();

    public static MetricsBybitCollector create(final NioReactor reactor, final Executor executor,
                                               final MetricsBybitRepository metricsBybitRepository) {
        return new MetricsBybitCollector(reactor, executor, metricsBybitRepository);
    }

    private MetricsBybitCollector(final NioReactor reactor, final Executor executor,
                                  final MetricsBybitRepository metricsBybitRepository) {
        super(reactor);
        this.executor = executor;
        this.metricsBybitRepository = metricsBybitRepository;
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.flushIntervalMs = JdbcConfig.getBybitFlushIntervalMs();
    }

    @Override
    public Promise<?> start() {
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        LOGGER.info("MetricsBybitCollector started");
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        final var promise = flush();
        LOGGER.info("MetricsBybitCollector stopped");
        return promise;
    }

    public Promise<?> save(final Payload<Map<String, Object>> payload) {
        if (!Provider.BYBIT.equals(payload.getProvider())) {
            LOGGER.warn("Invalid payload: {}", payload);
            return Promise.complete();
        }

        buffer.add(payload);
        if (buffer.size() >= batchSize) {
            return flush();
        }

        return Promise.complete();
    }

    private void scheduledFlush() {
        flush().whenComplete(($, e) ->
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush));
    }

    private Promise<?> flush() {
        if (buffer.isEmpty()) {
            return Promise.complete();
        }

        final var snapshot = new ArrayList<Payload<Map<String, Object>>>();
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
            final var lpl = new ArrayList<Map<String, Object>>();
            for (final var payload : snapshot) {
                final var source = payload.getSource();
                if (Source.LPL.equals(source)) {
                    lpl.add(payload.getData());
                }
            }

            if (!lpl.isEmpty()) {
                LOGGER.info("Inserted {} LPL points", metricsBybitRepository.insertLpl(lpl));
            }

            return null;
        });
    }
}
