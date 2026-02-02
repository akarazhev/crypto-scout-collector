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

import com.github.akarazhev.cryptoscout.collector.db.CryptoScoutRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import com.github.akarazhev.jcryptolib.stream.OffsetPayload;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CryptoScoutService extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(CryptoScoutService.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final CryptoScoutRepository cryptoScoutRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload<Map<String, Object>>> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public static CryptoScoutService create(final NioReactor reactor, final Executor executor,
                                            final StreamOffsetsRepository streamOffsetsRepository,
                                            final CryptoScoutRepository cryptoScoutRepository) {
        return new CryptoScoutService(reactor, executor, streamOffsetsRepository, cryptoScoutRepository);
    }

    private CryptoScoutService(final NioReactor reactor, final Executor executor,
                               final StreamOffsetsRepository streamOffsetsRepository,
                               final CryptoScoutRepository cryptoScoutRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.cryptoScoutRepository = cryptoScoutRepository;
        this.batchSize = JdbcConfig.getCryptoScoutBatchSize();
        this.flushIntervalMs = JdbcConfig.getCryptoScoutFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpCryptoScoutStream();
    }

    @Override
    public Promise<Void> start() {
        running.set(true);
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        running.set(false);
        return flush();
    }

    public Promise<Void> save(final Payload<Map<String, Object>> payload, final long offset) {
        if (!Provider.BYBIT.equals(payload.getProvider()) && !Provider.CMC.equals(payload.getProvider())) {
            LOGGER.warn("Invalid payload: {}", payload);
            return Promise.complete();
        }

        buffer.add(OffsetPayload.of(payload, offset));
        if (buffer.size() >= batchSize) {
            return flush();
        }

        return Promise.complete();
    }

    public Promise<List<Map<String, Object>>> getFgi(final OffsetDateTime from, final OffsetDateTime to) {
        return Promise.ofBlocking(executor, () -> cryptoScoutRepository.getFgi(from, to));
    }

    public Promise<List<Map<String, Object>>> getKline1d(final String symbol, final OffsetDateTime from,
                                                         final OffsetDateTime to) {
        return Promise.ofBlocking(executor, () -> cryptoScoutRepository.getKline1d(symbol, from, to));
    }

    public Promise<List<Map<String, Object>>> getKline1w(final String symbol, final OffsetDateTime from,
                                                         final OffsetDateTime to) {
        return Promise.ofBlocking(executor, () -> cryptoScoutRepository.getKline1w(symbol, from, to));
    }

    private void scheduledFlush() {
        if (!running.get() || flushInProgress.getAndSet(true)) {
            return;
        }
        flush().whenComplete((_, _) -> {
            flushInProgress.set(false);
            if (running.get()) {
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
            }
        });
    }

    private Promise<Void> flush() {
        if (buffer.isEmpty()) {
            return Promise.complete();
        }

        final var items = new ArrayList<OffsetPayload<Map<String, Object>>>();
        while (true) {
            final var item = buffer.poll();
            if (item == null) {
                break;
            }

            items.add(item);
        }

        if (items.isEmpty()) {
            return Promise.complete();
        }

        return Promise.ofBlocking(executor, () -> {
            var maxOffset = -1L;
            final var fgis = new ArrayList<Map<String, Object>>();
            final var klines1d = new ArrayList<Map<String, Object>>();
            final var klines1w = new ArrayList<Map<String, Object>>();
            for (final var item : items) {
                final var payload = item.payload();
                final var source = payload.getSource();
                final var data = payload.getData();
                if (Source.FGI.equals(source)) {
                    fgis.add(data);
                } else if (Source.BTC_USD_1D.equals(source)) {
                    klines1d.add(data);
                } else if (Source.BTC_USD_1W.equals(source)) {
                    klines1w.add(data);
                }

                if (item.offset() > maxOffset) {
                    maxOffset = item.offset();
                }
            }
            // No data to insert but we still may want to advance offset in rare cases
            if (fgis.isEmpty() && klines1d.isEmpty() && klines1w.isEmpty()) {
                if (maxOffset >= 0) {
                    streamOffsetsRepository.upsertOffset(stream, maxOffset);
                    LOGGER.warn("Upserted CMC stream offset {} (no data batch)", maxOffset);
                }
            } else {
                fgis.trimToSize();
                saveFgi(fgis, maxOffset);
                klines1d.trimToSize();
                saveKline1d(klines1d, maxOffset);
                klines1w.trimToSize();
                saveKline1w(klines1w, maxOffset);
            }
        });
    }

    private void saveFgi(final List<Map<String, Object>> fgis, final long maxOffset) throws SQLException {
        if (!fgis.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = cryptoScoutRepository.saveFgi(fgis, maxOffset);
                LOGGER.info("Save {} FGI points (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveKline1d(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = cryptoScoutRepository.saveKline1d(klines, maxOffset);
                LOGGER.info("Save {} CMC 1d klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveKline1w(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = cryptoScoutRepository.saveKline1w(klines, maxOffset);
                LOGGER.info("Save {} CMC 1w klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }
}
