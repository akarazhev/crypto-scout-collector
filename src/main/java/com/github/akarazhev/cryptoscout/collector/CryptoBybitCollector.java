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

import com.github.akarazhev.cryptoscout.collector.db.CryptoBybitRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TOPIC;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Topic.TICKERS_BTC_USDT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Topic.TICKERS_ETH_USDT;

public final class CryptoBybitCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(CryptoBybitCollector.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final CryptoBybitRepository cryptoBybitRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload> buffer = new ConcurrentLinkedQueue<>();

    public static CryptoBybitCollector create(final NioReactor reactor, final Executor executor,
                                              final StreamOffsetsRepository streamOffsetsRepository,
                                              final CryptoBybitRepository cryptoBybitRepository) {
        return new CryptoBybitCollector(reactor, executor, streamOffsetsRepository, cryptoBybitRepository);
    }

    private CryptoBybitCollector(final NioReactor reactor, final Executor executor,
                                 final StreamOffsetsRepository streamOffsetsRepository,
                                 final CryptoBybitRepository cryptoBybitRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.cryptoBybitRepository = cryptoBybitRepository;
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.flushIntervalMs = JdbcConfig.getBybitFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpCryptoBybitStream();
    }

    @Override
    public Promise<?> start() {
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        LOGGER.info("CryptoBybitCollector started");
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        final var promise = flush();
        LOGGER.info("CryptoBybitCollector stopped");
        return promise;
    }

    public Promise<?> save(final Payload<Map<String, Object>> payload, final long offset) {
        if (!Provider.BYBIT.equals(payload.getProvider())) {
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
            final var spts = new ArrayList<Map<String, Object>>();
            long maxOffset = -1L;
            for (final var msg : snapshot) {
                final var payload = msg.payload();
                final var source = payload.getSource();
                if (Source.PMST.equals(source)) {
                    final var data = payload.getData();
                    final var topic = (String) data.get(TOPIC);
                    if (Objects.equals(topic, TICKERS_BTC_USDT) || Objects.equals(topic, TICKERS_ETH_USDT)) {
                        spts.add(data);
                    }
                } else if (Source.PML.equals(source)) {
                    // TODO: implement futures
                }

                if (msg.offset() > maxOffset) {
                    maxOffset = msg.offset();
                }
            }

            if (!spts.isEmpty()) {
                if (maxOffset >= 0) {
                    LOGGER.info("Inserted {} spot tickers (tx) and updated offset {}",
                            cryptoBybitRepository.insertSpotTickers(spts, maxOffset), maxOffset);
                }
            } else if (maxOffset >= 0) {
                // No data to insert but we still may want to advance offset in rare cases
                streamOffsetsRepository.upsertOffset(stream, maxOffset);
                LOGGER.debug("Upserted Bybit spot stream offset {} (no data batch)", maxOffset);
            }

            return null;
        });
    }
}
