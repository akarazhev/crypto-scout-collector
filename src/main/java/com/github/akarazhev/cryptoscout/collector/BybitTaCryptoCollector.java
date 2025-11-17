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

import com.github.akarazhev.cryptoscout.collector.db.BybitTaLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaSpotRepository;
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.collector.PayloadParser.isSnapshot;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ALL_LIQUIDATION;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_1;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_1000;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_200;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_50;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.PUBLIC_TRADE;

public final class BybitTaCryptoCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(BybitTaCryptoCollector.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final BybitTaSpotRepository bybitTaSpotRepository;
    private final BybitTaLinearRepository bybitTaLinearRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload> buffer = new ConcurrentLinkedQueue<>();

    public static BybitTaCryptoCollector create(final NioReactor reactor, final Executor executor,
                                                final StreamOffsetsRepository streamOffsetsRepository,
                                                final BybitTaSpotRepository bybitTaSpotRepository,
                                                final BybitTaLinearRepository bybitTaLinearRepository) {
        return new BybitTaCryptoCollector(reactor, executor, streamOffsetsRepository, bybitTaSpotRepository,
                bybitTaLinearRepository);
    }

    private BybitTaCryptoCollector(final NioReactor reactor, final Executor executor,
                                   final StreamOffsetsRepository streamOffsetsRepository,
                                   final BybitTaSpotRepository bybitTaSpotRepository,
                                   final BybitTaLinearRepository bybitTaLinearRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.bybitTaSpotRepository = bybitTaSpotRepository;
        this.bybitTaLinearRepository = bybitTaLinearRepository;
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.flushIntervalMs = JdbcConfig.getBybitFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpBybitCryptoStream();
    }

    @Override
    public Promise<Void> start() {
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return flush();
    }

    public Promise<Void> save(final Payload<Map<String, Object>> payload, final long offset) {
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

    private Promise<Void> flush() {
        if (buffer.isEmpty()) {
            return Promise.complete();
        }

        final var items = new LinkedList<OffsetPayload>();
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
            // Spot data
            final var spotPublicTrades = new ArrayList<Map<String, Object>>();
            final var spotOrders1 = new ArrayList<Map<String, Object>>();
            final var spotOrders50 = new ArrayList<Map<String, Object>>();
            final var spotOrders200 = new ArrayList<Map<String, Object>>();
            final var spotOrders1000 = new ArrayList<Map<String, Object>>();
            // Linear data
            final var linearPublicTrades = new ArrayList<Map<String, Object>>();
            final var linearOrders1 = new ArrayList<Map<String, Object>>();
            final var linearOrders50 = new ArrayList<Map<String, Object>>();
            final var linearOrders200 = new ArrayList<Map<String, Object>>();
            final var linearOrders1000 = new ArrayList<Map<String, Object>>();
            final var linearAllLiqudation = new ArrayList<Map<String, Object>>();
            for (final var item : items) {
                final var payload = item.payload();
                final var source = payload.getSource();
                final var data = payload.getData();
                final var topic = (String) data.get(TOPIC_FIELD);
                if (Source.PMST.equals(source)) {
                    if (topic.contains(PUBLIC_TRADE)) {
                        if (isSnapshot(data)) {
                            spotPublicTrades.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_1)) {
                        if (isSnapshot(data)) {
                            spotOrders1.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_50)) {
                        if (isSnapshot(data)) {
                            spotOrders50.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_200)) {
                        if (isSnapshot(data)) {
                            spotOrders200.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_1000)) {
                        if (isSnapshot(data)) {
                            spotOrders1000.add(data);
                        }
                    }
                } else if (Source.PML.equals(source)) {
                    if (topic.contains(PUBLIC_TRADE)) {
                        if (isSnapshot(data)) {
                            linearPublicTrades.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_1)) {
                        if (isSnapshot(data)) {
                            linearOrders1.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_50)) {
                        if (isSnapshot(data)) {
                            linearOrders50.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_200)) {
                        if (isSnapshot(data)) {
                            linearOrders200.add(data);
                        }
                    } else if (topic.contains(ORDER_BOOK_1000)) {
                        if (isSnapshot(data)) {
                            linearOrders1000.add(data);
                        }
                    } else if (topic.contains(ALL_LIQUIDATION)) {
                        if (isSnapshot(data)) {
                            linearAllLiqudation.add(data);
                        }
                    }
                }

                if (item.offset() > maxOffset) {
                    maxOffset = item.offset();
                }
            }
            // No data to insert but we still may want to advance offset in rare cases
            if (spotPublicTrades.isEmpty() && spotOrders1.isEmpty() && spotOrders50.isEmpty() &&
                    spotOrders200.isEmpty() && spotOrders1000.isEmpty() &&
                    linearPublicTrades.isEmpty() && linearOrders1.isEmpty() && linearOrders50.isEmpty() &&
                    linearOrders200.isEmpty() && linearOrders1000.isEmpty() && linearAllLiqudation.isEmpty()) {
                streamOffsetsRepository.upsertOffset(stream, maxOffset);
                LOGGER.warn("Upserted Bybit stream offset {} (no data batch)", maxOffset);
            } else {
                // Save spot data
                spotPublicTrades.trimToSize();
                saveSpotPublicTrade(spotPublicTrades, maxOffset);
                spotOrders1.trimToSize();
                saveSpotOrderBook1(spotOrders1, maxOffset);
                spotOrders50.trimToSize();
                saveSpotOrderBook50(spotOrders50, maxOffset);
                spotOrders200.trimToSize();
                saveSpotOrderBook200(spotOrders200, maxOffset);
                spotOrders1000.trimToSize();
                saveSpotOrderBook1000(spotOrders1000, maxOffset);
                // Save linear data
                linearPublicTrades.trimToSize();
                saveLinearPublicTrade(linearPublicTrades, maxOffset);
                linearOrders1.trimToSize();
                saveLinearOrderBook1(linearOrders1, maxOffset);
                linearOrders50.trimToSize();
                saveLinearOrderBook50(linearOrders50, maxOffset);
                linearOrders200.trimToSize();
                saveLinearOrderBook200(linearOrders200, maxOffset);
                linearOrders1000.trimToSize();
                saveLinearOrderBook1000(linearOrders1000, maxOffset);
                linearAllLiqudation.trimToSize();
                saveLinearAllLiquidation(linearAllLiqudation, maxOffset);
            }
        });
    }

    private void saveSpotPublicTrade(final List<Map<String, Object>> publicTrades, final long maxOffset)
            throws SQLException {
        if (!publicTrades.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaSpotRepository.savePublicTrade(publicTrades, maxOffset);
                LOGGER.info("Save {} spot public trades (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook1(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaSpotRepository.saveOrderBook1(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 1 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook50(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaSpotRepository.saveOrderBook50(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 50 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook200(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaSpotRepository.saveOrderBook200(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 200 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook1000(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaSpotRepository.saveOrderBook1000(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 1000 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearPublicTrade(final List<Map<String, Object>> publicTrades, final long maxOffset)
            throws SQLException {
        if (!publicTrades.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.savePublicTrade(publicTrades, maxOffset);
                LOGGER.info("Save {} linear public trades (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook1(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.saveOrderBook1(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 1 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook50(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.saveOrderBook50(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 50 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook200(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.saveOrderBook200(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 200 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook1000(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.saveOrderBook1000(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 1000 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearAllLiquidation(final List<Map<String, Object>> allLiqudations, final long maxOffset)
            throws SQLException {
        if (!allLiqudations.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitTaLinearRepository.saveAllLiquidation(allLiqudations, maxOffset);
                LOGGER.info("Save {} linear all liquidations (tx) and updated offset {}", count, maxOffset);
            }
        }
    }
}
