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

import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.collector.PayloadParser.isKlineConfirmed;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.isSnapshot;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ALL_LIQUIDATION;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_1;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_15;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_240;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_5;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_60;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_D;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_1;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_1000;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_200;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_50;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.PUBLIC_TRADE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.TICKERS;

public final class BybitStreamService extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(BybitStreamService.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final BybitSpotRepository bybitSpotRepository;
    private final BybitLinearRepository bybitLinearRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload<Map<String, Object>>> buffer = new ConcurrentLinkedQueue<>();

    public enum Type {BYBIT_SPOT, BYBIT_LINEAR}

    public static BybitStreamService create(final NioReactor reactor, final Executor executor,
                                            final StreamOffsetsRepository streamOffsetsRepository,
                                            final BybitSpotRepository bybitSpotRepository,
                                            final BybitLinearRepository bybitLinearRepository) {
        return new BybitStreamService(reactor, executor, streamOffsetsRepository, bybitSpotRepository,
                bybitLinearRepository);
    }

    private BybitStreamService(final NioReactor reactor, final Executor executor,
                               final StreamOffsetsRepository streamOffsetsRepository,
                               final BybitSpotRepository bybitSpotRepository,
                               final BybitLinearRepository bybitLinearRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.bybitSpotRepository = bybitSpotRepository;
        this.bybitLinearRepository = bybitLinearRepository;
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.flushIntervalMs = JdbcConfig.getBybitFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpBybitStream();
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

        buffer.add(OffsetPayload.of(payload, offset));
        if (buffer.size() >= batchSize) {
            return flush();
        }

        return Promise.complete();
    }

    public Promise<List<Map<String, Object>>> getKline1m(final Type type, final String symbol,
                                                         final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline1m(symbol, from, to));
            case BYBIT_LINEAR -> Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline1m(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getKline5m(final Type type, final String symbol,
                                                         final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline5m(symbol, from, to));
            case BYBIT_LINEAR -> Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline5m(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getKline15m(final Type type, final String symbol,
                                                          final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline15m(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline15m(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getKline60m(final Type type, final String symbol,
                                                          final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline60m(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline60m(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getKline240m(final Type type, final String symbol,
                                                           final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline240m(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline240m(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getKline1d(final Type type, final String symbol,
                                                         final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getKline1d(symbol, from, to));
            case BYBIT_LINEAR -> Promise.ofBlocking(executor, () -> bybitLinearRepository.getKline1d(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getTicker(final Type type, final String symbol, final OffsetDateTime from,
                                                        final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT -> Promise.ofBlocking(executor, () -> bybitSpotRepository.getTicker(symbol, from, to));
            case BYBIT_LINEAR -> Promise.ofBlocking(executor, () -> bybitLinearRepository.getTicker(symbol, from, to));
        };
    }

    private void scheduledFlush() {
        flush().whenComplete((_, _) ->
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush));
    }

    private Promise<Void> flush() {
        if (buffer.isEmpty()) {
            return Promise.complete();
        }

        final var items = new LinkedList<OffsetPayload<Map<String, Object>>>();
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
            final var spotKlines1 = new ArrayList<Map<String, Object>>();
            final var spotKlines5 = new ArrayList<Map<String, Object>>();
            final var spotKlines15 = new ArrayList<Map<String, Object>>();
            final var spotKlines60 = new ArrayList<Map<String, Object>>();
            final var spotKlines240 = new ArrayList<Map<String, Object>>();
            final var spotKlines1d = new ArrayList<Map<String, Object>>();
            final var spotTickers = new ArrayList<Map<String, Object>>();
            final var spotPublicTrades = new ArrayList<Map<String, Object>>();
            final var spotOrders1 = new ArrayList<Map<String, Object>>();
            final var spotOrders50 = new ArrayList<Map<String, Object>>();
            final var spotOrders200 = new ArrayList<Map<String, Object>>();
            final var spotOrders1000 = new ArrayList<Map<String, Object>>();
            // Linear data
            final var linearKlines1 = new ArrayList<Map<String, Object>>();
            final var linearKlines5 = new ArrayList<Map<String, Object>>();
            final var linearKlines15 = new ArrayList<Map<String, Object>>();
            final var linearKlines60 = new ArrayList<Map<String, Object>>();
            final var linearKlines240 = new ArrayList<Map<String, Object>>();
            final var linearKlines1d = new ArrayList<Map<String, Object>>();
            final var linearTickers = new ArrayList<Map<String, Object>>();
            final var linearPublicTrades = new ArrayList<Map<String, Object>>();
            final var linearOrders1 = new ArrayList<Map<String, Object>>();
            final var linearOrders50 = new ArrayList<Map<String, Object>>();
            final var linearOrders200 = new ArrayList<Map<String, Object>>();
            final var linearOrders1000 = new ArrayList<Map<String, Object>>();
            final var linearAllLiquidation = new ArrayList<Map<String, Object>>();
            for (final var item : items) {
                final var payload = item.payload();
                final var source = payload.getSource();
                final var data = payload.getData();
                final var topic = (String) data.get(TOPIC_FIELD);
                if (Source.PMST.equals(source)) {
                    if (topic.contains(KLINE_1)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines1.add(data);
                        }
                    } else if (topic.contains(KLINE_5)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines5.add(data);
                        }
                    } else if (topic.contains(KLINE_15)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines15.add(data);
                        }
                    } else if (topic.contains(KLINE_60)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines60.add(data);
                        }
                    } else if (topic.contains(KLINE_240)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines240.add(data);
                        }
                    } else if (topic.contains(KLINE_D)) {
                        if (isKlineConfirmed(data)) {
                            spotKlines1d.add(data);
                        }
                    } else if (topic.contains(TICKERS)) {
                        if (isSnapshot(data)) {
                            spotTickers.add(data);
                        }
                    } else if (topic.contains(PUBLIC_TRADE)) {
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
                    if (topic.contains(KLINE_1)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines1.add(data);
                        }
                    } else if (topic.contains(KLINE_5)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines5.add(data);
                        }
                    } else if (topic.contains(KLINE_15)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines15.add(data);
                        }
                    } else if (topic.contains(KLINE_60)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines60.add(data);
                        }
                    } else if (topic.contains(KLINE_240)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines240.add(data);
                        }
                    } else if (topic.contains(KLINE_D)) {
                        if (isKlineConfirmed(data)) {
                            linearKlines1d.add(data);
                        }
                    } else if (topic.contains(TICKERS)) {
                        if (isSnapshot(data)) {
                            linearTickers.add(data);
                        }
                    } else if (topic.contains(PUBLIC_TRADE)) {
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
                            linearAllLiquidation.add(data);
                        }
                    }
                }

                if (item.offset() > maxOffset) {
                    maxOffset = item.offset();
                }
            }
            // No data to insert but we still may want to advance offset in rare cases
            if (spotKlines1.isEmpty() && spotKlines5.isEmpty() && spotKlines15.isEmpty() && spotKlines60.isEmpty() &&
                    spotKlines240.isEmpty() && spotKlines1d.isEmpty() && spotTickers.isEmpty() &&
                    spotPublicTrades.isEmpty() && spotOrders1.isEmpty() && spotOrders50.isEmpty() &&
                    spotOrders200.isEmpty() && spotOrders1000.isEmpty() &&
                    linearKlines1.isEmpty() && linearKlines5.isEmpty() && linearKlines15.isEmpty() && linearKlines60.isEmpty() &&
                    linearKlines240.isEmpty() && linearKlines1d.isEmpty() && linearTickers.isEmpty() &&
                    linearPublicTrades.isEmpty() && linearOrders1.isEmpty() && linearOrders50.isEmpty() &&
                    linearOrders200.isEmpty() && linearOrders1000.isEmpty() && linearAllLiquidation.isEmpty()) {
                streamOffsetsRepository.upsertOffset(stream, maxOffset);
                LOGGER.warn("Upserted Bybit stream offset {} (no data batch)", maxOffset);
            } else {
                // Save spot data
                spotKlines1.trimToSize();
                saveSpotKline1m(spotKlines1, maxOffset);
                spotKlines5.trimToSize();
                saveSpotKline5m(spotKlines5, maxOffset);
                spotKlines15.trimToSize();
                saveSpotKline15m(spotKlines15, maxOffset);
                spotKlines60.trimToSize();
                saveSpotKline60m(spotKlines60, maxOffset);
                spotKlines240.trimToSize();
                saveSpotKline240m(spotKlines240, maxOffset);
                spotKlines1d.trimToSize();
                saveSpotKline1d(spotKlines1d, maxOffset);
                spotTickers.trimToSize();
                saveSpotTicker(spotTickers, maxOffset);
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
                linearKlines1.trimToSize();
                saveLinearKline1m(linearKlines1, maxOffset);
                linearKlines5.trimToSize();
                saveLinearKline5m(linearKlines5, maxOffset);
                linearKlines15.trimToSize();
                saveLinearKline15m(linearKlines15, maxOffset);
                linearKlines60.trimToSize();
                saveLinearKline60m(linearKlines60, maxOffset);
                linearKlines240.trimToSize();
                saveLinearKline240m(linearKlines240, maxOffset);
                linearKlines1d.trimToSize();
                saveLinearKline1d(linearKlines1d, maxOffset);
                linearTickers.trimToSize();
                saveLinearTicker(linearTickers, maxOffset);
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
                linearAllLiquidation.trimToSize();
                saveLinearAllLiquidation(linearAllLiquidation, maxOffset);
            }
        });
    }

    private void saveSpotKline1m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline1m(klines, maxOffset);
                LOGGER.info("Save {} spot 1m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotKline5m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline5m(klines, maxOffset);
                LOGGER.info("Save {} spot 5m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotKline15m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline15m(klines, maxOffset);
                LOGGER.info("Save {} spot 15m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotKline60m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline60m(klines, maxOffset);
                LOGGER.info("Save {} spot 60m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotKline240m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline240m(klines, maxOffset);
                LOGGER.info("Save {} spot 240m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotKline1d(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveKline1d(klines, maxOffset);
                LOGGER.info("Save {} spot 1d klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotTicker(final List<Map<String, Object>> tickers, final long maxOffset) throws SQLException {
        if (!tickers.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveTicker(tickers, maxOffset);
                LOGGER.info("Save {} spot tickers (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline1m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline1m(klines, maxOffset);
                LOGGER.info("Save {} linear 1m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline5m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline5m(klines, maxOffset);
                LOGGER.info("Save {} linear 5m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline15m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline15m(klines, maxOffset);
                LOGGER.info("Save {} linear 15m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline60m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline60m(klines, maxOffset);
                LOGGER.info("Save {} linear 60m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline240m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline240m(klines, maxOffset);
                LOGGER.info("Save {} linear 240m klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearKline1d(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveKline1d(klines, maxOffset);
                LOGGER.info("Save {} linear 1d klines (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearTicker(final List<Map<String, Object>> tickers, final long maxOffset) throws SQLException {
        if (!tickers.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveTicker(tickers, maxOffset);
                LOGGER.info("Save {} linear tickers (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    public Promise<List<Map<String, Object>>> getOrderBook1(final Type type, final String symbol,
                                                            final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT ->
                    Promise.ofBlocking(executor, () -> bybitSpotRepository.getOrderBook1(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getOrderBook1(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getOrderBook50(final Type type, final String symbol,
                                                             final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT ->
                    Promise.ofBlocking(executor, () -> bybitSpotRepository.getOrderBook50(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getOrderBook50(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getOrderBook200(final Type type, final String symbol,
                                                              final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT ->
                    Promise.ofBlocking(executor, () -> bybitSpotRepository.getOrderBook200(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getOrderBook200(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getOrderBook1000(final Type type, final String symbol,
                                                               final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT ->
                    Promise.ofBlocking(executor, () -> bybitSpotRepository.getOrderBook1000(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getOrderBook1000(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getPublicTrade(final Type type, final String symbol,
                                                             final OffsetDateTime from, final OffsetDateTime to) {
        return switch (type) {
            case BYBIT_SPOT ->
                    Promise.ofBlocking(executor, () -> bybitSpotRepository.getPublicTrade(symbol, from, to));
            case BYBIT_LINEAR ->
                    Promise.ofBlocking(executor, () -> bybitLinearRepository.getPublicTrade(symbol, from, to));
        };
    }

    public Promise<List<Map<String, Object>>> getAllLiquidation(final String symbol, final OffsetDateTime from,
                                                                final OffsetDateTime to) {
        return Promise.ofBlocking(executor, () -> bybitLinearRepository.getAllLiquidation(symbol, from, to));
    }

    private void saveSpotPublicTrade(final List<Map<String, Object>> publicTrades, final long maxOffset)
            throws SQLException {
        if (!publicTrades.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.savePublicTrade(publicTrades, maxOffset);
                LOGGER.info("Save {} spot public trades (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook1(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveOrderBook1(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 1 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook50(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveOrderBook50(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 50 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook200(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveOrderBook200(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 200 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveSpotOrderBook1000(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitSpotRepository.saveOrderBook1000(orderBooks, maxOffset);
                LOGGER.info("Save {} spot order books 1000 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearPublicTrade(final List<Map<String, Object>> publicTrades, final long maxOffset)
            throws SQLException {
        if (!publicTrades.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.savePublicTrade(publicTrades, maxOffset);
                LOGGER.info("Save {} linear public trades (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook1(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveOrderBook1(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 1 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook50(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveOrderBook50(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 50 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook200(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveOrderBook200(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 200 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearOrderBook1000(final List<Map<String, Object>> orderBooks, final long maxOffset)
            throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveOrderBook1000(orderBooks, maxOffset);
                LOGGER.info("Save {} linear order books 1000 (tx) and updated offset {}", count, maxOffset);
            }
        }
    }

    private void saveLinearAllLiquidation(final List<Map<String, Object>> allLiquidations, final long maxOffset)
            throws SQLException {
        if (!allLiquidations.isEmpty()) {
            if (maxOffset >= 0) {
                final var count = bybitLinearRepository.saveAllLiquidation(allLiquidations, maxOffset);
                LOGGER.info("Save {} linear all liquidations (tx) and updated offset {}", count, maxOffset);
            }
        }
    }
}
