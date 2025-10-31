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

import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CONFIRM;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SNAPSHOT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TYPE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_15;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_240;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_60;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.KLINE_D;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.ORDER_BOOK_200;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.PUBLIC_TRADE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TopicType.TICKERS;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getFirstRow;

public final class BybitCryptoCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(BybitCryptoCollector.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final BybitSpotRepository bybitSpotRepository;
    private final String stream;
    private final int batchSize;
    private final long flushIntervalMs;
    private final Queue<OffsetPayload> buffer = new ConcurrentLinkedQueue<>();

    public static BybitCryptoCollector create(final NioReactor reactor, final Executor executor,
                                              final StreamOffsetsRepository streamOffsetsRepository,
                                              final BybitSpotRepository bybitSpotRepository) {
        return new BybitCryptoCollector(reactor, executor, streamOffsetsRepository, bybitSpotRepository);
    }

    private BybitCryptoCollector(final NioReactor reactor, final Executor executor,
                                 final StreamOffsetsRepository streamOffsetsRepository,
                                 final BybitSpotRepository bybitSpotRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.bybitSpotRepository = bybitSpotRepository;
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.flushIntervalMs = JdbcConfig.getBybitFlushIntervalMs();
        this.stream = AmqpConfig.getAmqpBybitCryptoStream();
    }

    @Override
    public Promise<?> start() {
        reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return flush();
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
            var maxOffset = -1L;
            // Spot data
            final var spotKlines15 = new ArrayList<Map<String, Object>>();
            final var spotKlines60 = new ArrayList<Map<String, Object>>();
            final var spotKlines240 = new ArrayList<Map<String, Object>>();
            final var spotKlines1d = new ArrayList<Map<String, Object>>();
            final var spotTickers = new ArrayList<Map<String, Object>>();
            final var spotPublicTrades = new ArrayList<Map<String, Object>>();
            final var spotOrders200 = new ArrayList<Map<String, Object>>();
            for (final var msg : snapshot) {
                final var payload = msg.payload();
                final var source = payload.getSource();
                if (Source.PMST.equals(source)) {
                    final var data = payload.getData();
                    final var topic = (String) data.get(TOPIC_FIELD);
                    if (topic.contains(KLINE_15)) {
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
                        spotTickers.add(data);
                    } else if (topic.contains(PUBLIC_TRADE)) {
                        spotPublicTrades.add(data);
                    } else if (topic.contains(ORDER_BOOK_200)) {
                        if (isOrderSnapshot(data)) {
                            spotOrders200.add(data);
                        }
                    }
                } else if (Source.PML.equals(source)) {
                    // TODO: implement futures
                }

                if (msg.offset() > maxOffset) {
                    maxOffset = msg.offset();
                }
            }
            // No data to insert but we still may want to advance offset in rare cases
            if (spotKlines15.isEmpty() && spotKlines60.isEmpty() && spotKlines240.isEmpty() && spotKlines1d.isEmpty() &&
                    spotTickers.isEmpty() && spotPublicTrades.isEmpty() && spotOrders200.isEmpty()) {
                streamOffsetsRepository.upsertOffset(stream, maxOffset);
                LOGGER.debug("Upserted Bybit spot stream offset {} (no data batch)", maxOffset);
            } else {
                // Save spot data
                saveKline15m(spotKlines15, maxOffset);
                saveKline60m(spotKlines60, maxOffset);
                saveKline240m(spotKlines240, maxOffset);
                saveKline1d(spotKlines1d, maxOffset);
                saveTicker(spotTickers, maxOffset);
                savePublicTrade(spotPublicTrades, maxOffset);
                saveOrderBook200(spotOrders200, maxOffset);
            }

            return null;
        });
    }

    private boolean isKlineConfirmed(final Map<String, Object> kline) {
        final var row = getFirstRow(DATA, kline);
        return row != null && row.containsKey(CONFIRM) && (Boolean) row.get(CONFIRM);
    }

    private boolean isOrderSnapshot(final Map<String, Object> order) {
        return SNAPSHOT.equals(order.get(TYPE));
    }

    private void saveKline15m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot 15m klines (tx) and updated offset {}",
                        bybitSpotRepository.saveKline15m(klines, maxOffset), maxOffset);
            }
        }
    }

    private void saveKline60m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot 60m klines (tx) and updated offset {}",
                        bybitSpotRepository.saveKline60m(klines, maxOffset), maxOffset);
            }
        }
    }

    private void saveKline240m(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot 240m klines (tx) and updated offset {}",
                        bybitSpotRepository.saveKline240m(klines, maxOffset), maxOffset);
            }
        }
    }

    private void saveKline1d(final List<Map<String, Object>> klines, final long maxOffset) throws SQLException {
        if (!klines.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot 1d klines (tx) and updated offset {}",
                        bybitSpotRepository.saveKline1d(klines, maxOffset), maxOffset);
            }
        }
    }

    private void saveTicker(final List<Map<String, Object>> tickers, final long maxOffset) throws SQLException {
        if (!tickers.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot tickers (tx) and updated offset {}",
                        bybitSpotRepository.saveTicker(tickers, maxOffset), maxOffset);
            }
        }
    }

    private void savePublicTrade(final List<Map<String, Object>> publicTrades, final long maxOffset) throws SQLException {
        if (!publicTrades.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot public trades (tx) and updated offset {}",
                        bybitSpotRepository.savePublicTrade(publicTrades, maxOffset), maxOffset);
            }
        }
    }

    private void saveOrderBook200(final List<Map<String, Object>> orderBooks, final long maxOffset) throws SQLException {
        if (!orderBooks.isEmpty()) {
            if (maxOffset >= 0) {
                LOGGER.info("Inserted {} spot order books 200 (tx) and updated offset {}",
                        bybitSpotRepository.saveOrderBook200(orderBooks, maxOffset), maxOffset);
            }
        }
    }
}
