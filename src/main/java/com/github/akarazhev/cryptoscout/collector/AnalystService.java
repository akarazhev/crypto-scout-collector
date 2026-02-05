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

import com.github.akarazhev.cryptoscout.collector.db.AnalystRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AnalystService extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AnalystService.class);
    private final Executor executor;
    private final StreamOffsetsRepository streamOffsetsRepository;
    private final AnalystRepository analystRepository;

    // Configuration
    private final int batchSize;
    private final long flushIntervalMs;

    // State
    private final MovingAverageCalculator maCalculator;
    private final Queue<IndicatorResult> resultBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean flushInProgress = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean initialized = false;

    // Target configuration for cmc_kline_1w
    private static final String TARGET_SYMBOL = "BTC";
    private static final Source TARGET_SOURCE = Source.BTC_USD_1W;
    private static final int INITIAL_LOOKBACK = 250;

    static final class IndicatorResult {
        final Map<String, Object> indicator;
        final long offset;

        IndicatorResult(final Map<String, Object> indicator, final long offset) {
            this.indicator = indicator;
            this.offset = offset;
        }
    }

    public static AnalystService create(final NioReactor reactor, final Executor executor,
                                        final StreamOffsetsRepository streamOffsetsRepository,
                                        final AnalystRepository analystRepository) {
        return new AnalystService(reactor, executor, streamOffsetsRepository, analystRepository);
    }

    private AnalystService(final NioReactor reactor, final Executor executor,
                           final StreamOffsetsRepository streamOffsetsRepository,
                           final AnalystRepository analystRepository) {
        super(reactor);
        this.executor = executor;
        this.streamOffsetsRepository = streamOffsetsRepository;
        this.analystRepository = analystRepository;
        this.batchSize = JdbcConfig.getAnalystBatchSize();
        this.flushIntervalMs = JdbcConfig.getAnalystFlushIntervalMs();
        this.maCalculator = new MovingAverageCalculator(250);
    }

    @Override
    public Promise<Void> start() {
        return initializeCalculator()
            .then(() -> {
                running.set(true);
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
                LOGGER.info("AnalystService started, MA calculator has {} data points",
                           maCalculator.getDataCount());
                return Promise.complete();
            });
    }

    @Override
    public Promise<Void> stop() {
        running.set(false);
        LOGGER.info("AnalystService stopping, flushing remaining {} results", resultBuffer.size());
        return flush();
    }

    /**
     * Initialize calculator with data from cmc_kline_1w and existing indicators.
     */
    private Promise<Void> initializeCalculator() {
        return Promise.ofBlocking(executor, () -> {
            try {
                // Step 1: Try to load existing indicators (has EMA values)
                final var existingIndicators = analystRepository.getRecentIndicators(
                    TARGET_SYMBOL, INITIAL_LOOKBACK);

                if (!existingIndicators.isEmpty()) {
                    maCalculator.initialize(existingIndicators);
                    LOGGER.info("Initialized from {} existing indicators", existingIndicators.size());
                } else {
                    LOGGER.info("No existing indicators found, will compute from raw klines");
                }

                // Step 2: Load raw klines from cmc_kline_1w to fill gaps or initialize
                final var currentDataCount = maCalculator.getDataCount();
                final var neededDataPoints = Math.max(0, 200 - currentDataCount);

                if (neededDataPoints > 0) {
                    final var klines = analystRepository.getRecentKlines(
                        TARGET_SYMBOL, INITIAL_LOOKBACK);

                    if (!klines.isEmpty()) {
                        initializeFromKlines(klines, neededDataPoints);
                    }
                }

                initialized = true;
                LOGGER.info("Calculator initialized with {} data points", maCalculator.getDataCount());
            } catch (final SQLException e) {
                throw new IllegalStateException("Failed to initialize calculator", e);
            }
        });
    }

    private void initializeFromKlines(final List<Map<String, Object>> klines,
                                      final int neededPoints) {
        // Sort ascending (oldest first)
        final var sorted = klines.stream()
            .sorted(Comparator.comparing(m -> (OffsetDateTime) m.get("timestamp")))
            .toList();

        // Take only what we need from the end, but process in order
        final var toProcess = sorted.size() <= neededPoints ?
            sorted : sorted.subList(sorted.size() - neededPoints, sorted.size());

        for (final var kline : toProcess) {
            final var timestamp = (OffsetDateTime) kline.get("timestamp");
            final var close = ((Number) kline.get("close")).doubleValue();
            maCalculator.addPrice(timestamp, close);
        }

        LOGGER.info("Loaded {} klines from cmc_kline_1w", toProcess.size());
    }

    /**
     * Process incoming payload - compute MAs if it's BTC 1W kline data.
     */
    public Promise<Void> analyze(final Payload<Map<String, Object>> payload, final long offset) {
        if (!initialized) {
            LOGGER.warn("Calculator not initialized, skipping analysis");
            return Promise.complete();
        }

        // Filter for CMC BTC 1W data only
        if (!isTargetPayload(payload)) {
            return Promise.complete();
        }

        final var data = payload.getData();
        final var timestamp = extractTimestamp(data);
        final var close = extractClosePrice(data);

        if (timestamp == null || Double.isNaN(close)) {
            LOGGER.warn("Invalid kline data: timestamp={}, close={}", timestamp, close);
            return Promise.complete();
        }

        // Compute moving averages
        final var mas = maCalculator.addPrice(timestamp, close);

        // Convert to indicator map
        final var indicator = mas.toMap(TARGET_SYMBOL, timestamp, close);

        // Queue for batch save
        resultBuffer.add(new IndicatorResult(indicator, offset));

        // Trigger flush if buffer is full
        if (resultBuffer.size() >= batchSize) {
            return flush();
        }

        return Promise.complete();
    }

    private boolean isTargetPayload(final Payload<Map<String, Object>> payload) {
        return Provider.CMC.equals(payload.getProvider()) &&
               TARGET_SOURCE.equals(payload.getSource());
    }

    private OffsetDateTime extractTimestamp(final Map<String, Object> data) {
        // Try various timestamp fields from CMC data
        final var ts = data.get("timestamp") != null ? data.get("timestamp") : data.get("time_close");
        if (ts instanceof OffsetDateTime) {
            return (OffsetDateTime) ts;
        }
        if (ts instanceof String) {
            return OffsetDateTime.parse((String) ts);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private double extractClosePrice(final Map<String, Object> data) {
        // CMC data structure: data -> quote -> close
        final var quotes = (Map<String, Object>) data.get("quote");
        if (quotes != null) {
            final var quote = (Map<String, Object>) quotes.get("USD");
            if (quote != null) {
                final var closeObj = quote.get("close");
                if (closeObj instanceof Number) {
                    return ((Number) closeObj).doubleValue();
                }
            }
        }
        // Fallback: try direct close field
        final var closeObj = data.get("close");
        if (closeObj instanceof Number) {
            return ((Number) closeObj).doubleValue();
        }
        return Double.NaN;
    }

    private void scheduledFlush() {
        if (!running.get() || flushInProgress.getAndSet(true)) {
            return;
        }
        flush().whenComplete((v, ex) -> {
            flushInProgress.set(false);
            if (running.get()) {
                reactor.delayBackground(flushIntervalMs, this::scheduledFlush);
            }
        });
    }

    private Promise<Void> flush() {
        if (resultBuffer.isEmpty()) {
            return Promise.complete();
        }

        final var items = new ArrayList<IndicatorResult>();
        while (true) {
            final var item = resultBuffer.poll();
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
            final var indicators = new ArrayList<Map<String, Object>>();

            for (final var item : items) {
                indicators.add(item.indicator);
                if (item.offset > maxOffset) {
                    maxOffset = item.offset;
                }
            }

            if (!indicators.isEmpty() && maxOffset >= 0) {
                try {
                    final var count = analystRepository.saveIndicators(indicators, maxOffset);
                    LOGGER.info("Saved {} indicators to cmc_kline_1w_indicators (offset: {})",
                               count, maxOffset);
                } catch (final SQLException e) {
                    throw new IllegalStateException("Failed to save indicators", e);
                }
            }
        });
    }
}
