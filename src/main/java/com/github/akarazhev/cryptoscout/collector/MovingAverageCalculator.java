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

import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread-safe stateful calculator for SMA and EMA.
 * Maintains circular buffers for price history and last EMA values.
 */
final class MovingAverageCalculator {
    private static final int MAX_PERIOD = 200;

    // Circular buffer for close prices (oldest at head, newest at tail)
    private final Deque<PricePoint> priceBuffer = new ArrayDeque<>();
    private final int maxSize;

    // Last EMA values (initialized to null)
    private Double lastEma50;
    private Double lastEma100;
    private Double lastEma200;

    // EMA multipliers: 2 / (period + 1)
    private static final double EMA50_MULTIPLIER = 2.0 / 51.0;
    private static final double EMA100_MULTIPLIER = 2.0 / 101.0;
    private static final double EMA200_MULTIPLIER = 2.0 / 201.0;

    static final class PricePoint {
        final OffsetDateTime timestamp;
        final double close;

        PricePoint(final OffsetDateTime timestamp, final double close) {
            this.timestamp = timestamp;
            this.close = close;
        }
    }

    static final class MovingAverages {
        final double sma50;
        final double sma100;
        final double sma200;
        final double ema50;
        final double ema100;
        final double ema200;

        MovingAverages(final double sma50, final double sma100, final double sma200,
                       final double ema50, final double ema100, final double ema200) {
            this.sma50 = sma50;
            this.sma100 = sma100;
            this.sma200 = sma200;
            this.ema50 = ema50;
            this.ema100 = ema100;
            this.ema200 = ema200;
        }

        /**
         * Convert to map with underscore keys for repository.
         */
        Map<String, Object> toMap(final String symbol, final OffsetDateTime timestamp,
                                  final double closePrice) {
            final var map = new HashMap<String, Object>();
            map.put("symbol", symbol);
            map.put("timestamp", timestamp);
            map.put("close_price", closePrice);
            map.put("sma_50", normalize(sma50));
            map.put("sma_100", normalize(sma100));
            map.put("sma_200", normalize(sma200));
            map.put("ema_50", normalize(ema50));
            map.put("ema_100", normalize(ema100));
            map.put("ema_200", normalize(ema200));
            return map;
        }

        private Double normalize(final double value) {
            return Double.isNaN(value) ? null : value;
        }
    }

    MovingAverageCalculator(final int maxPeriod) {
        this.maxSize = Math.max(maxPeriod, MAX_PERIOD);
    }

    /**
     * Initialize with historical data from database (newest first in list).
     * The data will be sorted to chronological order internally.
     */
    synchronized void initialize(final List<Map<String, Object>> historicalData) {
        if (historicalData == null || historicalData.isEmpty()) {
            return;
        }

        // Sort by timestamp ascending (oldest first)
        final var sorted = historicalData.stream()
            .sorted(Comparator.comparing(m -> (OffsetDateTime) m.get("timestamp")))
            .toList();

        // Load into buffer
        for (final var row : sorted) {
            final var timestamp = (OffsetDateTime) row.get("timestamp");
            final var close = ((Number) row.get("close_price")).doubleValue();
            priceBuffer.addLast(new PricePoint(timestamp, close));
            if (priceBuffer.size() > maxSize) {
                priceBuffer.removeFirst();
            }
        }

        // Initialize EMAs from last calculated values if available
        final var last = sorted.get(sorted.size() - 1);
        lastEma50 = (Double) last.get("ema_50");
        lastEma100 = (Double) last.get("ema_100");
        lastEma200 = (Double) last.get("ema_200");
    }

    /**
     * Add new price point and compute moving averages.
     */
    synchronized MovingAverages addPrice(final OffsetDateTime timestamp, final double close) {
        // Add to buffer
        priceBuffer.addLast(new PricePoint(timestamp, close));
        if (priceBuffer.size() > maxSize) {
            priceBuffer.removeFirst();
        }

        // Compute SMAs
        final var sma50 = computeSma(50);
        final var sma100 = computeSma(100);
        final var sma200 = computeSma(200);

        // Compute EMAs
        final var ema50 = computeEma(close, 50, lastEma50, EMA50_MULTIPLIER);
        final var ema100 = computeEma(close, 100, lastEma100, EMA100_MULTIPLIER);
        final var ema200 = computeEma(close, 200, lastEma200, EMA200_MULTIPLIER);

        // Update last EMA values
        lastEma50 = ema50;
        lastEma100 = ema100;
        lastEma200 = ema200;

        return new MovingAverages(sma50, sma100, sma200, ema50, ema100, ema200);
    }

    private double computeSma(final int period) {
        if (priceBuffer.size() < period) {
            return Double.NaN;
        }

        // Sum last 'period' prices
        final var prices = priceBuffer.stream()
            .skip(Math.max(0, priceBuffer.size() - period))
            .mapToDouble(pp -> pp.close)
            .toArray();

        return Arrays.stream(prices).average().orElse(Double.NaN);
    }

    private double computeEma(final double close, final int period,
                              final Double lastEma, final double multiplier) {
        if (lastEma == null || Double.isNaN(lastEma)) {
            // First EMA = SMA
            return computeSma(period);
        }
        // EMA = (Close * multiplier) + (Last EMA * (1 - multiplier))
        return (close * multiplier) + (lastEma * (1 - multiplier));
    }

    synchronized boolean hasEnoughData(final int period) {
        return priceBuffer.size() >= period;
    }

    synchronized int getDataCount() {
        return priceBuffer.size();
    }
}
