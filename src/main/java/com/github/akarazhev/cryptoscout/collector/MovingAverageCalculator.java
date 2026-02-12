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

import org.ta4j.core.BarSeries;
import org.ta4j.core.BaseBar;
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.indicators.EMAIndicator;
import org.ta4j.core.indicators.SMAIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread-safe stateful calculator for SMA and EMA using ta4j library.
 * Maintains a BarSeries for price history and computes indicators using ta4j.
 */
final class MovingAverageCalculator {
    private static final int MAX_PERIOD = 200;
    private static final Duration DEFAULT_DURATION = Duration.ofDays(7);

    private final BarSeries barSeries;
    private final ClosePriceIndicator closePriceIndicator;
    private final SMAIndicator sma50Indicator;
    private final SMAIndicator sma100Indicator;
    private final SMAIndicator sma200Indicator;
    private final EMAIndicator ema50Indicator;
    private final EMAIndicator ema100Indicator;
    private final EMAIndicator ema200Indicator;

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

        Map<String, Object> toMap(final String symbol, final OffsetDateTime timestamp, final double closePrice) {
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
        final var maxSize = Math.max(maxPeriod, MAX_PERIOD);
        this.barSeries = new BaseBarSeriesBuilder()
            .withMaxBarCount(maxSize)
            .withName("MA_Calculator")
            .build();
        this.closePriceIndicator = new ClosePriceIndicator(barSeries);
        this.sma50Indicator = new SMAIndicator(closePriceIndicator, 50);
        this.sma100Indicator = new SMAIndicator(closePriceIndicator, 100);
        this.sma200Indicator = new SMAIndicator(closePriceIndicator, 200);
        this.ema50Indicator = new EMAIndicator(closePriceIndicator, 50);
        this.ema100Indicator = new EMAIndicator(closePriceIndicator, 100);
        this.ema200Indicator = new EMAIndicator(closePriceIndicator, 200);
    }

    synchronized void initialize(final List<Map<String, Object>> historicalData) {
        if (historicalData == null || historicalData.isEmpty()) {
            return;
        }

        // Sort by timestamp ascending (oldest first)
        final var sorted = historicalData.stream()
            .sorted(Comparator.comparing(m -> (OffsetDateTime) m.get("timestamp")))
            .toList();

        // Load into BarSeries
        for (final var row : sorted) {
            final var timestamp = (OffsetDateTime) row.get("timestamp");
            final var close = ((Number) row.get("close_price")).doubleValue();
            addBar(timestamp, close);
        }
    }

    synchronized MovingAverages addPrice(final OffsetDateTime timestamp, final double close) {
        addBar(timestamp, close);

        final var lastIndex = barSeries.getEndIndex();

        // Compute SMAs (return NaN if insufficient data)
        final var sma50 = getIndicatorValue(sma50Indicator, lastIndex, 50);
        final var sma100 = getIndicatorValue(sma100Indicator, lastIndex, 100);
        final var sma200 = getIndicatorValue(sma200Indicator, lastIndex, 200);

        // Compute EMAs (return NaN if insufficient data)
        final var ema50 = getIndicatorValue(ema50Indicator, lastIndex, 50);
        final var ema100 = getIndicatorValue(ema100Indicator, lastIndex, 100);
        final var ema200 = getIndicatorValue(ema200Indicator, lastIndex, 200);

        return new MovingAverages(sma50, sma100, sma200, ema50, ema100, ema200);
    }

    synchronized int getDataCount() {
        return barSeries.getBarCount();
    }

    private void addBar(final OffsetDateTime timestamp, final double close) {
        final var instant = timestamp.toInstant();
        final var bar = BaseBar.builder()
            .timePeriod(DEFAULT_DURATION)
            .endTime(instant)
            .openPrice(close)
            .highPrice(close)
            .lowPrice(close)
            .closePrice(close)
            .volume(0)
            .build();
        barSeries.addBar(bar);
    }

    private double getIndicatorValue(final org.ta4j.core.Indicator<org.ta4j.core.num.Num> indicator,
                                     final int index,
                                     final int period) {
        if (barSeries.getBarCount() < period) {
            return Double.NaN;
        }
        return indicator.getValue(index).doubleValue();
    }
}
