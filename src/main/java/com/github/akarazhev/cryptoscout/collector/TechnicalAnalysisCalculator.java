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
import org.ta4j.core.BaseBarSeriesBuilder;
import org.ta4j.core.indicators.ATRIndicator;
import org.ta4j.core.indicators.MACDIndicator;
import org.ta4j.core.indicators.RSIIndicator;
import org.ta4j.core.indicators.StochasticOscillatorKIndicator;
import org.ta4j.core.indicators.volume.VWAPIndicator;
import org.ta4j.core.indicators.averages.EMAIndicator;
import org.ta4j.core.indicators.averages.SMAIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsLowerIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsMiddleIndicator;
import org.ta4j.core.indicators.bollinger.BollingerBandsUpperIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.indicators.helpers.VolumeIndicator;
import org.ta4j.core.indicators.statistics.StandardDeviationIndicator;
import org.ta4j.core.num.DecimalNumFactory;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thread-safe stateful calculator for technical indicators using ta4j library.
 * Supports OHLCV data, multiple technical indicators, and market fundamentals.
 * Maintains backward compatibility with the original MovingAverageCalculator API.
 */
final class TechnicalAnalysisCalculator {
    private static final int DEFAULT_MAX_PERIOD = 200;
    private static final Duration DEFAULT_DURATION = Duration.ofDays(7);
    private static final int RSI_PERIOD = 14;
    private static final int MACD_FAST = 12;
    private static final int MACD_SLOW = 26;
    private static final int MACD_SIGNAL = 9;
    private static final int BB_PERIOD = 20;
    private static final int ATR_PERIOD = 14;
    private static final int STOCHASTIC_PERIOD = 14;
    private static final int VOLUME_SMA_PERIOD = 20;

    private final Config config;
    private final BarSeries barSeries;

    // Core indicators (always present)
    private final ClosePriceIndicator closePriceIndicator;
    private final SMAIndicator sma50Indicator;
    private final SMAIndicator sma100Indicator;
    private final SMAIndicator sma200Indicator;
    private final EMAIndicator ema50Indicator;
    private final EMAIndicator ema100Indicator;
    private final EMAIndicator ema200Indicator;

    // Momentum indicators (optional)
    private final RSIIndicator rsiIndicator;
    private final StochasticOscillatorKIndicator stochasticIndicator;

    // Trend indicators (optional)
    private final MACDIndicator macdIndicator;
    private final SMAIndicator macdSignalIndicator;
    private final BollingerBandsMiddleIndicator bbMiddleIndicator;
    private final BollingerBandsUpperIndicator bbUpperIndicator;
    private final BollingerBandsLowerIndicator bbLowerIndicator;

    // Volatility indicators (optional)
    private final ATRIndicator atrIndicator;
    private final StandardDeviationIndicator stdDevIndicator;

    // Volume indicators (optional)
    private final VolumeIndicator volumeIndicator;
    private final VWAPIndicator vwapIndicator;
    private final SMAIndicator volumeSmaIndicator;

    // Market fundamentals (tracked but not calculated)
    private double lastMarketCap;
    private long lastCirculatingSupply;

    /**
     * Configuration for enabling/disabling specific indicators.
     * Use the builder pattern for easy configuration.
     */
    static final class Config {
        final int maxPeriod;
        final boolean enableSma;
        final boolean enableEma;
        final boolean enableRsi;
        final boolean enableStochastic;
        final boolean enableMacd;
        final boolean enableBollinger;
        final boolean enableAtr;
        final boolean enableStdDev;
        final boolean enableVwap;
        final boolean enableVolumeSma;
        final boolean includeMarketFundamentals;

        private Config(final Builder builder) {
            this.maxPeriod = builder.maxPeriod;
            this.enableSma = builder.enableSma;
            this.enableEma = builder.enableEma;
            this.enableRsi = builder.enableRsi;
            this.enableStochastic = builder.enableStochastic;
            this.enableMacd = builder.enableMacd;
            this.enableBollinger = builder.enableBollinger;
            this.enableAtr = builder.enableAtr;
            this.enableStdDev = builder.enableStdDev;
            this.enableVwap = builder.enableVwap;
            this.enableVolumeSma = builder.enableVolumeSma;
            this.includeMarketFundamentals = builder.includeMarketFundamentals;
        }

        static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for Config with sensible defaults.
         */
        static final class Builder {
            private int maxPeriod = DEFAULT_MAX_PERIOD;
            private boolean enableSma = true;
            private boolean enableEma = true;
            private boolean enableRsi = false;
            private boolean enableStochastic = false;
            private boolean enableMacd = false;
            private boolean enableBollinger = false;
            private boolean enableAtr = false;
            private boolean enableStdDev = false;
            private boolean enableVwap = false;
            private boolean enableVolumeSma = false;
            private boolean includeMarketFundamentals = false;

            Builder maxPeriod(final int maxPeriod) {
                this.maxPeriod = maxPeriod;
                return this;
            }

            Builder enableSma(final boolean enable) {
                this.enableSma = enable;
                return this;
            }

            Builder enableEma(final boolean enable) {
                this.enableEma = enable;
                return this;
            }

            Builder enableRsi(final boolean enable) {
                this.enableRsi = enable;
                return this;
            }

            Builder enableStochastic(final boolean enable) {
                this.enableStochastic = enable;
                return this;
            }

            Builder enableMacd(final boolean enable) {
                this.enableMacd = enable;
                return this;
            }

            Builder enableBollinger(final boolean enable) {
                this.enableBollinger = enable;
                return this;
            }

            Builder enableAtr(final boolean enable) {
                this.enableAtr = enable;
                return this;
            }

            Builder enableStdDev(final boolean enable) {
                this.enableStdDev = enable;
                return this;
            }

            Builder enableVwap(final boolean enable) {
                this.enableVwap = enable;
                return this;
            }

            Builder enableVolumeSma(final boolean enable) {
                this.enableVolumeSma = enable;
                return this;
            }

            Builder includeMarketFundamentals(final boolean include) {
                this.includeMarketFundamentals = include;
                return this;
            }

            Config build() {
                return new Config(this);
            }
        }
    }

    /**
     * Represents a single OHLCV data point.
     */
    static final class OhlcvPoint {
        final OffsetDateTime timestamp;
        final double open;
        final double high;
        final double low;
        final double close;
        final double volume;
        final double marketCap;
        final long circulatingSupply;

        OhlcvPoint(final OffsetDateTime timestamp, final double open, final double high,
                   final double low, final double close, final double volume) {
            this(timestamp, open, high, low, close, volume, 0.0, 0L);
        }

        OhlcvPoint(final OffsetDateTime timestamp, final double open, final double high,
                   final double low, final double close, final double volume,
                   final double marketCap, final long circulatingSupply) {
            validateOhlcv(open, high, low, close, volume);
            this.timestamp = timestamp;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.marketCap = marketCap;
            this.circulatingSupply = circulatingSupply;
        }

        private void validateOhlcv(final double open, final double high, final double low,
                                   final double close, final double volume) {
            if (high < low) {
                throw new IllegalArgumentException(
                    String.format("High (%.2f) cannot be less than low (%.2f)", high, low));
            }
            if (high < Math.max(open, close)) {
                throw new IllegalArgumentException("High must be >= max(open, close)");
            }
            if (low > Math.min(open, close)) {
                throw new IllegalArgumentException("Low must be <= min(open, close)");
            }
            if (volume < 0) {
                throw new IllegalArgumentException("Volume cannot be negative");
            }
        }
    }

    /**
     * Legacy result class for backward compatibility.
     * Contains only SMA and EMA values.
     */
    static final class MovingAverages {
        final Double sma50;
        final Double sma100;
        final Double sma200;
        final Double ema50;
        final Double ema100;
        final Double ema200;

        MovingAverages(final Double sma50, final Double sma100, final Double sma200,
                       final Double ema50, final Double ema100, final Double ema200) {
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
            map.put("sma_50", sma50);
            map.put("sma_100", sma100);
            map.put("sma_200", sma200);
            map.put("ema_50", ema50);
            map.put("ema_100", ema100);
            map.put("ema_200", ema200);
            return map;
        }
    }

    /**
     * Extended analysis result with all available indicators.
     */
    static final class AnalysisResult {
        // Moving averages
        final Double sma50;
        final Double sma100;
        final Double sma200;
        final Double ema50;
        final Double ema100;
        final Double ema200;

        // Momentum
        final Double rsi14;
        final Double stochastic14;

        // Trend (MACD)
        final Double macdLine;
        final Double macdSignal;
        final Double macdHistogram;

        // Bollinger Bands
        final Double bbMiddle;
        final Double bbUpper;
        final Double bbLower;
        final Double bbWidth;
        final Double bbPercentB;

        // Volatility
        final Double atr14;
        final Double stdDev20;

        // Volume
        final Double vwap;
        final Double volumeSma20;

        // Market fundamentals
        final Double marketCap;
        final Long circulatingSupply;
        final Double marketCapToVolume;

        AnalysisResult(final Double sma50, final Double sma100, final Double sma200,
                       final Double ema50, final Double ema100, final Double ema200,
                       final Double rsi14, final Double stochastic14,
                       final Double macdLine, final Double macdSignal, final Double macdHistogram,
                       final Double bbMiddle, final Double bbUpper, final Double bbLower,
                       final Double atr14, final Double stdDev20,
                       final Double vwap, final Double volumeSma20,
                       final Double marketCap, final Long circulatingSupply) {
            this.sma50 = sma50;
            this.sma100 = sma100;
            this.sma200 = sma200;
            this.ema50 = ema50;
            this.ema100 = ema100;
            this.ema200 = ema200;
            this.rsi14 = rsi14;
            this.stochastic14 = stochastic14;
            this.macdLine = macdLine;
            this.macdSignal = macdSignal;
            this.macdHistogram = macdHistogram;
            this.bbMiddle = bbMiddle;
            this.bbUpper = bbUpper;
            this.bbLower = bbLower;
            this.bbWidth = calculateBbWidth(bbUpper, bbLower, bbMiddle);
            this.bbPercentB = calculateBbPercentB(bbUpper, bbLower, ema50);
            this.atr14 = atr14;
            this.stdDev20 = stdDev20;
            this.vwap = vwap;
            this.volumeSma20 = volumeSma20;
            this.marketCap = marketCap;
            this.circulatingSupply = circulatingSupply;
            this.marketCapToVolume = calculateMarketCapToVolume(marketCap, volumeSma20);
        }

        private static Double calculateBbWidth(final Double upper, final Double lower, final Double middle) {
            if (upper == null || lower == null || middle == null || middle == 0.0) {
                return null;
            }
            return (upper - lower) / middle;
        }

        private static Double calculateBbPercentB(final Double upper, final Double lower,
                                                   final Double price) {
            if (upper == null || lower == null || upper.equals(lower)) {
                return null;
            }
            return (price - lower) / (upper - lower);
        }

        private static Double calculateMarketCapToVolume(final Double marketCap, final Double volume) {
            if (marketCap == null || volume == null || volume == 0.0) {
                return null;
            }
            return marketCap / volume;
        }

        Map<String, Object> toMap(final String symbol, final OffsetDateTime timestamp, final OhlcvPoint point) {
            final var map = new HashMap<String, Object>();
            map.put("symbol", symbol);
            map.put("timestamp", timestamp);
            map.put("open_price", point.open);
            map.put("high_price", point.high);
            map.put("low_price", point.low);
            map.put("close_price", point.close);
            map.put("volume", point.volume);

            // Moving averages
            map.put("sma_50", sma50);
            map.put("sma_100", sma100);
            map.put("sma_200", sma200);
            map.put("ema_50", ema50);
            map.put("ema_100", ema100);
            map.put("ema_200", ema200);

            // Momentum
            map.put("rsi_14", rsi14);
            map.put("stochastic_14", stochastic14);

            // MACD
            map.put("macd_line", macdLine);
            map.put("macd_signal", macdSignal);
            map.put("macd_histogram", macdHistogram);

            // Bollinger Bands
            map.put("bb_middle", bbMiddle);
            map.put("bb_upper", bbUpper);
            map.put("bb_lower", bbLower);
            map.put("bb_width", bbWidth);
            map.put("bb_percent_b", bbPercentB);

            // Volatility
            map.put("atr_14", atr14);
            map.put("std_dev_20", stdDev20);

            // Volume
            map.put("vwap", vwap);
            map.put("volume_sma_20", volumeSma20);

            // Market fundamentals
            map.put("market_cap", marketCap);
            map.put("circulating_supply", circulatingSupply);
            map.put("market_cap_to_volume", marketCapToVolume);

            return map;
        }
    }

    // Legacy constructor for backward compatibility
    TechnicalAnalysisCalculator(final int maxPeriod) {
        this(Config.builder().maxPeriod(maxPeriod).build());
    }

    // New constructor with configuration
    TechnicalAnalysisCalculator(final Config config) {
        this.config = config != null ? config : Config.builder().build();
        final var maxSize = Math.max(this.config.maxPeriod, DEFAULT_MAX_PERIOD);

        this.barSeries = new BaseBarSeriesBuilder()
            .withMaxBarCount(maxSize)
            .withName("MA_Calculator")
            .withNumFactory(DecimalNumFactory.getInstance())
            .build();

        // Core indicators (always initialized)
        this.closePriceIndicator = new ClosePriceIndicator(barSeries);
        this.sma50Indicator = this.config.enableSma ? new SMAIndicator(closePriceIndicator, 50) : null;
        this.sma100Indicator = this.config.enableSma ? new SMAIndicator(closePriceIndicator, 100) : null;
        this.sma200Indicator = this.config.enableSma ? new SMAIndicator(closePriceIndicator, 200) : null;
        this.ema50Indicator = this.config.enableEma ? new EMAIndicator(closePriceIndicator, 50) : null;
        this.ema100Indicator = this.config.enableEma ? new EMAIndicator(closePriceIndicator, 100) : null;
        this.ema200Indicator = this.config.enableEma ? new EMAIndicator(closePriceIndicator, 200) : null;

        // Momentum indicators
        this.rsiIndicator = this.config.enableRsi ? new RSIIndicator(closePriceIndicator, RSI_PERIOD) : null;
        this.stochasticIndicator = this.config.enableStochastic
            ? new StochasticOscillatorKIndicator(barSeries, STOCHASTIC_PERIOD)
            : null;

        // Trend indicators
        this.macdIndicator = this.config.enableMacd
            ? new MACDIndicator(closePriceIndicator, MACD_FAST, MACD_SLOW)
            : null;
        this.macdSignalIndicator = this.config.enableMacd
            ? new SMAIndicator(macdIndicator, MACD_SIGNAL)
            : null;
        // Bollinger Bands - need stdDev for upper/lower bands
        final StandardDeviationIndicator bbStdDev = this.config.enableBollinger
            ? new StandardDeviationIndicator(closePriceIndicator, BB_PERIOD)
            : null;
        this.bbMiddleIndicator = this.config.enableBollinger
            ? new BollingerBandsMiddleIndicator(closePriceIndicator)
            : null;
        this.bbUpperIndicator = this.config.enableBollinger
            ? new BollingerBandsUpperIndicator(bbMiddleIndicator, bbStdDev)
            : null;
        this.bbLowerIndicator = this.config.enableBollinger
            ? new BollingerBandsLowerIndicator(bbMiddleIndicator, bbStdDev)
            : null;

        // Volatility indicators
        this.atrIndicator = this.config.enableAtr ? new ATRIndicator(barSeries, ATR_PERIOD) : null;
        this.stdDevIndicator = this.config.enableStdDev
            ? new StandardDeviationIndicator(closePriceIndicator, BB_PERIOD)
            : null;

        // Volume indicators
        this.volumeIndicator = (this.config.enableVwap || this.config.enableVolumeSma)
            ? new VolumeIndicator(barSeries)
            : null;
        this.vwapIndicator = this.config.enableVwap ? new VWAPIndicator(barSeries, BB_PERIOD) : null;
        this.volumeSmaIndicator = this.config.enableVolumeSma
            ? new SMAIndicator(volumeIndicator, VOLUME_SMA_PERIOD)
            : null;

        // Initialize market fundamentals
        this.lastMarketCap = 0.0;
        this.lastCirculatingSupply = 0L;
    }

    // Legacy method for backward compatibility
    synchronized void initialize(final List<Map<String, Object>> historicalData) {
        if (historicalData == null || historicalData.isEmpty()) {
            return;
        }

        final var sorted = historicalData.stream()
            .sorted(Comparator.comparing(m -> (OffsetDateTime) m.get("timestamp")))
            .toList();

        for (final var row : sorted) {
            final var timestamp = (OffsetDateTime) row.get("timestamp");
            final var close = ((Number) row.get("close_price")).doubleValue();

            // Try to get OHLCV data if available
            final var open = row.containsKey("open_price") ? ((Number) row.get("open_price")).doubleValue() : close;
            final var high = row.containsKey("high_price") ? ((Number) row.get("high_price")).doubleValue() : close;
            final var low = row.containsKey("low_price") ? ((Number) row.get("low_price")).doubleValue() : close;
            final var volume = row.containsKey("volume") ? ((Number) row.get("volume")).doubleValue() : 0.0;

            if (config.includeMarketFundamentals) {
                final var marketCap = row.containsKey("market_cap") ? ((Number) row.get("market_cap")).doubleValue() : 0.0;
                final var supply = row.containsKey("circulating_supply") ? ((Number) row.get("circulating_supply")).longValue() : 0L;
                addBar(timestamp, open, high, low, close, volume, marketCap, supply);
            } else {
                addBar(timestamp, open, high, low, close, volume, 0.0, 0L);
            }
        }
    }

    // New method for OHLCV initialization
    synchronized void initializeWithOhlcv(final List<OhlcvPoint> historicalData) {
        if (historicalData == null || historicalData.isEmpty()) {
            return;
        }

        final var sorted = historicalData.stream()
            .sorted(Comparator.comparing(p -> p.timestamp))
            .toList();

        for (final var point : sorted) {
            addBar(point.timestamp, point.open, point.high, point.low, point.close,
                   point.volume, point.marketCap, point.circulatingSupply);
        }
    }

    // Legacy method for backward compatibility
    synchronized MovingAverages addPrice(final OffsetDateTime timestamp, final double close) {
        addBar(timestamp, close, close, close, close, 0.0, 0.0, 0L);
        return computeMovingAverages();
    }

    // New method for OHLCV data
    synchronized AnalysisResult addOhlcv(final OhlcvPoint point) {
        addBar(point.timestamp, point.open, point.high, point.low, point.close,
               point.volume, point.marketCap, point.circulatingSupply);
        return computeAnalysisResult();
    }

    synchronized int getDataCount() {
        return barSeries.getBarCount();
    }

    private void addBar(final OffsetDateTime timestamp, final double open, final double high,
                        final double low, final double close, final double volume,
                        final double marketCap, final long circulatingSupply) {
        final var instant = timestamp.toInstant();
        final var bar = barSeries.barBuilder()
            .timePeriod(DEFAULT_DURATION)
            .endTime(instant)
            .openPrice(open)
            .highPrice(high)
            .lowPrice(low)
            .closePrice(close)
            .volume(volume)
            .build();
        barSeries.addBar(bar);

        // Track market fundamentals
        this.lastMarketCap = marketCap;
        this.lastCirculatingSupply = circulatingSupply;
    }

    private MovingAverages computeMovingAverages() {
        final var lastIndex = barSeries.getEndIndex();

        final var sma50 = getIndicatorValue(sma50Indicator, lastIndex, 50);
        final var sma100 = getIndicatorValue(sma100Indicator, lastIndex, 100);
        final var sma200 = getIndicatorValue(sma200Indicator, lastIndex, 200);
        final var ema50 = getIndicatorValue(ema50Indicator, lastIndex, 50);
        final var ema100 = getIndicatorValue(ema100Indicator, lastIndex, 100);
        final var ema200 = getIndicatorValue(ema200Indicator, lastIndex, 200);

        return new MovingAverages(sma50, sma100, sma200, ema50, ema100, ema200);
    }

    private AnalysisResult computeAnalysisResult() {
        final var lastIndex = barSeries.getEndIndex();

        // Moving averages
        final var sma50 = getIndicatorValue(sma50Indicator, lastIndex, 50);
        final var sma100 = getIndicatorValue(sma100Indicator, lastIndex, 100);
        final var sma200 = getIndicatorValue(sma200Indicator, lastIndex, 200);
        final var ema50 = getIndicatorValue(ema50Indicator, lastIndex, 50);
        final var ema100 = getIndicatorValue(ema100Indicator, lastIndex, 100);
        final var ema200 = getIndicatorValue(ema200Indicator, lastIndex, 200);

        // Momentum
        final var rsi14 = getIndicatorValue(rsiIndicator, lastIndex, RSI_PERIOD);
        final var stochastic14 = getIndicatorValue(stochasticIndicator, lastIndex, STOCHASTIC_PERIOD);

        // MACD
        Double macdLine = null;
        Double macdSignal = null;
        Double macdHistogram = null;
        if (macdIndicator != null && macdSignalIndicator != null) {
            macdLine = getIndicatorValue(macdIndicator, lastIndex, MACD_SLOW);
            macdSignal = getIndicatorValue(macdSignalIndicator, lastIndex, MACD_SIGNAL);
            if (macdLine != null && macdSignal != null) {
                macdHistogram = macdLine - macdSignal;
            }
        }

        // Bollinger Bands
        final var bbMiddle = getIndicatorValue(bbMiddleIndicator, lastIndex, BB_PERIOD);
        final var bbUpper = getIndicatorValue(bbUpperIndicator, lastIndex, BB_PERIOD);
        final var bbLower = getIndicatorValue(bbLowerIndicator, lastIndex, BB_PERIOD);

        // Volatility
        final var atr14 = getIndicatorValue(atrIndicator, lastIndex, ATR_PERIOD);
        final var stdDev20 = getIndicatorValue(stdDevIndicator, lastIndex, BB_PERIOD);

        // Volume
        final var vwap = getIndicatorValue(vwapIndicator, lastIndex, 1);
        final var volumeSma20 = getIndicatorValue(volumeSmaIndicator, lastIndex, VOLUME_SMA_PERIOD);

        // Market fundamentals (use last known values)
        final var marketCap = config.includeMarketFundamentals ? lastMarketCap : null;
        final var circulatingSupply = config.includeMarketFundamentals ? lastCirculatingSupply : null;

        return new AnalysisResult(
            sma50, sma100, sma200, ema50, ema100, ema200,
            rsi14, stochastic14,
            macdLine, macdSignal, macdHistogram,
            bbMiddle, bbUpper, bbLower,
            atr14, stdDev20,
            vwap, volumeSma20,
            marketCap, circulatingSupply
        );
    }

    private Double getIndicatorValue(final org.ta4j.core.Indicator<org.ta4j.core.num.Num> indicator,
                                      final int index,
                                      final int period) {
        if (indicator == null || barSeries.getBarCount() < period) {
            return null;
        }
        try {
            return indicator.getValue(index).doubleValue();
        } catch (final Exception e) {
            return null;
        }
    }
}
