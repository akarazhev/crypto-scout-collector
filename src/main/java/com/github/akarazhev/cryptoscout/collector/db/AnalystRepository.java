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

package com.github.akarazhev.cryptoscout.collector.db;

import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CIRCULATING_SUPPLY;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.KLINE_1W_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.MARKET_CAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.TIME_CLOSE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.TIME_HIGH;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.TIME_LOW;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.TIME_OPEN;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.ATR_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.BB_LOWER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.BB_MIDDLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.BB_PERCENT_B;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.BB_UPPER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.BB_WIDTH;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.CIRCULATING_SUPPLY;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.INDICATORS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.INDICATORS_SELECT_BY_SYMBOL_RANGE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_ATR_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_BB_LOWER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_BB_MIDDLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_BB_PERCENT_B;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_BB_UPPER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_BB_WIDTH;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_CIRCULATING_SUPPLY;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_MACD_HISTOGRAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_MACD_LINE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_MACD_SIGNAL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_MARKET_CAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_MARKET_CAP_TO_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_RSI_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_STD_DEV_20;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_STOCHASTIC_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_VOLUME_SMA_20;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_VWAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.MACD_HISTOGRAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.MACD_LINE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.MACD_SIGNAL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.MARKET_CAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.MARKET_CAP_TO_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.RSI_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.STD_DEV_20;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.STOCHASTIC_14;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.VOLUME_SMA_20;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.VWAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRangeBySymbol;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.CLOSE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.HIGH;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.LOW;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.OPEN;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.VOLUME;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toDouble;

public final class AnalystRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static AnalystRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new AnalystRepository(reactor, collectorDataSource);
    }

    private AnalystRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getAnalystBatchSize();
        this.stream = AmqpConfig.getAmqpCryptoScoutStream();
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    public List<Map<String, Object>> getIndicators(final String symbol, final OffsetDateTime from,
                                                          final OffsetDateTime to) throws SQLException {
        return fetchRangeBySymbol(dataSource, INDICATORS_SELECT_BY_SYMBOL_RANGE, symbol, from, to,
                SYMBOL, TIMESTAMP, CLOSE_PRICE, SMA_50, SMA_100, SMA_200, EMA_50, EMA_100, EMA_200,
                RSI_14, STOCHASTIC_14, MACD_LINE, MACD_SIGNAL, MACD_HISTOGRAM,
                BB_MIDDLE, BB_UPPER, BB_LOWER, BB_WIDTH, BB_PERCENT_B,
                ATR_14, STD_DEV_20, VWAP, VOLUME_SMA_20,
                MARKET_CAP, CIRCULATING_SUPPLY, MARKET_CAP_TO_VOLUME);
    }

    public List<Map<String, Object>> getKlines(final String symbol, final OffsetDateTime from,
                                               final OffsetDateTime to) throws SQLException {
        return fetchRangeBySymbol(dataSource, KLINE_1W_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, TIME_OPEN, TIME_CLOSE, TIME_HIGH, TIME_LOW,
                OPEN, HIGH, LOW, CLOSE, VOLUME, MARKET_CAP, CIRCULATING_SUPPLY, TIMESTAMP);
    }

    public int saveIndicators(final List<Map<String, Object>> indicators, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(INDICATORS_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var indicator : indicators) {
                    final var symbol = (String) indicator.get(SYMBOL);
                    final var timestamp = indicator.get(TIMESTAMP);
                    final var closePrice = toDouble(indicator.get(CLOSE_PRICE));

                    if (symbol == null || timestamp == null || closePrice == null) {
                        continue;
                    }

                    // Moving Averages
                    final var sma50 = toDouble(indicator.get(SMA_50));
                    final var sma100 = toDouble(indicator.get(SMA_100));
                    final var sma200 = toDouble(indicator.get(SMA_200));
                    final var ema50 = toDouble(indicator.get(EMA_50));
                    final var ema100 = toDouble(indicator.get(EMA_100));
                    final var ema200 = toDouble(indicator.get(EMA_200));

                    // Momentum
                    final var rsi14 = toDouble(indicator.get(RSI_14));
                    final var stochastic14 = toDouble(indicator.get(STOCHASTIC_14));

                    // MACD
                    final var macdLine = toDouble(indicator.get(MACD_LINE));
                    final var macdSignal = toDouble(indicator.get(MACD_SIGNAL));
                    final var macdHistogram = toDouble(indicator.get(MACD_HISTOGRAM));

                    // Bollinger Bands
                    final var bbMiddle = toDouble(indicator.get(BB_MIDDLE));
                    final var bbUpper = toDouble(indicator.get(BB_UPPER));
                    final var bbLower = toDouble(indicator.get(BB_LOWER));
                    final var bbWidth = toDouble(indicator.get(BB_WIDTH));
                    final var bbPercentB = toDouble(indicator.get(BB_PERCENT_B));

                    // Volatility
                    final var atr14 = toDouble(indicator.get(ATR_14));
                    final var stdDev20 = toDouble(indicator.get(STD_DEV_20));

                    // Volume
                    final var vwap = toDouble(indicator.get(VWAP));
                    final var volumeSma20 = toDouble(indicator.get(VOLUME_SMA_20));

                    // Market Fundamentals
                    final var marketCap = toDouble(indicator.get(MARKET_CAP));
                    final var circulatingSupply = indicator.get(CIRCULATING_SUPPLY) instanceof Number ?
                            ((Number) indicator.get(CIRCULATING_SUPPLY)).longValue() : null;
                    final var marketCapToVolume = toDouble(indicator.get(MARKET_CAP_TO_VOLUME));

                    // Set parameters
                    ps.setString(IND_SYMBOL, symbol);
                    ps.setObject(IND_TIMESTAMP, timestamp);
                    ps.setDouble(IND_CLOSE_PRICE, closePrice);

                    // Moving Averages
                    setDoubleOrNull(ps, IND_SMA_50, sma50);
                    setDoubleOrNull(ps, IND_SMA_100, sma100);
                    setDoubleOrNull(ps, IND_SMA_200, sma200);
                    setDoubleOrNull(ps, IND_EMA_50, ema50);
                    setDoubleOrNull(ps, IND_EMA_100, ema100);
                    setDoubleOrNull(ps, IND_EMA_200, ema200);

                    // Momentum
                    setDoubleOrNull(ps, IND_RSI_14, rsi14);
                    setDoubleOrNull(ps, IND_STOCHASTIC_14, stochastic14);

                    // MACD
                    setDoubleOrNull(ps, IND_MACD_LINE, macdLine);
                    setDoubleOrNull(ps, IND_MACD_SIGNAL, macdSignal);
                    setDoubleOrNull(ps, IND_MACD_HISTOGRAM, macdHistogram);

                    // Bollinger Bands
                    setDoubleOrNull(ps, IND_BB_MIDDLE, bbMiddle);
                    setDoubleOrNull(ps, IND_BB_UPPER, bbUpper);
                    setDoubleOrNull(ps, IND_BB_LOWER, bbLower);
                    setDoubleOrNull(ps, IND_BB_WIDTH, bbWidth);
                    setDoubleOrNull(ps, IND_BB_PERCENT_B, bbPercentB);

                    // Volatility
                    setDoubleOrNull(ps, IND_ATR_14, atr14);
                    setDoubleOrNull(ps, IND_STD_DEV_20, stdDev20);

                    // Volume
                    setDoubleOrNull(ps, IND_VWAP, vwap);
                    setDoubleOrNull(ps, IND_VOLUME_SMA_20, volumeSma20);

                    // Market Fundamentals
                    setDoubleOrNull(ps, IND_MARKET_CAP, marketCap);
                    if (circulatingSupply != null) {
                        ps.setLong(IND_CIRCULATING_SUPPLY, circulatingSupply);
                    } else {
                        ps.setNull(IND_CIRCULATING_SUPPLY, Types.BIGINT);
                    }
                    setDoubleOrNull(ps, IND_MARKET_CAP_TO_VOLUME, marketCapToVolume);

                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                    }
                }

                ps.executeBatch();
                updateOffset(psOffset, stream, offset);
                c.commit();
            } catch (final Exception ex) {
                c.rollback();
                throw ex;
            } finally {
                c.setAutoCommit(oldAutoCommit);
            }
        }
        return count;
    }

    private void setDoubleOrNull(final PreparedStatement ps, final int index, final Double value)
            throws SQLException {
        if (value == null || Double.isNaN(value)) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }
}
