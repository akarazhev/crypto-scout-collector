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

package com.github.akarazhev.cryptoscout.collector.db;

import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_15M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1D_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_240M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_5M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_60M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_5M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_15M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_60M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_240M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1D_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1M_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_5M_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_15M_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_60M_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_240M_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1D_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_END_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_HIGH_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_LOW_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_OPEN_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_START_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_TURNOVER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_HIGH_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_LAST_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_LOW_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PREV_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PREV_PRICE_1H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_MARK_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_INDEX_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_OPEN_INTEREST;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_OPEN_INTEREST_VALUE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PRICE_24H_PCNT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_FUNDING_INTERVAL_HOUR;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_FUNDING_CAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_NEXT_FUNDING_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_FUNDING_RATE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_BID1_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_BID1_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_ASK1_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_ASK1_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_DELIVERY_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_BASIS_RATE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_DELIVERY_FEE_RATE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PREDICTED_DELIVERY_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_BASIS;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_BASIS_RATE_YEAR;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PRE_OPEN_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_PRE_QTY;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_CUR_PRE_LISTING_PHASE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_TICK_DIRECTION;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_TURNOVER_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_VOLUME_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRange;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRangeBySymbol;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CLOSE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.END;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.HIGH;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.HIGH_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LAST_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LOW;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LOW_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.OPEN;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PREV_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PREV_PRICE_1H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRICE_24H_PCNT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TICK_DIRECTION;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.MARK_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.INDEX_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.OPEN_INTEREST;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.OPEN_INTEREST_VALUE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.FUNDING_INTERVAL_HOUR;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.FUNDING_CAP;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.NEXT_FUNDING_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.FUNDING_RATE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BID1_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BID1_SIZE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.ASK1_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.ASK1_SIZE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DELIVERY_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BASIS_RATE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DELIVERY_FEE_RATE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PREDICTED_DELIVERY_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BASIS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BASIS_RATE_YEAR;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRE_OPEN_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRE_QTY;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CUR_PRE_LISTING_PHASE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getFirstRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getSymbol;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toDouble;

public final class BybitLinearRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static BybitLinearRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new BybitLinearRepository(reactor, collectorDataSource);
    }

    private BybitLinearRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.stream = AmqpConfig.getAmqpBybitCryptoStream();
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    public int saveKline1m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_1M_INSERT);
    }

    public int saveKline5m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_5M_INSERT);
    }

    public int saveKline15m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_15M_INSERT);
    }

    public int saveKline60m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_60M_INSERT);
    }

    public int saveKline240m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_240M_INSERT);
    }

    public int saveKline1d(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, LINEAR_KLINE_1D_INSERT);
    }

    public int saveTicker(final List<Map<String, Object>> tickers, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(LINEAR_TICKERS_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var ticker : tickers) {
                    final var row = getRow(DATA, ticker);
                    if (row == null) {
                        continue;
                    }

                    final var timestamp = ticker.get(TS);
                    // Linear perpetual
                    final var symbol = (String) row.get(SYMBOL);
                    final var tickDirection = (String) row.get(TICK_DIRECTION);
                    final var price24hPcnt = toDouble(row.get(PRICE_24H_PCNT));
                    final var lastPrice = toDouble(row.get(LAST_PRICE));
                    final var prevPrice24h = toDouble(row.get(PREV_PRICE_24H));
                    final var highPrice24h = toDouble(row.get(HIGH_PRICE_24H));
                    final var lowPrice24h = toDouble(row.get(LOW_PRICE_24H));
                    final var prevPrice1h = toDouble(row.get(PREV_PRICE_1H));
                    final var markPrice = toDouble(row.get(MARK_PRICE));
                    final var indexPrice = toDouble(row.get(INDEX_PRICE));
                    final var openInterest = toDouble(row.get(OPEN_INTEREST));
                    final var openInterestValue = toDouble(row.get(OPEN_INTEREST_VALUE));
                    final var turnover24h = toDouble(row.get(TURNOVER_24H));
                    final var volume24h = toDouble(row.get(VOLUME_24H));
                    final var fundingIntervalHour = toDouble(row.get(FUNDING_INTERVAL_HOUR)); // can be null
                    final var fundingCap = toDouble(row.get(FUNDING_CAP)); // can be null
                    final var nextFundingTime = (String) row.get(NEXT_FUNDING_TIME);
                    final var fundingRate = toDouble(row.get(FUNDING_RATE));
                    final var bid1Price = toDouble(row.get(BID1_PRICE));
                    final var bid1Size = toDouble(row.get(BID1_SIZE));
                    final var ask1Price = toDouble(row.get(ASK1_PRICE));
                    final var ask1Size = toDouble(row.get(ASK1_SIZE));
                    final var preOpenPrice = toDouble(row.get(PRE_OPEN_PRICE)); // can be null
                    final var preQty = toDouble(row.get(PRE_QTY)); // cen be null
                    final var curPreListingPhase = (String) row.get(CUR_PRE_LISTING_PHASE); // can be empty
                    // Linear futures
                    final var deliveryTime = row.get(DELIVERY_TIME); // can be null
                    final var basisRate = toDouble(row.get(BASIS_RATE)); // can be null
                    final var deliveryFeeRate = toDouble(row.get(DELIVERY_FEE_RATE)); // can be null
                    final var predictedDeliveryPrice = toDouble(row.get(PREDICTED_DELIVERY_PRICE)); // can be null
                    final var basis = toDouble(row.get(BASIS)); // can be null
                    final var basisRateYear = toDouble(row.get(BASIS_RATE_YEAR)); // can be null

                    if (timestamp == null || symbol == null || tickDirection == null || price24hPcnt == null ||
                            lastPrice == null || prevPrice24h == null || highPrice24h == null || lowPrice24h == null ||
                            prevPrice1h == null || markPrice == null || indexPrice == null || openInterest == null ||
                            openInterestValue == null || turnover24h == null || volume24h == null ||
                            nextFundingTime == null || fundingRate == null || bid1Price == null || bid1Size == null ||
                            ask1Price == null || ask1Size == null) {
                        continue; // skip malformed rows
                    }

                    ps.setObject(LINEAR_TICKERS_TIMESTAMP, toOdt(timestamp));
                    ps.setString(LINEAR_TICKERS_SYMBOL, symbol);
                    ps.setString(LINEAR_TICKERS_TICK_DIRECTION, tickDirection);
                    ps.setDouble(LINEAR_TICKERS_PRICE_24H_PCNT, price24hPcnt);
                    ps.setDouble(LINEAR_TICKERS_LAST_PRICE, lastPrice);
                    ps.setDouble(LINEAR_TICKERS_PREV_PRICE_24H, prevPrice24h);
                    ps.setDouble(LINEAR_TICKERS_HIGH_PRICE_24H, highPrice24h);
                    ps.setDouble(LINEAR_TICKERS_LOW_PRICE_24H, lowPrice24h);
                    ps.setDouble(LINEAR_TICKERS_PREV_PRICE_1H, prevPrice1h);
                    ps.setDouble(LINEAR_TICKERS_MARK_PRICE, markPrice);
                    ps.setDouble(LINEAR_TICKERS_INDEX_PRICE, indexPrice);
                    ps.setDouble(LINEAR_TICKERS_OPEN_INTEREST, openInterest);
                    ps.setDouble(LINEAR_TICKERS_OPEN_INTEREST_VALUE, openInterestValue);
                    ps.setDouble(LINEAR_TICKERS_TURNOVER_24H, turnover24h);
                    ps.setDouble(LINEAR_TICKERS_VOLUME_24H, volume24h);
                    if (fundingIntervalHour != null) {
                        ps.setInt(LINEAR_TICKERS_FUNDING_INTERVAL_HOUR, fundingIntervalHour.intValue());
                    } else {
                        ps.setNull(LINEAR_TICKERS_FUNDING_INTERVAL_HOUR, Types.INTEGER);
                    }

                    if (fundingCap != null) {
                        ps.setDouble(LINEAR_TICKERS_FUNDING_CAP, fundingCap);
                    } else {
                        ps.setNull(LINEAR_TICKERS_FUNDING_CAP, Types.DOUBLE);
                    }

                    ps.setObject(LINEAR_TICKERS_NEXT_FUNDING_TIME, toOdt(Long.parseLong(nextFundingTime)));
                    ps.setDouble(LINEAR_TICKERS_FUNDING_RATE, fundingRate);
                    ps.setDouble(LINEAR_TICKERS_BID1_PRICE, bid1Price);
                    ps.setDouble(LINEAR_TICKERS_BID1_SIZE, bid1Size);
                    ps.setDouble(LINEAR_TICKERS_ASK1_PRICE, ask1Price);
                    ps.setDouble(LINEAR_TICKERS_ASK1_SIZE, ask1Size);
                    if (preOpenPrice != null) {
                        ps.setDouble(LINEAR_TICKERS_PRE_OPEN_PRICE, preOpenPrice);
                    } else {
                        ps.setNull(LINEAR_TICKERS_PRE_OPEN_PRICE, Types.DOUBLE);
                    }

                    if (preQty != null) {
                        ps.setDouble(LINEAR_TICKERS_PRE_QTY, preQty);
                    } else {
                        ps.setNull(LINEAR_TICKERS_PRE_QTY, Types.DOUBLE);
                    }

                    if (curPreListingPhase != null && !curPreListingPhase.isBlank()) {
                        ps.setString(LINEAR_TICKERS_CUR_PRE_LISTING_PHASE, curPreListingPhase);
                    } else {
                        ps.setNull(LINEAR_TICKERS_CUR_PRE_LISTING_PHASE, Types.VARCHAR);
                    }

                    if (deliveryTime != null) {
                        ps.setObject(LINEAR_TICKERS_DELIVERY_TIME, toOdt(deliveryTime));
                    } else {
                        ps.setNull(LINEAR_TICKERS_DELIVERY_TIME, Types.TIMESTAMP);
                    }

                    if (basisRate != null) {
                        ps.setDouble(LINEAR_TICKERS_BASIS_RATE, basisRate);
                    } else {
                        ps.setNull(LINEAR_TICKERS_BASIS_RATE, Types.DOUBLE);
                    }

                    if (deliveryFeeRate != null) {
                        ps.setDouble(LINEAR_TICKERS_DELIVERY_FEE_RATE, deliveryFeeRate);
                    } else {
                        ps.setNull(LINEAR_TICKERS_DELIVERY_FEE_RATE, Types.DOUBLE);
                    }

                    if (predictedDeliveryPrice != null) {
                        ps.setDouble(LINEAR_TICKERS_PREDICTED_DELIVERY_PRICE, predictedDeliveryPrice);
                    } else {
                        ps.setNull(LINEAR_TICKERS_PREDICTED_DELIVERY_PRICE, Types.DOUBLE);
                    }

                    if (basis != null) {
                        ps.setDouble(LINEAR_TICKERS_BASIS, basis);
                    } else {
                        ps.setNull(LINEAR_TICKERS_BASIS, Types.DOUBLE);
                    }

                    if (basisRateYear != null) {
                        ps.setDouble(LINEAR_TICKERS_BASIS_RATE_YEAR, basisRateYear);
                    } else {
                        ps.setNull(LINEAR_TICKERS_BASIS_RATE_YEAR, Types.DOUBLE);
                    }

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

    public List<Map<String, Object>> getKline1m(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_1M_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline5m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_KLINE_5M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline5m(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_5M_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline15m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_KLINE_15M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline15m(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_15M_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline60m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_KLINE_60M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline60m(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_60M_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline240m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_KLINE_240M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline240m(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_240M_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline1d(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_KLINE_1D_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline1d(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, LINEAR_KLINE_1D_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getTicker(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_TICKERS_SELECT, from, to,
                SYMBOL, TS, TICK_DIRECTION, PRICE_24H_PCNT, LAST_PRICE, PREV_PRICE_24H, HIGH_PRICE_24H,
                LOW_PRICE_24H, PREV_PRICE_1H, MARK_PRICE, INDEX_PRICE, OPEN_INTEREST, OPEN_INTEREST_VALUE, TURNOVER_24H,
                VOLUME_24H, FUNDING_INTERVAL_HOUR, FUNDING_CAP, NEXT_FUNDING_TIME, FUNDING_RATE, BID1_PRICE, BID1_SIZE,
                ASK1_PRICE, ASK1_SIZE, DELIVERY_TIME, BASIS_RATE, DELIVERY_FEE_RATE, PREDICTED_DELIVERY_PRICE, BASIS,
                BASIS_RATE_YEAR, PRE_OPEN_PRICE, PRE_QTY, CUR_PRE_LISTING_PHASE);
    }

    private int saveKlines(final List<Map<String, Object>> klines, final long offset, final String insertSql)
            throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(insertSql);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var kline : klines) {
                    final var row = getFirstRow(DATA, kline);
                    if (row == null) {
                        continue;
                    }

                    final var symbol = getSymbol((String) kline.get(TOPIC_FIELD));
                    final var start = row.get(START);
                    final var end = row.get(END);
                    final var open = toDouble(row.get(OPEN));
                    final var close = toDouble(row.get(CLOSE));
                    final var high = toDouble(row.get(HIGH));
                    final var low = toDouble(row.get(LOW));
                    final var volume = toDouble(row.get(VOLUME));
                    final var turnover = toDouble(row.get(TURNOVER));

                    if (symbol == null || start == null || end == null || open == null || close == null ||
                            high == null || low == null || volume == null || turnover == null) {
                        continue; // skip malformed rows
                    }

                    ps.setString(LINEAR_KLINE_SYMBOL, symbol);
                    ps.setObject(LINEAR_KLINE_START_TIME, toOdt(start));
                    ps.setObject(LINEAR_KLINE_END_TIME, toOdt(end));
                    ps.setDouble(LINEAR_KLINE_OPEN_PRICE, open);
                    ps.setDouble(LINEAR_KLINE_CLOSE_PRICE, close);
                    ps.setDouble(LINEAR_KLINE_HIGH_PRICE, high);
                    ps.setDouble(LINEAR_KLINE_LOW_PRICE, low);
                    ps.setDouble(LINEAR_KLINE_VOLUME, volume);
                    ps.setDouble(LINEAR_KLINE_TURNOVER, turnover);

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
}
