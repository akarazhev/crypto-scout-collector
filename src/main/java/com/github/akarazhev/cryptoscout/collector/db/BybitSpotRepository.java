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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_CROSS_SEQUENCE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_ENGINE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_IS_SNAPSHOT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_UPDATE_ID;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_15M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1D_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_240M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_60M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_END_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_HIGH_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_LOW_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_OPEN_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_START_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_TURNOVER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_CROSS_SEQUENCE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_IS_RPI;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TAKER_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TRADE_ID;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TRADE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_CROSS_SEQUENCE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_HIGH_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LAST_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LOW_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PREV_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PRICE_24H_PCNT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TURNOVER_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_USD_INDEX_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_VOLUME_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.LAST_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.UPSERT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CLOSE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.END;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.HIGH;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.HIGH_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LAST_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LOW;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LOW_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.OPEN;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PREV_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRICE_24H_PCNT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.USD_INDEX_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.asRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getSymbol;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBigDecimal;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBoolean;

public final class BybitSpotRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static BybitSpotRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new BybitSpotRepository(reactor, collectorDataSource);
    }

    private BybitSpotRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.stream = AmqpConfig.getAmqpBybitCryptoStream();
    }

    @Override
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public int saveKline15m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_15M_INSERT);
    }

    public int saveKline60m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_60M_INSERT);
    }

    public int saveKline240m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_240M_INSERT);
    }

    public int saveKline1d(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_1D_INSERT);
    }

    private int saveKlines(final Iterable<Map<String, Object>> klines, final long offset, final String insertSql)
            throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(insertSql);
                 final var psOffset = c.prepareStatement(UPSERT)) {
                for (final var kline : klines) {
                    final var row = asRow(DATA, kline);
                    if (row == null) {
                        continue;
                    }

                    final var symbol = getSymbol((String) kline.get(TOPIC_FIELD));
                    final var start = row.get(START);
                    final var end = row.get(END);
                    final var open = toBigDecimal(row.get(OPEN));
                    final var close = toBigDecimal(row.get(CLOSE));
                    final var high = toBigDecimal(row.get(HIGH));
                    final var low = toBigDecimal(row.get(LOW));
                    final var volume = toBigDecimal(row.get(VOLUME));
                    final var turnover = toBigDecimal(row.get(TURNOVER));

                    if (symbol == null || start == null || end == null || open == null || close == null ||
                            high == null || low == null || volume == null || turnover == null) {
                        continue; // skip malformed rows
                    }

                    ps.setString(SPOT_KLINE_SYMBOL, symbol);
                    ps.setObject(SPOT_KLINE_START_TIME, toOdt(start));
                    ps.setObject(SPOT_KLINE_END_TIME, toOdt(end));
                    ps.setBigDecimal(SPOT_KLINE_OPEN_PRICE, open);
                    ps.setBigDecimal(SPOT_KLINE_CLOSE_PRICE, close);
                    ps.setBigDecimal(SPOT_KLINE_HIGH_PRICE, high);
                    ps.setBigDecimal(SPOT_KLINE_LOW_PRICE, low);
                    ps.setBigDecimal(SPOT_KLINE_VOLUME, volume);
                    ps.setBigDecimal(SPOT_KLINE_TURNOVER, turnover);

                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                    }
                }

                ps.executeBatch();
                updateOffset(psOffset, offset);
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

    public int saveTicker(final Iterable<Map<String, Object>> tickers, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(String.format(SPOT_TICKERS_INSERT));
                 final var psOffset = c.prepareStatement(UPSERT)) {
                for (final var ticker : tickers) {
                    final var row = asRow(DATA, ticker);
                    if (row == null) {
                        continue;
                    }

                    final var timestamp = ticker.get(TS);
                    final var crossSequence = ticker.get(CS);
                    final var symbol = (String) row.get(SYMBOL);
                    final var lastPrice = toBigDecimal(row.get(LAST_PRICE));
                    final var highPrice24h = toBigDecimal(row.get(HIGH_PRICE_24H));
                    final var lowPrice24h = toBigDecimal(row.get(LOW_PRICE_24H));
                    final var prevPrice24h = toBigDecimal(row.get(PREV_PRICE_24H));
                    final var volume24h = toBigDecimal(row.get(VOLUME_24H));
                    final var turnover24h = toBigDecimal(row.get(TURNOVER_24H));
                    final var price24hPcnt = toBigDecimal(row.get(PRICE_24H_PCNT));
                    final var usdIndexPrice = row.get(USD_INDEX_PRICE);

                    if (timestamp == null || crossSequence == null || symbol == null || lastPrice == null ||
                            highPrice24h == null || lowPrice24h == null || prevPrice24h == null || volume24h == null ||
                            turnover24h == null || price24hPcnt == null) {
                        continue; // skip malformed rows
                    }

                    ps.setObject(SPOT_TICKERS_TIMESTAMP, toOdt(timestamp));
                    ps.setObject(SPOT_TICKERS_CROSS_SEQUENCE, ((Number) crossSequence).longValue());
                    ps.setString(SPOT_TICKERS_SYMBOL, symbol);
                    ps.setBigDecimal(SPOT_TICKERS_LAST_PRICE, lastPrice);
                    ps.setBigDecimal(SPOT_TICKERS_HIGH_PRICE_24H, highPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_LOW_PRICE_24H, lowPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_PREV_PRICE_24H, prevPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_VOLUME_24H, volume24h);
                    ps.setBigDecimal(SPOT_TICKERS_TURNOVER_24H, turnover24h);
                    ps.setBigDecimal(SPOT_TICKERS_PRICE_24H_PCNT, price24hPcnt);
                    // may be null
                    if (usdIndexPrice != null) {
                        ps.setBigDecimal(SPOT_TICKERS_USD_INDEX_PRICE, toBigDecimal(usdIndexPrice));
                    } else {
                        ps.setNull(SPOT_TICKERS_USD_INDEX_PRICE, Types.NUMERIC);
                    }

                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                    }
                }

                ps.executeBatch();
                updateOffset(psOffset, offset);
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

    public int savePublicTrade(final Iterable<Map<String, Object>> trades, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(SPOT_PUBLIC_TRADE_INSERT);
                 final var psOffset = c.prepareStatement(UPSERT)) {
                for (final var trade : trades) {
                    final var row = asRow(DATA, trade);
                    if (row == null) continue;

                    final var symbol = (String) row.get("symbol");
                    final var tradeId = (String) row.get("trade_id");
                    final var tradeTimeObj = row.get("trade_time");
                    final var price = toBigDecimal(row.get("price"));
                    final var size = toBigDecimal(row.get("size"));
                    final var takerSide = (String) row.get("taker_side");
                    final var csObj = row.containsKey("cross_sequence") ? row.get("cross_sequence") : trade.get(CS);
                    final var isBlock = toBoolean(row.get("is_block_trade"));
                    final var isRpi = toBoolean(row.get("is_rpi"));

                    if (symbol == null || tradeId == null || tradeTimeObj == null || price == null || size == null ||
                            takerSide == null || csObj == null || isBlock == null || isRpi == null) {
                        continue; // skip malformed rows
                    }

                    ps.setString(SPOT_PUBLIC_TRADE_SYMBOL, symbol);
                    ps.setObject(SPOT_PUBLIC_TRADE_TRADE_TIME, toOdt(tradeTimeObj));
                    ps.setString(SPOT_PUBLIC_TRADE_TRADE_ID, tradeId);
                    ps.setBigDecimal(SPOT_PUBLIC_TRADE_PRICE, price);
                    ps.setBigDecimal(SPOT_PUBLIC_TRADE_SIZE, size);
                    ps.setString(SPOT_PUBLIC_TRADE_TAKER_SIDE, takerSide);
                    ps.setLong(SPOT_PUBLIC_TRADE_CROSS_SEQUENCE, ((Number) csObj).longValue());
                    ps.setBoolean(SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE, isBlock);
                    ps.setBoolean(SPOT_PUBLIC_TRADE_IS_RPI, isRpi);

                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                    }
                }

                ps.executeBatch();
                updateOffset(psOffset, offset);
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

    public int saveOrderBook200(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(SPOT_ORDER_BOOK_200_INSERT);
                 final var psOffset = c.prepareStatement(UPSERT)) {
                for (final var ob : orderBooks) {
                    final var row = asRow(DATA, ob);
                    if (row == null) continue;

                    final var symbol = (String) row.get("symbol");
                    final var engineTime = row.get("engine_time");
                    final var side = (String) row.get("side");
                    final var price = toBigDecimal(row.get("price"));
                    final var size = toBigDecimal(row.get("size"));
                    final var updateIdObj = row.get("update_id");
                    final var csObj = row.containsKey("cross_sequence") ? row.get("cross_sequence") : ob.get(CS);
                    final var isSnapshot = toBoolean(row.get("is_snapshot"));

                    if (symbol == null || engineTime == null || side == null || price == null || size == null ||
                            updateIdObj == null || csObj == null || isSnapshot == null) {
                        continue; // skip malformed rows
                    }

                    ps.setString(SPOT_ORDER_BOOK_200_SYMBOL, symbol);
                    ps.setObject(SPOT_ORDER_BOOK_200_ENGINE_TIME, toOdt(engineTime));
                    ps.setString(SPOT_ORDER_BOOK_200_SIDE, side);
                    ps.setBigDecimal(SPOT_ORDER_BOOK_200_PRICE, price);
                    ps.setBigDecimal(SPOT_ORDER_BOOK_200_SIZE, size);
                    ps.setLong(SPOT_ORDER_BOOK_200_UPDATE_ID, ((Number) updateIdObj).longValue());
                    ps.setLong(SPOT_ORDER_BOOK_200_CROSS_SEQUENCE, ((Number) csObj).longValue());
                    ps.setBoolean(SPOT_ORDER_BOOK_200_IS_SNAPSHOT, isSnapshot);

                    ps.addBatch();
                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                    }
                }

                ps.executeBatch();
                updateOffset(psOffset, offset);
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

    private void updateOffset(final PreparedStatement ps, final long offset) throws SQLException {
        ps.setString(STREAM, stream);
        ps.setLong(LAST_OFFSET, offset);
        ps.executeUpdate();
    }
}
