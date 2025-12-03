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
import java.time.OffsetDateTime;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.ASK;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BID;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1000_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_50_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_ENGINE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_IS_RPI;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TAKER_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TRADE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_50_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1000_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRange;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRangeBySymbol;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.A;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.B;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.BT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.P;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RPI;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SIDE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SYMBOL_NAME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.U;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.V;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getRows;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toDouble;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBoolean;

public final class BybitTaSpotRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static BybitTaSpotRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new BybitTaSpotRepository(reactor, collectorDataSource);
    }

    private BybitTaSpotRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.stream = AmqpConfig.getAmqpBybitTaCryptoStream();
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    public int savePublicTrade(final List<Map<String, Object>> trades, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(SPOT_PUBLIC_TRADE_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var trade : trades) {
                    final var rows = getRows(DATA, trade);
                    if (rows != null) {
                        for (final var row : rows) {
                            final var symbol = (String) row.get(SYMBOL_NAME);
                            final var tradeTime = row.get(T);
                            final var price = toDouble(row.get(P));
                            final var size = toDouble(row.get(V));
                            final var takerSide = (String) row.get(SIDE);
                            final var isBlock = toBoolean(row.get(BT));
                            final var isRpi = toBoolean(row.get(RPI));

                            if (symbol == null || tradeTime == null || price == null || size == null || takerSide == null
                                    || isBlock == null || isRpi == null) {
                                continue; // skip malformed rows
                            }

                            ps.setString(SPOT_PUBLIC_TRADE_SYMBOL, symbol);
                            ps.setObject(SPOT_PUBLIC_TRADE_TRADE_TIME, toOdt(tradeTime));
                            ps.setDouble(SPOT_PUBLIC_TRADE_PRICE, price);
                            ps.setDouble(SPOT_PUBLIC_TRADE_SIZE, size);
                            ps.setString(SPOT_PUBLIC_TRADE_TAKER_SIDE, takerSide);
                            ps.setBoolean(SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE, isBlock);
                            ps.setBoolean(SPOT_PUBLIC_TRADE_IS_RPI, isRpi);

                            ps.addBatch();
                            if (++count % batchSize == 0) {
                                ps.executeBatch();
                            }
                        }
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

    public int saveOrderBook1(final List<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, SPOT_ORDER_BOOK_1_INSERT);
    }

    public int saveOrderBook50(final List<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, SPOT_ORDER_BOOK_50_INSERT);
    }

    public int saveOrderBook200(final List<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, SPOT_ORDER_BOOK_200_INSERT);
    }

    public int saveOrderBook1000(final List<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, SPOT_ORDER_BOOK_1000_INSERT);
    }

    private int saveOrderBooks(final List<Map<String, Object>> orderBooks, final long offset, final String insertSql)
            throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(insertSql);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var order : orderBooks) {
                    final var row = getRow(DATA, order);
                    if (row == null) {
                        continue;
                    }

                    final var symbol = (String) row.get(SYMBOL_NAME);
                    final var engineTime = order.get(CTS);
                    @SuppressWarnings("unchecked") final var bids = (List<List<String>>) row.get(B);
                    @SuppressWarnings("unchecked") final var asks = (List<List<String>>) row.get(A);
                    final var updateId = row.get(U);

                    if (symbol == null || engineTime == null || updateId == null) {
                        continue; // skip malformed rows
                    }

                    for (final var bid : bids) {
                        ps.setString(SPOT_ORDER_BOOK_SYMBOL, symbol);
                        ps.setObject(SPOT_ORDER_BOOK_ENGINE_TIME, toOdt(engineTime));
                        ps.setString(SPOT_ORDER_BOOK_SIDE, BID);
                        ps.setDouble(SPOT_ORDER_BOOK_PRICE, toDouble(bid.getFirst()));
                        ps.setDouble(SPOT_ORDER_BOOK_SIZE, toDouble(bid.get(1)));

                        ps.addBatch();
                        if (++count % batchSize == 0) {
                            ps.executeBatch();
                        }
                    }

                    for (final var ask : asks) {
                        ps.setString(SPOT_ORDER_BOOK_SYMBOL, symbol);
                        ps.setObject(SPOT_ORDER_BOOK_ENGINE_TIME, toOdt(engineTime));
                        ps.setString(SPOT_ORDER_BOOK_SIDE, ASK);
                        ps.setDouble(SPOT_ORDER_BOOK_PRICE, toDouble(ask.getFirst()));
                        ps.setDouble(SPOT_ORDER_BOOK_SIZE, toDouble(ask.get(1)));

                        ps.addBatch();
                        if (++count % batchSize == 0) {
                            ps.executeBatch();
                        }
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

    public List<Map<String, Object>> getPublicTrade(final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRange(dataSource, SPOT_PUBLIC_TRADE_SELECT, from, to,
                SYMBOL_NAME, T, P, V, SIDE, BT, RPI);
    }

    public List<Map<String, Object>> getPublicTrade(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, SPOT_PUBLIC_TRADE_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL_NAME, T, P, V, SIDE, BT, RPI);
    }

    public List<Map<String, Object>> getOrderBook1(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, SPOT_ORDER_BOOK_1_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook50(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, SPOT_ORDER_BOOK_50_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook200(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, SPOT_ORDER_BOOK_200_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook1000(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, SPOT_ORDER_BOOK_1000_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }
}
