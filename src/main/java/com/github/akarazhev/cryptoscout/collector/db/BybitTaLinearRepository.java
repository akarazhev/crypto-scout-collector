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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_BANKRUPTCY_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_EVENT_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_EXECUTED_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_POSITION_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1000_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_200_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_200_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_50_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_50_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_ENGINE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1000_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_IS_BLOCK_TRADE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_IS_RPI;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_SIZE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_TAKER_SIDE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_TRADE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRange;
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
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBigDecimal;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBoolean;

public final class BybitTaLinearRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static BybitTaLinearRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new BybitTaLinearRepository(reactor, collectorDataSource);
    }

    private BybitTaLinearRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
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

    public int savePublicTrade(final Iterable<Map<String, Object>> trades, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(LINEAR_PUBLIC_TRADE_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var trade : trades) {
                    final var rows = getRows(DATA, trade);
                    if (rows != null) {
                        for (final var row : rows) {
                            final var symbol = (String) row.get(SYMBOL_NAME);
                            final var tradeTime = row.get(T);
                            final var price = toBigDecimal(row.get(P));
                            final var size = toBigDecimal(row.get(V));
                            final var takerSide = (String) row.get(SIDE);
                            final var isBlock = toBoolean(row.get(BT));
                            final var isRpi = toBoolean(row.get(RPI));

                            if (symbol == null || tradeTime == null || price == null || size == null || takerSide == null
                                    || isBlock == null || isRpi == null) {
                                continue; // skip malformed rows
                            }

                            ps.setString(LINEAR_PUBLIC_TRADE_SYMBOL, symbol);
                            ps.setObject(LINEAR_PUBLIC_TRADE_TRADE_TIME, toOdt(tradeTime));
                            ps.setBigDecimal(LINEAR_PUBLIC_TRADE_PRICE, price);
                            ps.setBigDecimal(LINEAR_PUBLIC_TRADE_SIZE, size);
                            ps.setString(LINEAR_PUBLIC_TRADE_TAKER_SIDE, takerSide);
                            ps.setBoolean(LINEAR_PUBLIC_TRADE_IS_BLOCK_TRADE, isBlock);
                            ps.setBoolean(LINEAR_PUBLIC_TRADE_IS_RPI, isRpi);

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

    public List<Map<String, Object>> getPublicTrade(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_PUBLIC_TRADE_SELECT, from, to,
                SYMBOL_NAME, T, P, V, SIDE, BT, RPI);
    }

    public List<Map<String, Object>> getOrderBook1(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_ORDER_BOOK_1_SELECT, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook50(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_ORDER_BOOK_50_SELECT, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook200(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_ORDER_BOOK_200_SELECT, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getOrderBook1000(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_ORDER_BOOK_1000_SELECT, from, to,
                SYMBOL_NAME, CTS, SIDE, P, V);
    }

    public List<Map<String, Object>> getAllLiquidation(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, LINEAR_ALL_LIQUIDATION_SELECT, from, to,
                SYMBOL_NAME, T, SIDE, V, P);
    }

    public int saveOrderBook1(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, LINEAR_ORDER_BOOK_1_INSERT);
    }

    public int saveOrderBook50(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, LINEAR_ORDER_BOOK_50_INSERT);
    }

    public int saveOrderBook200(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return saveOrderBooks(orderBooks, offset, LINEAR_ORDER_BOOK_200_INSERT);
    }

    public int saveOrderBook1000(final Iterable<Map<String, Object>> orderBooks, final long offset)
            throws SQLException {
        return saveOrderBooks(orderBooks, offset, LINEAR_ORDER_BOOK_1000_INSERT);
    }

    public int saveAllLiquidation(final Iterable<Map<String, Object>> allLiquidations, final long offset)
            throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(LINEAR_ALL_LIQUIDATION_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var allLiquidation : allLiquidations) {
                    final var rows = getRows(DATA, allLiquidation);
                    if (rows != null) {
                        for (final var row : rows) {
                            final var timestamp = row.get(T);
                            final var symbol = (String) row.get(SYMBOL_NAME);
                            final var positionSide = (String) row.get(SIDE);
                            final var executedSize = toBigDecimal(row.get(V));
                            final var bankruptcyPrice = toBigDecimal(row.get(P));

                            if (symbol == null || timestamp == null || positionSide == null || executedSize == null ||
                                    bankruptcyPrice == null) {
                                continue; // skip malformed rows
                            }

                            ps.setString(LINEAR_ALL_LIQUIDATION_SYMBOL, symbol);
                            ps.setObject(LINEAR_ALL_LIQUIDATION_EVENT_TIME, toOdt(timestamp));
                            ps.setString(LINEAR_ALL_LIQUIDATION_POSITION_SIDE, positionSide);
                            ps.setBigDecimal(LINEAR_ALL_LIQUIDATION_EXECUTED_SIZE, executedSize);
                            ps.setBigDecimal(LINEAR_ALL_LIQUIDATION_BANKRUPTCY_PRICE, bankruptcyPrice);

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

    private int saveOrderBooks(final Iterable<Map<String, Object>> orderBooks, final long offset, final String insertSql)
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
                        ps.setString(LINEAR_ORDER_BOOK_SYMBOL, symbol);
                        ps.setObject(LINEAR_ORDER_BOOK_ENGINE_TIME, toOdt(engineTime));
                        ps.setString(LINEAR_ORDER_BOOK_SIDE, BID);
                        ps.setBigDecimal(LINEAR_ORDER_BOOK_PRICE, toBigDecimal(bid.getFirst()));
                        ps.setBigDecimal(LINEAR_ORDER_BOOK_SIZE, toBigDecimal(bid.get(1)));

                        ps.addBatch();
                        if (++count % batchSize == 0) {
                            ps.executeBatch();
                        }
                    }

                    for (final var ask : asks) {
                        ps.setString(LINEAR_ORDER_BOOK_SYMBOL, symbol);
                        ps.setObject(LINEAR_ORDER_BOOK_ENGINE_TIME, toOdt(engineTime));
                        ps.setString(LINEAR_ORDER_BOOK_SIDE, ASK);
                        ps.setBigDecimal(LINEAR_ORDER_BOOK_PRICE, toBigDecimal(ask.getFirst()));
                        ps.setBigDecimal(LINEAR_ORDER_BOOK_SIZE, toBigDecimal(ask.get(1)));

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
}
