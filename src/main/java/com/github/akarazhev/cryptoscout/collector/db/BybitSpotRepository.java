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
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_15M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1D_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_5M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_5M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_15M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1D_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_240M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_240M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_60M_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_60M_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_END_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_HIGH_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_LOW_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_OPEN_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_START_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_TURNOVER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_HIGH_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LAST_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LOW_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PREV_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PRICE_24H_PCNT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TURNOVER_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_VOLUME_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRange;
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
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRICE_24H_PCNT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.TOPIC_FIELD;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getFirstRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getSymbol;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBigDecimal;

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
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    public int saveKline1m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_1M_INSERT);
    }

    public int saveKline5m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_5M_INSERT);
    }

    public int saveKline15m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_15M_INSERT);
    }

    public int saveKline60m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_60M_INSERT);
    }

    public int saveKline240m(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_240M_INSERT);
    }

    public int saveKline1d(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        return saveKlines(klines, offset, SPOT_KLINE_1D_INSERT);
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

    public int saveTicker(final List<Map<String, Object>> tickers, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(String.format(SPOT_TICKERS_INSERT));
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var ticker : tickers) {
                    final var row = getRow(DATA, ticker);
                    if (row == null) {
                        continue;
                    }

                    final var timestamp = ticker.get(TS);
                    final var symbol = (String) row.get(SYMBOL);
                    final var lastPrice = toBigDecimal(row.get(LAST_PRICE));
                    final var highPrice24h = toBigDecimal(row.get(HIGH_PRICE_24H));
                    final var lowPrice24h = toBigDecimal(row.get(LOW_PRICE_24H));
                    final var prevPrice24h = toBigDecimal(row.get(PREV_PRICE_24H));
                    final var volume24h = toBigDecimal(row.get(VOLUME_24H));
                    final var turnover24h = toBigDecimal(row.get(TURNOVER_24H));
                    final var price24hPcnt = toBigDecimal(row.get(PRICE_24H_PCNT));

                    if (timestamp == null || symbol == null || lastPrice == null || highPrice24h == null ||
                            lowPrice24h == null || prevPrice24h == null || volume24h == null || turnover24h == null ||
                            price24hPcnt == null) {
                        continue; // skip malformed rows
                    }

                    ps.setObject(SPOT_TICKERS_TIMESTAMP, toOdt(timestamp));
                    ps.setString(SPOT_TICKERS_SYMBOL, symbol);
                    ps.setBigDecimal(SPOT_TICKERS_LAST_PRICE, lastPrice);
                    ps.setBigDecimal(SPOT_TICKERS_HIGH_PRICE_24H, highPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_LOW_PRICE_24H, lowPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_PREV_PRICE_24H, prevPrice24h);
                    ps.setBigDecimal(SPOT_TICKERS_VOLUME_24H, volume24h);
                    ps.setBigDecimal(SPOT_TICKERS_TURNOVER_24H, turnover24h);
                    ps.setBigDecimal(SPOT_TICKERS_PRICE_24H_PCNT, price24hPcnt);

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

    public List<Map<String, Object>> getKline1m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_1M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline5m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_5M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline15m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_15M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline60m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_60M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline240m(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_240M_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getKline1d(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_KLINE_1D_SELECT, from, to,
                SYMBOL, START, END, OPEN, CLOSE, HIGH, LOW, VOLUME, TURNOVER);
    }

    public List<Map<String, Object>> getTicker(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        return fetchRange(dataSource, SPOT_TICKERS_SELECT, from, to,
                SYMBOL, TS, LAST_PRICE, HIGH_PRICE_24H, LOW_PRICE_24H, PREV_PRICE_24H, VOLUME_24H,
                TURNOVER_24H, PRICE_24H_PCNT);
    }
}
