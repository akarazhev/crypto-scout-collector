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
import java.sql.SQLException;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_CIRCULATING_SUPPLY;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_CLOSE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_HIGH;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_LOW;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_MARKET_CAP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_OPEN;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_TIME_CLOSE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_TIME_HIGH;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_TIME_LOW;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_TIME_OPEN;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.FGI_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.FGI_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.FGI_UPDATE_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.FGI_VALUE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.FGI_VALUE_CLASSIFICATION;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.FROM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.TO;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.fetchRangeBySymbol;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.CIRCULATING_SUPPLY;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.CLOSE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.HIGH;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.LOW;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.MARKET_CAP2;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.OPEN;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIME_CLOSE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIME_HIGH;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIME_LOW;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIME_OPEN;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.VALUE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.VALUE_CLASSIFICATION;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.VOLUME;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getFirstRow;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getRow;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static com.github.akarazhev.jcryptolib.util.ValueUtils.toBigDecimal;

public final class CryptoScoutRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static CryptoScoutRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new CryptoScoutRepository(reactor, collectorDataSource);
    }

    private CryptoScoutRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getCryptoScoutBatchSize();
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

    public int saveFgi(final List<Map<String, Object>> fgis, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(FGI_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var fgi : fgis) {
                    if (fgi != null) {
                        final var value = fgi.get(VALUE);
                        if (value instanceof Number n) {
                            ps.setInt(FGI_VALUE, n.intValue());
                        } else if (value instanceof String s) {
                            ps.setInt(FGI_VALUE, Integer.parseInt(s));
                        } else {
                            ps.setNull(FGI_VALUE, Types.INTEGER);
                        }

                        ps.setString(FGI_VALUE_CLASSIFICATION, (String) fgi.get(VALUE_CLASSIFICATION));
                        ps.setObject(FGI_UPDATE_TIME, toOdt(fgi.get(UPDATE_TIME)));

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

    public List<Map<String, Object>> getFgi(final OffsetDateTime from, final OffsetDateTime to) throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(FGI_SELECT)) {
            ps.setObject(FROM, from);
            ps.setObject(TO, to);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put(VALUE, rs.getObject(FGI_VALUE));
                    row.put(VALUE_CLASSIFICATION, rs.getObject(FGI_VALUE_CLASSIFICATION));
                    row.put(UPDATE_TIME, rs.getObject(FGI_UPDATE_TIME));
                    results.add(row);
                }
            }
        }

        return results;
    }

    public int saveKline1d(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(CMC_KLINE_1D_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var kline : klines) {
                    final var row = getFirstRow(QUOTES, kline);
                    if (row == null) {
                        continue;
                    }

                    final var quote = getRow(QUOTE, row);
                    if (quote == null) {
                        continue;
                    }

                    final var symbol = (String) kline.get(SYMBOL);
                    final var timeOpen = row.get(TIME_OPEN);
                    final var timeClose = row.get(TIME_CLOSE);
                    final var timeHigh = row.get(TIME_HIGH);
                    final var timeLow = row.get(TIME_LOW);
                    final var open = toBigDecimal(quote.get(OPEN));
                    final var high = toBigDecimal(quote.get(HIGH));
                    final var low = toBigDecimal(quote.get(LOW));
                    final var close = toBigDecimal(quote.get(CLOSE));
                    final var volume = toBigDecimal(quote.get(VOLUME));
                    final var marketCap = toBigDecimal(quote.get(MARKET_CAP2));
                    final var circulatingSupply = toBigDecimal(quote.get(CIRCULATING_SUPPLY));
                    final var timestamp = quote.get(TIMESTAMP);

                    if (symbol == null || timeOpen == null || timeClose == null || timeHigh == null ||
                            timeLow == null || open == null || high == null || low == null || close == null ||
                            volume == null || marketCap == null || circulatingSupply == null || timestamp == null) {
                        continue;
                    }

                    ps.setString(CMC_KLINE_SYMBOL, symbol);
                    ps.setObject(CMC_KLINE_TIME_OPEN, toOdt(timeOpen));
                    ps.setObject(CMC_KLINE_TIME_CLOSE, toOdt(timeClose));
                    ps.setObject(CMC_KLINE_TIME_HIGH, toOdt(timeHigh));
                    ps.setObject(CMC_KLINE_TIME_LOW, toOdt(timeLow));
                    ps.setBigDecimal(CMC_KLINE_OPEN, open);
                    ps.setBigDecimal(CMC_KLINE_HIGH, high);
                    ps.setBigDecimal(CMC_KLINE_LOW, low);
                    ps.setBigDecimal(CMC_KLINE_CLOSE, close);
                    ps.setBigDecimal(CMC_KLINE_VOLUME, volume);
                    ps.setBigDecimal(CMC_KLINE_MARKET_CAP, marketCap);
                    ps.setBigDecimal(CMC_KLINE_CIRCULATING_SUPPLY, circulatingSupply);
                    ps.setObject(CMC_KLINE_TIMESTAMP, toOdt(timestamp));

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

    public List<Map<String, Object>> getKline1d(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, CMC_KLINE_1D_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, TIME_OPEN, TIME_CLOSE, TIME_HIGH, TIME_LOW,
                OPEN, HIGH, LOW, CLOSE, VOLUME, MARKET_CAP2, CIRCULATING_SUPPLY, TIMESTAMP);
    }

    public int saveKline1w(final List<Map<String, Object>> klines, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(CMC_KLINE_1W_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var kline : klines) {
                    final var row = getFirstRow(QUOTES, kline);
                    if (row == null) {
                        continue;
                    }

                    final var quote = getRow(QUOTE, row);
                    if (quote == null) {
                        continue;
                    }

                    final var symbol = (String) kline.get(SYMBOL);
                    final var timeOpen = row.get(TIME_OPEN);
                    final var timeClose = row.get(TIME_CLOSE);
                    final var timeHigh = row.get(TIME_HIGH);
                    final var timeLow = row.get(TIME_LOW);
                    final var open = toBigDecimal(quote.get(OPEN));
                    final var high = toBigDecimal(quote.get(HIGH));
                    final var low = toBigDecimal(quote.get(LOW));
                    final var close = toBigDecimal(quote.get(CLOSE));
                    final var volume = toBigDecimal(quote.get(VOLUME));
                    final var marketCap = toBigDecimal(quote.get(MARKET_CAP2));
                    final var circulatingSupply = toBigDecimal(quote.get(CIRCULATING_SUPPLY));
                    final var timestamp = quote.get(TIMESTAMP);

                    if (symbol == null || timeOpen == null || timeClose == null || timeHigh == null ||
                            timeLow == null || open == null || high == null || low == null || close == null ||
                            volume == null || marketCap == null || circulatingSupply == null || timestamp == null) {
                        continue;
                    }

                    ps.setString(CMC_KLINE_SYMBOL, symbol);
                    ps.setObject(CMC_KLINE_TIME_OPEN, toOdt(timeOpen));
                    ps.setObject(CMC_KLINE_TIME_CLOSE, toOdt(timeClose));
                    ps.setObject(CMC_KLINE_TIME_HIGH, toOdt(timeHigh));
                    ps.setObject(CMC_KLINE_TIME_LOW, toOdt(timeLow));
                    ps.setBigDecimal(CMC_KLINE_OPEN, open);
                    ps.setBigDecimal(CMC_KLINE_HIGH, high);
                    ps.setBigDecimal(CMC_KLINE_LOW, low);
                    ps.setBigDecimal(CMC_KLINE_CLOSE, close);
                    ps.setBigDecimal(CMC_KLINE_VOLUME, volume);
                    ps.setBigDecimal(CMC_KLINE_MARKET_CAP, marketCap);
                    ps.setBigDecimal(CMC_KLINE_CIRCULATING_SUPPLY, circulatingSupply);
                    ps.setObject(CMC_KLINE_TIMESTAMP, toOdt(timestamp));

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

    public List<Map<String, Object>> getKline1w(final String symbol, final OffsetDateTime from, final OffsetDateTime to)
            throws SQLException {
        return fetchRangeBySymbol(dataSource, CMC_KLINE_1W_SELECT_BY_SYMBOL, symbol, from, to,
                SYMBOL, TIME_OPEN, TIME_CLOSE, TIME_HIGH, TIME_LOW,
                OPEN, HIGH, LOW, CLOSE, VOLUME, MARKET_CAP2, CIRCULATING_SUPPLY, TIMESTAMP);
    }
}
