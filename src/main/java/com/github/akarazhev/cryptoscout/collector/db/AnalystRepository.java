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

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.KLINE_1W_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.EMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.INDICATORS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.INDICATORS_SELECT_BY_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_CLOSE_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_EMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.IND_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_100;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_200;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.SMA_50;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.LIMIT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.CLOSE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.HIGH;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.LOW;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.OPEN;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.VOLUME;

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

    public List<Map<String, Object>> getIndicators(final String symbol, final int limit) throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(INDICATORS_SELECT_BY_SYMBOL)) {
            ps.setString(SYMBOL, symbol);
            ps.setInt(LIMIT, limit);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put(SYMBOL, rs.getString(SYMBOL));
                    row.put(TIMESTAMP, rs.getObject(TIMESTAMP, OffsetDateTime.class));
                    row.put(CLOSE_PRICE, rs.getDouble(CLOSE_PRICE));
                    row.put(SMA_50, rs.getObject(SMA_50));
                    row.put(SMA_100, rs.getObject(SMA_100));
                    row.put(SMA_200, rs.getObject(SMA_200));
                    row.put(EMA_50, rs.getObject(EMA_50));
                    row.put(EMA_100, rs.getObject(EMA_100));
                    row.put(EMA_200, rs.getObject(EMA_200));
                    results.add(row);
                }
            }
        }
        return results;
    }

    public List<Map<String, Object>> getKlines(final String symbol, final int limit) throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(KLINE_1W_SELECT_BY_SYMBOL)) {
            ps.setString(SYMBOL, symbol);
            ps.setInt(LIMIT, limit);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put(SYMBOL, rs.getString(SYMBOL));
                    row.put(TIMESTAMP, rs.getObject(TIMESTAMP, OffsetDateTime.class));
                    row.put(CLOSE, rs.getDouble(CLOSE));
                    row.put(OPEN, rs.getDouble(OPEN));
                    row.put(HIGH, rs.getDouble(HIGH));
                    row.put(LOW, rs.getDouble(LOW));
                    row.put(VOLUME, rs.getDouble(VOLUME));
                    results.add(row);
                }
            }
        }
        return results;
    }

    public int saveIndicators(final List<Map<String, Object>> indicators, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(INDICATORS_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var indicator : indicators) {
                    ps.setString(IND_SYMBOL, (String) indicator.get(SYMBOL));
                    ps.setObject(IND_TIMESTAMP, indicator.get(TIMESTAMP));
                    ps.setDouble(IND_CLOSE_PRICE, ((Number) indicator.get(CLOSE_PRICE)).doubleValue());
                    setNullableDouble(ps, IND_SMA_50, indicator.get(SMA_50));
                    setNullableDouble(ps, IND_SMA_100, indicator.get(SMA_100));
                    setNullableDouble(ps, IND_SMA_200, indicator.get(SMA_200));
                    setNullableDouble(ps, IND_EMA_50, indicator.get(EMA_50));
                    setNullableDouble(ps, IND_EMA_100, indicator.get(EMA_100));
                    setNullableDouble(ps, IND_EMA_200, indicator.get(EMA_200));

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

    private void setNullableDouble(final java.sql.PreparedStatement ps, final int index, final Object value)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else if (value instanceof Double d) {
            if (Double.isNaN(d)) {
                ps.setNull(index, Types.DOUBLE);
            } else {
                ps.setDouble(index, d);
            }
        } else if (value instanceof Number) {
            ps.setDouble(index, ((Number) value).doubleValue());
        } else {
            ps.setNull(index, Types.DOUBLE);
        }
    }
}
