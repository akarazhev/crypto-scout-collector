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

import static com.github.akarazhev.cryptoscout.collector.db.Constants.CmcKline1wIndicators.*;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;

public final class AnalystRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static AnalystRepository create(final NioReactor reactor,
                                           final CollectorDataSource collectorDataSource) {
        return new AnalystRepository(reactor, collectorDataSource);
    }

    private AnalystRepository(final NioReactor reactor,
                              final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getAnalystBatchSize();
        this.stream = "analyst-cmc-kline-1w";
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    /**
     * Load recent indicators for EMA initialization.
     * Returns data in descending timestamp order (newest first).
     */
    public List<Map<String, Object>> getRecentIndicators(final String symbol, final int limit)
            throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(INDICATORS_SELECT_RECENT)) {
            ps.setString(1, symbol);
            ps.setInt(2, limit);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put("symbol", rs.getString("symbol"));
                    row.put("timestamp", rs.getObject("timestamp", OffsetDateTime.class));
                    row.put("close_price", rs.getDouble("close_price"));
                    row.put("sma_50", rs.getObject("sma_50"));
                    row.put("sma_100", rs.getObject("sma_100"));
                    row.put("sma_200", rs.getObject("sma_200"));
                    row.put("ema_50", rs.getObject("ema_50"));
                    row.put("ema_100", rs.getObject("ema_100"));
                    row.put("ema_200", rs.getObject("ema_200"));
                    results.add(row);
                }
            }
        }
        return results;
    }

    /**
     * Load recent klines from base cmc_kline_1w table for initialization.
     * Returns data in descending timestamp order (newest first).
     */
    public List<Map<String, Object>> getRecentKlines(final String symbol, final int limit)
            throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(KLINE_1W_SELECT_RECENT)) {
            ps.setString(1, symbol);
            ps.setInt(2, limit);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put("symbol", rs.getString("symbol"));
                    row.put("timestamp", rs.getObject("timestamp", OffsetDateTime.class));
                    row.put("close", rs.getDouble("close"));
                    row.put("open", rs.getDouble("open"));
                    row.put("high", rs.getDouble("high"));
                    row.put("low", rs.getDouble("low"));
                    row.put("volume", rs.getDouble("volume"));
                    results.add(row);
                }
            }
        }
        return results;
    }

    /**
     * Save indicators with batching and offset tracking.
     */
    public int saveIndicators(final List<Map<String, Object>> indicators, final long offset)
            throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final var oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(INDICATORS_UPSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {

                for (final var ind : indicators) {
                    ps.setString(IND_SYMBOL, (String) ind.get("symbol"));
                    ps.setObject(IND_TIMESTAMP, ind.get("timestamp"));
                    ps.setDouble(IND_CLOSE_PRICE, ((Number) ind.get("close_price")).doubleValue());
                    setNullableDouble(ps, IND_SMA_50, ind.get("sma_50"));
                    setNullableDouble(ps, IND_SMA_100, ind.get("sma_100"));
                    setNullableDouble(ps, IND_SMA_200, ind.get("sma_200"));
                    setNullableDouble(ps, IND_EMA_50, ind.get("ema_50"));
                    setNullableDouble(ps, IND_EMA_100, ind.get("ema_100"));
                    setNullableDouble(ps, IND_EMA_200, ind.get("ema_200"));

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

    private void setNullableDouble(final java.sql.PreparedStatement ps, final int index,
                                    final Object value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else if (value instanceof Double) {
            final var d = (Double) value;
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
