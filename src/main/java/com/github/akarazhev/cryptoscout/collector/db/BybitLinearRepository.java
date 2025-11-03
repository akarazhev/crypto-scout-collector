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
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.LAST_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM;

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
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public int saveKline1m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;

    }

    public int saveKline5m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;
    }

    public int saveKline15m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;
    }

    public int saveKline60m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;
    }

    public int saveKline240m(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;
    }

    public int saveKline1d(final Iterable<Map<String, Object>> klines, final long offset) throws SQLException {
        return 0;
    }

    public int saveTicker(final Iterable<Map<String, Object>> tickers, final long offset) throws SQLException {
        return 0;
    }

    public int savePublicTrade(final Iterable<Map<String, Object>> trades, final long offset) throws SQLException {
        return 0;

    }

    public int saveOrderBook1(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return 0;
    }

    public int saveOrderBook50(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return 0;
    }

    public int saveOrderBook200(final Iterable<Map<String, Object>> orderBooks, final long offset) throws SQLException {
        return 0;
    }

    public int saveOrderBook1000(final Iterable<Map<String, Object>> orderBooks, final long offset)
            throws SQLException {
        return 0;
    }

    public int saveAllLiquidation(final Iterable<Map<String, Object>> allLiquidations, final long offset)
            throws SQLException {
        return 0;
    }

    private void updateOffset(final PreparedStatement ps, final long offset) throws SQLException {
        ps.setString(STREAM, stream);
        ps.setLong(LAST_OFFSET, offset);
        ps.executeUpdate();
    }
}
