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

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_DESC;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RETURN_COIN;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RETURN_COIN_ICON;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RULES;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_STAKE_BEGIN_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_STAKE_END_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_TRADE_BEGIN_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_WEBSITE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_WHITE_PAPER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.LAST_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Utils.toOffsetDateTime;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DESC;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RETURN_COIN;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RETURN_COIN_ICON;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RULES;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_END_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TRADE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.WEBSITE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.WHITE_PAPER;

public final class MetricsBybitRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static MetricsBybitRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new MetricsBybitRepository(reactor, collectorDataSource);
    }

    private MetricsBybitRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.stream = AmqpConfig.getAmqpMetricsBybitStream();
    }

    @Override
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public int insertLpl(final Iterable<Map<String, Object>> lpls, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final boolean oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(LPL_INSERT);
                 final var psOffset = c.prepareStatement(UPSERT)) {
                for (final var lpl : lpls) {
                    ps.setString(LPL_RETURN_COIN, (String) lpl.get(RETURN_COIN));
                    ps.setString(LPL_RETURN_COIN_ICON, (String) lpl.get(RETURN_COIN_ICON));
                    ps.setString(LPL_DESC, (String) lpl.get(DESC));
                    ps.setString(LPL_WEBSITE, (String) lpl.get(WEBSITE));
                    ps.setString(LPL_WHITE_PAPER, (String) lpl.get(WHITE_PAPER));
                    ps.setString(LPL_RULES, (String) lpl.get(RULES));
                    ps.setObject(LPL_STAKE_BEGIN_TIME, toOffsetDateTime((Long) lpl.get(STAKE_BEGIN_TIME)));
                    ps.setObject(LPL_STAKE_END_TIME, toOffsetDateTime((Long) lpl.get(STAKE_END_TIME)));
                    final var tradeBegin = (Long) lpl.get(TRADE_BEGIN_TIME);
                    if (tradeBegin != null) {
                        ps.setObject(LPL_TRADE_BEGIN_TIME, toOffsetDateTime(tradeBegin));
                    } else {
                        ps.setNull(LPL_TRADE_BEGIN_TIME, Types.TIMESTAMP_WITH_TIMEZONE);
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

    private void updateOffset(final PreparedStatement psOffset, final long offset) throws SQLException {
        psOffset.setString(STREAM, stream);
        psOffset.setLong(LAST_OFFSET, offset);
        psOffset.executeUpdate();
    }
}
