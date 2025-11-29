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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.FROM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_DESC;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RETURN_COIN;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RETURN_COIN_ICON;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_RULES;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_STAKE_BEGIN_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_STAKE_END_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_TRADE_BEGIN_TIME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_WEBSITE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LPL_WHITE_PAPER;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.TO;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_UPSERT;
import static com.github.akarazhev.cryptoscout.collector.db.DBUtils.updateOffset;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DESC;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RETURN_COIN;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RETURN_COIN_ICON;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.RULES;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_END_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TRADE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.WEBSITE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.WHITE_PAPER;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;

public final class BybitParserRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;
    private final String stream;

    public static BybitParserRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new BybitParserRepository(reactor, collectorDataSource);
    }

    private BybitParserRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
        this.stream = AmqpConfig.getAmqpBybitParserStream();
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }

    public int saveLpl(final List<Map<String, Object>> lpls, final long offset) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection()) {
            final boolean oldAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try (final var ps = c.prepareStatement(LPL_INSERT);
                 final var psOffset = c.prepareStatement(STREAM_OFFSETS_UPSERT)) {
                for (final var lpl : lpls) {
                    ps.setString(LPL_RETURN_COIN, (String) lpl.get(RETURN_COIN));
                    ps.setString(LPL_RETURN_COIN_ICON, (String) lpl.get(RETURN_COIN_ICON));
                    ps.setString(LPL_DESC, (String) lpl.get(DESC));
                    ps.setString(LPL_WEBSITE, (String) lpl.get(WEBSITE));
                    ps.setString(LPL_WHITE_PAPER, (String) lpl.get(WHITE_PAPER));
                    ps.setString(LPL_RULES, (String) lpl.get(RULES));
                    ps.setObject(LPL_STAKE_BEGIN_TIME, toOdt((Long) lpl.get(STAKE_BEGIN_TIME)));
                    ps.setObject(LPL_STAKE_END_TIME, toOdt((Long) lpl.get(STAKE_END_TIME)));
                    final var tradeBegin = (Long) lpl.get(TRADE_BEGIN_TIME);
                    if (tradeBegin != null) {
                        ps.setObject(LPL_TRADE_BEGIN_TIME, toOdt(tradeBegin));
                    } else {
                        ps.setNull(LPL_TRADE_BEGIN_TIME, Types.TIMESTAMP_WITH_TIMEZONE);
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

    public List<Map<String, Object>> getLpl(final OffsetDateTime odt) throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection()) {
            final var ps = c.prepareStatement(LPL_SELECT);
            ps.setObject(FROM, odt);
            ps.setObject(TO, odt);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    row.put(RETURN_COIN, rs.getObject(LPL_RETURN_COIN));
                    row.put(RETURN_COIN_ICON, rs.getObject(LPL_RETURN_COIN_ICON));
                    row.put(DESC, rs.getObject(LPL_DESC));
                    row.put(WEBSITE, rs.getObject(LPL_WEBSITE));
                    row.put(WHITE_PAPER, rs.getObject(LPL_WHITE_PAPER));
                    row.put(RULES, rs.getObject(LPL_RULES));
                    row.put(STAKE_BEGIN_TIME, rs.getObject(LPL_STAKE_BEGIN_TIME));
                    row.put(STAKE_END_TIME, rs.getObject(LPL_STAKE_END_TIME));
                    row.put(TRADE_BEGIN_TIME, rs.getObject(LPL_TRADE_BEGIN_TIME));
                    results.add(row);
                }
            }
        }

        return results;
    }
}
