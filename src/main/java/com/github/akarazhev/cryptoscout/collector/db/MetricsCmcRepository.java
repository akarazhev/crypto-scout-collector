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

import com.github.akarazhev.cryptoscout.config.JdbcConfig;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_BTC_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_BTC_VOLUME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_NAME;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_SCORE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.FGI_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Utils.toBigDecimal;
import static com.github.akarazhev.cryptoscout.collector.db.Utils.toOffsetDateTimeFromSeconds;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.BTC_PRICE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.BTC_VOLUME;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.DATA_LIST;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.NAME;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SCORE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;

public final class MetricsCmcRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;

    public static MetricsCmcRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new MetricsCmcRepository(reactor, collectorDataSource);
    }

    private MetricsCmcRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getCmcBatchSize();
    }

    @Override
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public int insertFgi(final List<Map<String, Object>> fgis) throws SQLException {
        var count = 0;
        try (final var c = dataSource.getConnection(); final var ps = c.prepareStatement(FGI_INSERT)) {
            for (final var fgi : fgis) {
                if (fgi != null && fgi.containsKey(DATA_LIST)) {
                    for (final var dl : (List<Map<String, Object>>) fgi.get(DATA_LIST)) {
                        final var score = dl.get(SCORE);
                        if (score instanceof Number n) {
                            ps.setInt(FGI_SCORE, n.intValue());
                        } else if (score instanceof String s) {
                            ps.setInt(FGI_SCORE, Integer.parseInt(s));
                        } else {
                            ps.setNull(FGI_SCORE, Types.INTEGER);
                        }

                        ps.setString(FGI_NAME, (String) dl.get(NAME));
                        final var ts = (String) dl.get(TIMESTAMP);
                        ps.setObject(FGI_TIMESTAMP, toOffsetDateTimeFromSeconds(ts != null ? Long.parseLong(ts) : 0L));
                        ps.setBigDecimal(FGI_BTC_PRICE, toBigDecimal(dl.get(BTC_PRICE)));
                        ps.setBigDecimal(FGI_BTC_VOLUME, toBigDecimal(dl.get(BTC_VOLUME)));

                        ps.addBatch();
                        if (++count % batchSize == 0) {
                            ps.executeBatch();
                        }
                    }
                }
            }

            ps.executeBatch();
        }

        return count;
    }
}
