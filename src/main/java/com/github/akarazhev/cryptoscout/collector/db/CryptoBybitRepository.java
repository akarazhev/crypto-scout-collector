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
import java.sql.Types;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_CROSS_SEQUENCE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_HIGH_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_INSERT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LAST_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_LOW_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PREV_PRICE_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_PRICE_24H_PCNT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TIMESTAMP;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TURNOVER_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_USD_INDEX_PRICE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_VOLUME_24H;
import static com.github.akarazhev.cryptoscout.collector.db.Utils.toBigDecimal;
import static com.github.akarazhev.cryptoscout.collector.db.Utils.toOffsetDateTime;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.HIGH_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LAST_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.LOW_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PREV_PRICE_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.PRICE_24H_PCNT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TURNOVER_24H;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.USD_INDEX_PRICE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.VOLUME_24H;

public final class CryptoBybitRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;
    private final int batchSize;

    public static CryptoBybitRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new CryptoBybitRepository(reactor, collectorDataSource);
    }

    private CryptoBybitRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
        this.batchSize = JdbcConfig.getBybitBatchSize();
    }

    @Override
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public int insertSpotTickers(final Iterable<Map<String, Object>> rows) throws Exception {
        int count = 0;
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(String.format(SPOT_TICKERS_INSERT))) {
            for (final var row : rows) {
                final var dObj = row.get(DATA);
                if (!(dObj instanceof Map<?, ?> map)) {
                    // skip malformed rows
                    continue;
                }

                final var odt = toOffsetDateTime((Long) row.get(TS));
                if (odt != null) {
                    ps.setObject(SPOT_TICKERS_TIMESTAMP, odt);
                } else {
                    ps.setNull(SPOT_TICKERS_TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE);
                }

                final var cs = (Long) row.get(CS);
                if (cs != null) {
                    ps.setObject(SPOT_TICKERS_CROSS_SEQUENCE, cs);
                } else {
                    ps.setNull(SPOT_TICKERS_CROSS_SEQUENCE, Types.BIGINT);
                }

                @SuppressWarnings("unchecked") final var d = (Map<String, Object>) map;
                ps.setString(SPOT_TICKERS_SYMBOL, (String) d.get(SYMBOL));
                ps.setBigDecimal(SPOT_TICKERS_LAST_PRICE, toBigDecimal(d.get(LAST_PRICE)));
                ps.setBigDecimal(SPOT_TICKERS_HIGH_PRICE_24H, toBigDecimal(d.get(HIGH_PRICE_24H)));
                ps.setBigDecimal(SPOT_TICKERS_LOW_PRICE_24H, toBigDecimal(d.get(LOW_PRICE_24H)));
                ps.setBigDecimal(SPOT_TICKERS_PREV_PRICE_24H, toBigDecimal(d.get(PREV_PRICE_24H)));
                ps.setBigDecimal(SPOT_TICKERS_VOLUME_24H, toBigDecimal(d.get(VOLUME_24H)));
                ps.setBigDecimal(SPOT_TICKERS_TURNOVER_24H, toBigDecimal(d.get(TURNOVER_24H)));
                ps.setBigDecimal(SPOT_TICKERS_PRICE_24H_PCNT, toBigDecimal(d.get(PRICE_24H_PCNT)));
                // may be null
                final var usd = toBigDecimal(d.get(USD_INDEX_PRICE));
                if (usd != null) {
                    ps.setBigDecimal(SPOT_TICKERS_USD_INDEX_PRICE, usd);
                } else {
                    ps.setNull(SPOT_TICKERS_USD_INDEX_PRICE, Types.NUMERIC);
                }

                ps.addBatch();
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }

            ps.executeBatch();
        }

        return count;
    }
}
