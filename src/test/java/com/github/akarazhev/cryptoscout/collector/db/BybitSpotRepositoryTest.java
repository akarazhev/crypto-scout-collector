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
import com.github.akarazhev.cryptoscout.test.BybitMockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import io.activej.eventloop.Eventloop;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_TICKERS_TABLE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.A;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.B;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BybitSpotRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitSpotRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitSpotRepository.create(reactor, dataSource);
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> dataSource.stop().whenComplete(() -> reactor.breakEventloop()));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void shouldSaveKline1m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_1);
        assertEquals(1, repository.saveKline1m(List.of(data), 100L));
        assertTableCount(BYBIT_SPOT_KLINE_1M_TABLE, 1);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_5);
        assertEquals(1, repository.saveKline5m(List.of(data), 200L));
        assertTableCount(BYBIT_SPOT_KLINE_5M_TABLE, 1);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_15);
        assertEquals(1, repository.saveKline15m(List.of(data), 300L));
        assertTableCount(BYBIT_SPOT_KLINE_15M_TABLE, 1);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_60);
        assertEquals(1, repository.saveKline60m(List.of(data), 400L));
        assertTableCount(BYBIT_SPOT_KLINE_60M_TABLE, 1);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_240);
        assertEquals(1, repository.saveKline240m(List.of(data), 500L));
        assertTableCount(BYBIT_SPOT_KLINE_240M_TABLE, 1);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(data), 600L));
        assertTableCount(BYBIT_SPOT_KLINE_1D_TABLE, 1);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.TICKERS);
        assertEquals(1, repository.saveTicker(List.of(data), 700L));
        assertTableCount(BYBIT_SPOT_TICKERS_TABLE, 1);
    }

    @Test
    void shouldSavePublicTrade() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.PUBLIC_TRADE);
        final var expected = getPublicTradeCount(data);
        assertEquals(expected, repository.savePublicTrade(List.of(data), 800L));
        assertTableCount(BYBIT_SPOT_PUBLIC_TRADE_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook1(List.of(data), 900L));
        assertTableCount(BYBIT_SPOT_ORDER_BOOK_1_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook50() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook50(List.of(data), 1000L));
        assertTableCount(BYBIT_SPOT_ORDER_BOOK_50_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook200() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook200(List.of(data), 1100L));
        assertTableCount(BYBIT_SPOT_ORDER_BOOK_200_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1000() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook1000(List.of(data), 1200L));
        assertTableCount(BYBIT_SPOT_ORDER_BOOK_1000_TABLE, expected);
    }

    private int getPublicTradeCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var rows = (List<Map<String, Object>>) payload.get(DATA);
        return rows == null ? 0 : rows.size();
    }

    private int getOrderBookLevelsCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var row = (Map<String, Object>) payload.get(DATA);
        if (row == null) {
            return 0;
        }

        @SuppressWarnings("unchecked") final var bids = (List<List<String>>) row.get(B);
        final var b = bids == null ? 0 : bids.size();
        @SuppressWarnings("unchecked") final var asks = (List<List<String>>) row.get(A);
        final var a = asks == null ? 0 : asks.size();
        assertTrue(b > 0 || a > 0, "Orderbook should contain bids or asks");
        return b + a;
    }

    private void assertTableCount(final String table, final long expected) throws SQLException {
        try (final var c = DriverManager.getConnection(JdbcConfig.getUrl(), JdbcConfig.getUsername(), JdbcConfig.getPassword());
             final var rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1), "Unexpected row count for table: " + table);
        }
    }
}
