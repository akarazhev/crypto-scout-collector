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

import com.github.akarazhev.cryptoscout.test.BybitMockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import io.activej.eventloop.Eventloop;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        // Ensure JDBC settings match test DB compose
        System.setProperty("jdbc.datasource.url", "jdbc:postgresql://localhost:5432/crypto_scout");
        System.setProperty("jdbc.datasource.username", "crypto_scout_db");
        System.setProperty("jdbc.datasource.password", "crypto_scout_db");

        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitSpotRepository.create(reactor, dataSource);
    }

    @AfterAll
    static void cleanup() {
        PodmanCompose.down();
        reactor.post(() -> dataSource.stop().whenComplete(() -> reactor.breakEventloop()));
        reactor.run();
        executor.shutdown();
    }

    @Test
    void shouldSaveKline1m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_1);
        final var count = repository.saveKline1m(List.of(data), 100L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_1m", 1);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_5);
        final var count = repository.saveKline5m(List.of(data), 200L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_5m", 1);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_15);
        final var count = repository.saveKline15m(List.of(data), 300L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_15m", 1);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_60);
        final var count = repository.saveKline60m(List.of(data), 400L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_60m", 1);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_240);
        final var count = repository.saveKline240m(List.of(data), 500L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_240m", 1);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.KLINE_D);
        final var count = repository.saveKline1d(List.of(data), 600L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_kline_1d", 1);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.TICKERS);
        final var count = repository.saveTicker(List.of(data), 700L);
        assertEquals(1, count);
        assertTableCount("crypto_scout.bybit_spot_tickers", 1);
    }

    @Test
    void shouldSavePublicTrade() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.PUBLIC_TRADE);
        final var expected = getPublicTradeCount(data);
        final var count = repository.savePublicTrade(List.of(data), 800L);
        assertEquals(expected, count);
        assertTableCount("crypto_scout.bybit_spot_public_trade", expected);
    }

    @Test
    void shouldSaveOrderBook1() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(data);
        final var count = repository.saveOrderBook1(List.of(data), 900L);
        assertEquals(expected, count);
        assertTableCount("crypto_scout.bybit_spot_order_book_1", expected);
    }

    @Test
    void shouldSaveOrderBook50() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(data);
        final var count = repository.saveOrderBook50(List.of(data), 1000L);
        assertEquals(expected, count);
        assertTableCount("crypto_scout.bybit_spot_order_book_50", expected);
    }

    @Test
    void shouldSaveOrderBook200() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(data);
        final var count = repository.saveOrderBook200(List.of(data), 1100L);
        assertEquals(expected, count);
        assertTableCount("crypto_scout.bybit_spot_order_book_200", expected);
    }

    @Test
    void shouldSaveOrderBook1000() throws Exception {
        final var data = BybitMockData.get(BybitMockData.Source.SPOT, BybitMockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(data);
        final var count = repository.saveOrderBook1000(List.of(data), 1200L);
        assertEquals(expected, count);
        assertTableCount("crypto_scout.bybit_spot_order_book_1000", expected);
    }

    private int getPublicTradeCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var rows = (List<Map<String, Object>>) payload.get("data");
        return rows == null ? 0 : rows.size();
    }

    private int getOrderBookLevelsCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var row = (Map<String, Object>) payload.get("data");
        if (row == null) return 0;
        @SuppressWarnings("unchecked") final var bids = (List<List<String>>) row.get("b");
        final var b = bids == null ? 0 : bids.size();
        @SuppressWarnings("unchecked") final var asks = (List<List<String>>) row.get("b");
        final var a = asks == null ? 0 : asks.size();
        assertTrue(b > 0 || a > 0, "Orderbook should contain bids or asks");
        return b + a;
    }

    private void assertTableCount(final String table, final long expected) throws Exception {
        final var url = System.getProperty("jdbc.datasource.url", "jdbc:postgresql://localhost:5432/crypto_scout");
        final var user = System.getProperty("jdbc.datasource.username", "crypto_scout_db");
        final var pass = System.getProperty("jdbc.datasource.password", "crypto_scout_db");
        try (final var c = DriverManager.getConnection(url, user, pass);
             final var st = c.createStatement();
             final var rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            final var actual = rs.getLong(1);
            assertEquals(expected, actual, "Unexpected row count for table: " + table);
        }
    }
}
