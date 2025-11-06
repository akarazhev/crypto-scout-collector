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

import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import io.activej.eventloop.Eventloop;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_ALL_LIQUIDATION_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LINEAR_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getOrderBookLevelsCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getPublicTradeCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitLinearRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitLinearRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitLinearRepository.create(reactor, dataSource);
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
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        assertEquals(1, repository.saveKline1m(List.of(data), 100L));
        assertTableCount(BYBIT_LINEAR_KLINE_1M_TABLE, 1);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        assertEquals(1, repository.saveKline5m(List.of(data), 200L));
        assertTableCount(BYBIT_LINEAR_KLINE_5M_TABLE, 1);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        assertEquals(1, repository.saveKline15m(List.of(data), 300L));
        assertTableCount(BYBIT_LINEAR_KLINE_15M_TABLE, 1);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        assertEquals(1, repository.saveKline60m(List.of(data), 400L));
        assertTableCount(BYBIT_LINEAR_KLINE_60M_TABLE, 1);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        assertEquals(1, repository.saveKline240m(List.of(data), 500L));
        assertTableCount(BYBIT_LINEAR_KLINE_240M_TABLE, 1);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(data), 600L));
        assertTableCount(BYBIT_LINEAR_KLINE_1D_TABLE, 1);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        assertEquals(1, repository.saveTicker(List.of(data), 700L));
        assertTableCount(BYBIT_LINEAR_TICKERS_TABLE, 1);
    }

    @Test
    void shouldSavePublicTrade() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var expected = getPublicTradeCount(data);
        assertEquals(expected, repository.savePublicTrade(List.of(data), 800L));
        assertTableCount(BYBIT_LINEAR_PUBLIC_TRADE_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook1(List.of(data), 900L));
        assertTableCount(BYBIT_LINEAR_ORDER_BOOK_1_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook50() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook50(List.of(data), 1000L));
        assertTableCount(BYBIT_LINEAR_ORDER_BOOK_50_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook200() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook200(List.of(data), 1100L));
        assertTableCount(BYBIT_LINEAR_ORDER_BOOK_200_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1000() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(data);
        assertEquals(expected, repository.saveOrderBook1000(List.of(data), 1200L));
        assertTableCount(BYBIT_LINEAR_ORDER_BOOK_1000_TABLE, expected);
    }

    @Test
    void shouldSaveAllLiquidation() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);
        assertEquals(1, repository.saveAllLiquidation(List.of(data), 1300L));
        assertTableCount(BYBIT_LINEAR_ALL_LIQUIDATION_TABLE, 1);
    }
}
