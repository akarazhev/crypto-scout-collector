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

import com.github.akarazhev.cryptoscout.test.DBUtils;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import io.activej.eventloop.Eventloop;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getOrderBookLevelsCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getRowsCount;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Symbol.BTC_USDT;
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
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitLinearRepository.create(reactor, dataSource);
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                LINEAR_KLINE_1M_TABLE,
                LINEAR_KLINE_5M_TABLE,
                LINEAR_KLINE_15M_TABLE,
                LINEAR_KLINE_60M_TABLE,
                LINEAR_KLINE_240M_TABLE,
                LINEAR_KLINE_1D_TABLE,
                LINEAR_TICKERS_TABLE,
                LINEAR_ORDER_BOOK_1_TABLE,
                LINEAR_ORDER_BOOK_50_TABLE,
                LINEAR_ORDER_BOOK_200_TABLE,
                LINEAR_ORDER_BOOK_1000_TABLE,
                LINEAR_PUBLIC_TRADE_TABLE,
                LINEAR_ALL_LIQUIDATION_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> dataSource.stop()
                .whenComplete(() -> reactor.breakEventloop()));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void shouldSaveKline1m() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var expected = getRowsCount(k1);
        assertEquals(expected, repository.saveKline1m(List.of(k1), 100L));
        assertTableCount(LINEAR_KLINE_1M_TABLE, expected);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var expected = getRowsCount(k5);
        assertEquals(expected, repository.saveKline5m(List.of(k5), 200L));
        assertTableCount(LINEAR_KLINE_5M_TABLE, expected);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var expected = getRowsCount(k15);
        assertEquals(expected, repository.saveKline15m(List.of(k15), 300L));
        assertTableCount(LINEAR_KLINE_15M_TABLE, expected);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var expected = getRowsCount(k60);
        assertEquals(expected, repository.saveKline60m(List.of(k60), 400L));
        assertTableCount(LINEAR_KLINE_60M_TABLE, expected);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var expected = getRowsCount(k240);
        assertEquals(expected, repository.saveKline240m(List.of(k240), 500L));
        assertTableCount(LINEAR_KLINE_240M_TABLE, expected);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var expected = getRowsCount(kd);
        assertEquals(expected, repository.saveKline1d(List.of(kd), 600L));
        assertTableCount(LINEAR_KLINE_1D_TABLE, expected);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        final var expected = getRowsCount(tickers);
        assertEquals(expected, repository.saveTicker(List.of(tickers), 700L));
        assertTableCount(LINEAR_TICKERS_TABLE, expected);
    }

    @Test
    void shouldGetKline1m() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var expected = getRowsCount(k1);
        final var start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline1m(List.of(k1), 800L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(expected, repository.getKline1m(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline5m() throws Exception {
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var expected = getRowsCount(k5);
        final var start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline5m(List.of(k5), 900L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(expected, repository.getKline5m(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline15m() throws Exception {
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var expected = getRowsCount(k15);
        final var start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline15m(List.of(k15), 1000L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(expected, repository.getKline15m(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline60m() throws Exception {
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var expected = getRowsCount(k60);
        final var start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline60m(List.of(k60), 1100L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(expected, repository.getKline60m(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline240m() throws Exception {
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var expected = getRowsCount(k240);
        final var start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline240m(List.of(k240), 1200L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline240m(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline1d() throws Exception {
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var expected = getRowsCount(kd);
        final var start = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        assertEquals(expected, repository.saveKline1d(List.of(kd), 1300L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(expected, repository.getKline1d(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetTicker() throws Exception {
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        final var expected = getRowsCount(tickers);
        assertEquals(expected, repository.saveTicker(List.of(tickers), 1400L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(expected, repository.getTicker(BTC_USDT, from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldSavePublicTrade() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var expected = getRowsCount(pt);
        assertEquals(expected, repository.savePublicTrade(List.of(pt), 1500L));
        assertTableCount(LINEAR_PUBLIC_TRADE_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1() throws Exception {
        final var ob1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(ob1);
        assertEquals(expected, repository.saveOrderBook1(List.of(ob1), 1600L));
        assertTableCount(LINEAR_ORDER_BOOK_1_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook50() throws Exception {
        final var ob50 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(ob50);
        assertEquals(expected, repository.saveOrderBook50(List.of(ob50), 1700L));
        assertTableCount(LINEAR_ORDER_BOOK_50_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook200() throws Exception {
        final var ob200 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(ob200);
        assertEquals(expected, repository.saveOrderBook200(List.of(ob200), 1800L));
        assertTableCount(LINEAR_ORDER_BOOK_200_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1000() throws Exception {
        final var ob1000 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(ob1000);
        assertEquals(expected, repository.saveOrderBook1000(List.of(ob1000), 1900L));
        assertTableCount(LINEAR_ORDER_BOOK_1000_TABLE, expected);
    }

    @Test
    void shouldSaveAllLiquidation() throws Exception {
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);
        final var expected = getOrderBookLevelsCount(al);
        assertEquals(expected, repository.saveAllLiquidation(List.of(al), 2000L));
        assertTableCount(LINEAR_ALL_LIQUIDATION_TABLE, expected);
    }

    @Test
    void shouldGetPublicTrade() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var expected = getRowsCount(pt);
        assertEquals(expected, repository.savePublicTrade(List.of(pt), 2100L));
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(expected, repository.getPublicTrade(BTC_USDT, from, from).size());
    }

    @Test
    void shouldGetOrderBook1() throws Exception {
        final var ob1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(ob1);
        assertEquals(expected, repository.saveOrderBook1(List.of(ob1), 2200L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        assertEquals(expected, repository.getOrderBook1(BTC_USDT, from, from).size());
    }

    @Test
    void shouldGetOrderBook50() throws Exception {
        final var ob50 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(ob50);
        assertEquals(expected, repository.saveOrderBook50(List.of(ob50), 2300L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        assertEquals(expected, repository.getOrderBook50(BTC_USDT, from, from).size());
    }

    @Test
    void shouldGetOrderBook200() throws Exception {
        final var ob200 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(ob200);
        assertEquals(expected, repository.saveOrderBook200(List.of(ob200), 2400L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        assertEquals(expected, repository.getOrderBook200(BTC_USDT, from, from).size());
    }

    @Test
    void shouldGetOrderBook1000() throws Exception {
        final var ob1000 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(ob1000);
        assertEquals(expected, repository.saveOrderBook1000(List.of(ob1000), 2500L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);
        assertEquals(expected, repository.getOrderBook1000(BTC_USDT, from, from).size());
    }

    @Test
    void shouldGetAllLiquidation() throws Exception {
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);
        final var expected = getRowsCount(al);
        assertEquals(expected, repository.saveAllLiquidation(List.of(al), 2600L));
        final var t = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(expected, repository.getAllLiquidation(BTC_USDT, from, from).size());
    }
}
