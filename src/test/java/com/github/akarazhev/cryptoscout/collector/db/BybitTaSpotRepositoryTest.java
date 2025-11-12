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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getOrderBookLevelsCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getRowsCount;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BybitTaSpotRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitTaSpotRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitTaSpotRepository.create(reactor, dataSource);
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                TA_SPOT_ORDER_BOOK_1_TABLE,
                TA_SPOT_ORDER_BOOK_50_TABLE,
                TA_SPOT_ORDER_BOOK_200_TABLE,
                TA_SPOT_ORDER_BOOK_1000_TABLE,
                TA_SPOT_PUBLIC_TRADE_TABLE
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
    void shouldSavePublicTrade() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.PUBLIC_TRADE);
        final var expected = getRowsCount(data);
        assertEquals(expected, repository.savePublicTrade(List.of(data), 100L));
        assertTableCount(TA_SPOT_PUBLIC_TRADE_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1);
        final var expected = getOrderBookLevelsCount(data);
        assertTrue(expected > 0);
        assertEquals(expected, repository.saveOrderBook1(List.of(data), 200L));
        assertTableCount(TA_SPOT_ORDER_BOOK_1_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook50() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_50);
        final var expected = getOrderBookLevelsCount(data);
        assertTrue(expected > 0);
        assertEquals(expected, repository.saveOrderBook50(List.of(data), 300L));
        assertTableCount(TA_SPOT_ORDER_BOOK_50_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook200() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_200);
        final var expected = getOrderBookLevelsCount(data);
        assertTrue(expected > 0);
        assertEquals(expected, repository.saveOrderBook200(List.of(data), 400L));
        assertTableCount(TA_SPOT_ORDER_BOOK_200_TABLE, expected);
    }

    @Test
    void shouldSaveOrderBook1000() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1000);
        final var expected = getOrderBookLevelsCount(data);
        assertTrue(expected > 0);
        assertEquals(expected, repository.saveOrderBook1000(List.of(data), 500L));
        assertTableCount(TA_SPOT_ORDER_BOOK_1000_TABLE, expected);
    }
}
