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

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_SPOT_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitSpotRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitSpotRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitSpotRepository.create(reactor, dataSource);
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                BYBIT_SPOT_KLINE_1M_TABLE,
                BYBIT_SPOT_KLINE_5M_TABLE,
                BYBIT_SPOT_KLINE_15M_TABLE,
                BYBIT_SPOT_KLINE_60M_TABLE,
                BYBIT_SPOT_KLINE_240M_TABLE,
                BYBIT_SPOT_TICKERS_TABLE
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
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        assertEquals(1, repository.saveKline1m(List.of(data), 100L));
        assertTableCount(BYBIT_SPOT_KLINE_1M_TABLE, 1);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        assertEquals(1, repository.saveKline5m(List.of(data), 200L));
        assertTableCount(BYBIT_SPOT_KLINE_5M_TABLE, 1);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        assertEquals(1, repository.saveKline15m(List.of(data), 300L));
        assertTableCount(BYBIT_SPOT_KLINE_15M_TABLE, 1);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        assertEquals(1, repository.saveKline60m(List.of(data), 400L));
        assertTableCount(BYBIT_SPOT_KLINE_60M_TABLE, 1);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        assertEquals(1, repository.saveKline240m(List.of(data), 500L));
        assertTableCount(BYBIT_SPOT_KLINE_240M_TABLE, 1);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(data), 600L));
        assertTableCount(BYBIT_SPOT_KLINE_1D_TABLE, 1);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        assertEquals(1, repository.saveTicker(List.of(data), 700L));
        assertTableCount(BYBIT_SPOT_TICKERS_TABLE, 1);
    }
}
