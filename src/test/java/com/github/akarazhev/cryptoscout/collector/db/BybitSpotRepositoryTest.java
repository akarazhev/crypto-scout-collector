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

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
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
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                SPOT_KLINE_1M_TABLE,
                SPOT_KLINE_5M_TABLE,
                SPOT_KLINE_15M_TABLE,
                SPOT_KLINE_60M_TABLE,
                SPOT_KLINE_240M_TABLE,
                SPOT_KLINE_1D_TABLE,
                SPOT_TICKERS_TABLE
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
        assertTableCount(SPOT_KLINE_1M_TABLE, 1);
    }

    @Test
    void shouldSaveKline5m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        assertEquals(1, repository.saveKline5m(List.of(data), 200L));
        assertTableCount(SPOT_KLINE_5M_TABLE, 1);
    }

    @Test
    void shouldSaveKline15m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        assertEquals(1, repository.saveKline15m(List.of(data), 300L));
        assertTableCount(SPOT_KLINE_15M_TABLE, 1);
    }

    @Test
    void shouldSaveKline60m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        assertEquals(1, repository.saveKline60m(List.of(data), 400L));
        assertTableCount(SPOT_KLINE_60M_TABLE, 1);
    }

    @Test
    void shouldSaveKline240m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        assertEquals(1, repository.saveKline240m(List.of(data), 500L));
        assertTableCount(SPOT_KLINE_240M_TABLE, 1);
    }

    @Test
    void shouldSaveKline1d() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(data), 600L));
        assertTableCount(SPOT_KLINE_1D_TABLE, 1);
    }

    @Test
    void shouldSaveTicker() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        assertEquals(1, repository.saveTicker(List.of(data), 700L));
        assertTableCount(SPOT_TICKERS_TABLE, 1);
    }

    @Test
    void shouldGetKline1m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline1m(List.of(data), 800L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline1m(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline5m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline5m(List.of(data), 900L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline5m(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline15m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline15m(List.of(data), 1000L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline15m(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline60m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline60m(List.of(data), 1100L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline60m(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline240m() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline240m(List.of(data), 1200L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline240m(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetKline1d() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        final var start = ((Map<?, ?>)((List<?>) data.get(DATA)).getFirst()).get(START);
        assertEquals(1, repository.saveKline1d(List.of(data), 1300L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);
        assertEquals(1, repository.getKline1d(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }

    @Test
    void shouldGetTicker() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        assertEquals(1, repository.saveTicker(List.of(data), 1400L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) data.get(TS)), ZoneOffset.UTC);
        assertEquals(1, repository.getTicker(from, OffsetDateTime.now(ZoneOffset.UTC)).size());
    }
}
