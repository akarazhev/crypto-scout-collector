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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CryptoScoutRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource collectorDataSource;
    private static CryptoScoutRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        collectorDataSource = CollectorDataSource.create(reactor, executor);
        repository = CryptoScoutRepository.create(reactor, collectorDataSource);
    }

    @BeforeEach
    void resetState() {
        DBUtils.deleteFromTables(collectorDataSource.getDataSource(), CMC_FGI_TABLE, CMC_KLINE_1D_TABLE, CMC_KLINE_1W_TABLE);
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> collectorDataSource.stop()
                .whenComplete(() -> reactor.breakEventloop()));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void saveFgi() throws Exception {
        final var fgi = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.FGI);
        assertEquals(1, repository.saveFgi(List.of(fgi), 100L));
        assertTableCount(CMC_FGI_TABLE, 1);
    }

    @Test
    void getFgi() throws Exception {
        final var fgi = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.FGI);
        assertEquals(1, repository.saveFgi(List.of(fgi), 200L));
        final var odt = toOdt(fgi.get(UPDATE_TIME));
        assertEquals(1, repository.getFgi(odt, odt).size());
    }

    @Test
    void saveKline1d() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(kline), 300L));
        assertTableCount(CMC_KLINE_1D_TABLE, 1);
    }

    @Test
    void getKline1d() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_D);
        assertEquals(1, repository.saveKline1d(List.of(kline), 400L));

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, repository.getKline1d(symbol, from, from).size());
    }

    @Test
    void saveKline1w() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_W);
        assertEquals(1, repository.saveKline1w(List.of(kline), 500L));
        assertTableCount(CMC_KLINE_1W_TABLE, 1);
    }

    @Test
    void getKline1w() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_W);
        assertEquals(1, repository.saveKline1w(List.of(kline), 600L));

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, repository.getKline1w(symbol, from, from).size());
    }
}
