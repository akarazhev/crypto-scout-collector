/*
 * MIT License
 *
 * Copyright (c) 2026 Andrey Karazhev
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

package com.github.akarazhev.cryptoscout.collector;

import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.CryptoScoutRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.test.DBUtils;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.CRYPTO_SCOUT;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CryptoScoutServiceTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource collectorDataSource;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static CryptoScoutRepository cryptoScoutRepository;
    private static CryptoScoutService cryptoScoutService;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        collectorDataSource = CollectorDataSource.create(reactor, executor);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, collectorDataSource);
        cryptoScoutRepository = CryptoScoutRepository.create(reactor, collectorDataSource);
        cryptoScoutService = CryptoScoutService.create(reactor, executor, streamOffsetsRepository, cryptoScoutRepository);
        TestUtils.await(cryptoScoutService.start());
    }

    @BeforeEach
    void resetState() {
        DBUtils.deleteFromTables(collectorDataSource.getDataSource(),
                CMC_FGI_TABLE,
                CMC_KLINE_1D_TABLE,
                CMC_KLINE_1W_TABLE,
                STREAM_OFFSETS_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> cryptoScoutService.stop()
                .whenComplete(() -> collectorDataSource.stop()
                        .whenComplete(() -> reactor.breakEventloop())));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void kline1wSavedAndOffsetsUpdated() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_W);
        TestUtils.await(cryptoScoutService.save(Payload.of(Provider.CMC, Source.BTC_USD_1W, kline), 100L));
        TestUtils.await(cryptoScoutService.stop());

        assertTableCount(CMC_KLINE_1W_TABLE, 1);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCryptoScoutStream());
        assertEquals(100L, offset.isPresent() ? offset.getAsLong() : 0L);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void kline1dSavedAndOffsetsUpdated() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_D);
        TestUtils.await(cryptoScoutService.save(Payload.of(Provider.CMC, Source.BTC_USD_1D, kline), 200L));
        TestUtils.await(cryptoScoutService.stop());

        assertTableCount(CMC_KLINE_1D_TABLE, 1);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCryptoScoutStream());
        assertEquals(200L, offset.isPresent() ? offset.getAsLong() : 0L);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void fgiSavedAndOffsetsUpdated() throws Exception {
        final var fgi = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.FGI);
        TestUtils.await(cryptoScoutService.save(Payload.of(Provider.CMC, Source.FGI, fgi), 300L));
        TestUtils.await(cryptoScoutService.stop());

        final var odt = toOdt(fgi.get(UPDATE_TIME));
        assertEquals(1, cryptoScoutRepository.getFgi(odt, odt).size());
        assertTableCount(CMC_FGI_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCryptoScoutStream());
        assertEquals(300L, offset.isPresent() ? offset.getAsLong() : 0L);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void kline1dRetrieved() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_D);
        assertEquals(1, cryptoScoutRepository.saveKline1d(List.of(kline), 400L));

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, TestUtils.await(cryptoScoutService.getKline1d(symbol, from, from)).size());
    }

    @Test
    void kline1wRetrieved() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_W);
        assertEquals(1, cryptoScoutRepository.saveKline1w(List.of(kline), 500L));

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, TestUtils.await(cryptoScoutService.getKline1w(symbol, from, from)).size());
    }

    @Test
    void fgiRetrieved() throws Exception {
        final var fgi = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.FGI);
        assertEquals(1, cryptoScoutRepository.saveFgi(List.of(fgi), 600L));
        final var odt = toOdt(fgi.get(UPDATE_TIME));
        assertEquals(1, TestUtils.await(cryptoScoutService.getFgi(odt, odt)).size());
    }
}
