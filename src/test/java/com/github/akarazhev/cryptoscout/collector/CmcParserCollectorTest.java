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

package com.github.akarazhev.cryptoscout.collector;

import com.github.akarazhev.cryptoscout.collector.db.CmcParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.CMC_PARSER;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CmcParserCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static CmcParserRepository cmcParserRepository;
    private static CmcParserCollector collector;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        dataSource = CollectorDataSource.create(reactor, executor);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        cmcParserRepository = CmcParserRepository.create(reactor, dataSource);
        collector = CmcParserCollector.create(reactor, executor, streamOffsetsRepository, cmcParserRepository);
        TestUtils.await(collector.start());
    }

    @Test
    void shouldCollectKline1dAndUpdateOffsets() throws Exception {
        final var kline = MockData.get(CMC_PARSER, MockData.Type.KLINE_D);
        TestUtils.await(collector.save(Payload.of(Provider.CMC, Source.BTC_USD_1D, kline), 110L));

        TestUtils.await(collector.stop());

        assertTableCount(CMC_KLINE_1D_TABLE, 1);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(110L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldGetKline1d() throws Exception {
        final var kline = MockData.get(CMC_PARSER, MockData.Type.KLINE_D);
        assertEquals(1, cmcParserRepository.saveKline1d(List.of(kline), 120L));

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        assertEquals(1, TestUtils.await(collector.getKline1d(from, from)).size());
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                CMC_FGI_TABLE,
                CMC_KLINE_1D_TABLE,
                STREAM_OFFSETS_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> collector.stop()
                .whenComplete(() -> dataSource.stop()
                        .whenComplete(() -> reactor.breakEventloop())));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void shouldGetFgi() throws Exception {
        final var fgi = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.FGI);
        assertEquals(1, cmcParserRepository.saveFgi(List.of(fgi), 200L));
        assertEquals(1, TestUtils.await(collector.getFgi(toOdt(fgi.get(UPDATE_TIME)))).size());
    }

    @Test
    void shouldCollectFgiAndUpdateOffsets() throws Exception {
        final var fgi = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.FGI);
        TestUtils.await(collector.save(Payload.of(Provider.CMC, Source.FGI, fgi), 100L));

        TestUtils.await(collector.stop());

        assertEquals(1, cmcParserRepository.getFgi(toOdt(fgi.get(UPDATE_TIME))).size());
        assertTableCount(CMC_FGI_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(100L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldUpsertOffsetWhenNoDataBatch() throws Exception {
        TestUtils.await(collector.save(Payload.of(Provider.CMC, Source.LPL, Map.of()), 200L));

        TestUtils.await(collector.stop());

        assertTableCount(CMC_FGI_TABLE, 0);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(200L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldIgnoreInvalidProvider() throws Exception {
        final var fgi = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.FGI);
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.FGI, fgi), 300L));

        TestUtils.await(collector.stop());

        assertTableCount(CMC_FGI_TABLE, 0);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }
}
