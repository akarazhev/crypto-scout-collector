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

import com.github.akarazhev.cryptoscout.collector.db.BybitParserRepository;
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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LPL_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitParserCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitParserRepository bybitParserRepository;
    private static BybitParserCollector collector;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        dataSource = CollectorDataSource.create(reactor, executor);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        bybitParserRepository = BybitParserRepository.create(reactor, dataSource);
        collector = BybitParserCollector.create(reactor, executor, streamOffsetsRepository, bybitParserRepository);
        TestUtils.await(collector.start());
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                BYBIT_LPL_TABLE,
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
    void shouldGetLpl() throws Exception {
        final var lpl = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        assertEquals(1, bybitParserRepository.saveLpl(List.of(lpl), 200L));
        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) lpl.get(STAKE_BEGIN_TIME)), ZoneOffset.UTC);
        assertEquals(1, TestUtils.await(collector.getLpl(odt)).size());
    }

    @Test
    void shouldCollectLplAndUpdateOffsets() throws Exception {
        final var lpl = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.LPL, lpl), 100L));

        TestUtils.await(collector.stop());

        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) lpl.get(STAKE_BEGIN_TIME)), ZoneOffset.UTC);
        assertEquals(1, bybitParserRepository.getLpl(odt).size());
        assertTableCount(BYBIT_LPL_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitParserStream());
        assertEquals(100L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldUpsertOffsetWhenNoDataBatch() throws Exception {
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.FGI, java.util.Map.of()), 200L));

        TestUtils.await(collector.stop());

        assertTableCount(BYBIT_LPL_TABLE, 0);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitParserStream());
        assertEquals(200L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldIgnoreInvalidProvider() throws Exception {
        final var lpl = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        TestUtils.await(collector.save(Payload.of(Provider.CMC, Source.LPL, lpl), 300L));

        TestUtils.await(collector.stop());

        assertTableCount(BYBIT_LPL_TABLE, 0);
        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }
}
