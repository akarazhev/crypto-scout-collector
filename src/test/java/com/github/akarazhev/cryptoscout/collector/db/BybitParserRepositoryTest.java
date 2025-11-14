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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LPL_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitParserRepositoryTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitParserRepository repository;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        repository = BybitParserRepository.create(reactor, dataSource);
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(), BYBIT_LPL_TABLE);
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
    void shouldSaveLpl() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        assertEquals(1, repository.saveLpl(List.of(data), 100L));
        assertTableCount(BYBIT_LPL_TABLE, 1);
    }

    @Test
    void shouldGetLpl() throws Exception {
        final var data = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        assertEquals(1, repository.saveLpl(List.of(data), 200L));
        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) data.get(STAKE_BEGIN_TIME)), ZoneOffset.UTC);
        assertEquals(1, repository.getLpl(odt).size());
    }
}
