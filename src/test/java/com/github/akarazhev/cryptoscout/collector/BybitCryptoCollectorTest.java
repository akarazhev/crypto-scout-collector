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

import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LPL_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_15M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_1M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_240M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_5M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_KLINE_60M_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_TICKERS_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ALL_LIQUIDATION_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitCryptoCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitCryptoCollector collector;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        linearRepository = BybitLinearRepository.create(reactor, dataSource);
        spotRepository = BybitSpotRepository.create(reactor, dataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        collector = BybitCryptoCollector.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        TestUtils.await(collector.start());
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

                SPOT_KLINE_1M_TABLE,
                SPOT_KLINE_5M_TABLE,
                SPOT_KLINE_15M_TABLE,
                SPOT_KLINE_60M_TABLE,
                SPOT_KLINE_240M_TABLE,
                SPOT_KLINE_1D_TABLE,
                SPOT_TICKERS_TABLE,

                TA_LINEAR_PUBLIC_TRADE_TABLE,
                TA_LINEAR_ORDER_BOOK_1_TABLE,
                TA_LINEAR_ORDER_BOOK_50_TABLE,
                TA_LINEAR_ORDER_BOOK_200_TABLE,
                TA_LINEAR_ORDER_BOOK_1000_TABLE,
                TA_LINEAR_ALL_LIQUIDATION_TABLE,

                TA_SPOT_PUBLIC_TRADE_TABLE,
                TA_SPOT_ORDER_BOOK_1_TABLE,
                TA_SPOT_ORDER_BOOK_50_TABLE,
                TA_SPOT_ORDER_BOOK_200_TABLE,
                TA_SPOT_ORDER_BOOK_1000_TABLE,

                CMC_FGI_TABLE,
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
    void shouldGetCollectSpotData() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);

        assertEquals(1, spotRepository.saveKline1m(List.of(k1), 100L));
        assertEquals(1, spotRepository.saveKline5m(List.of(k5), 200L));
        assertEquals(1, spotRepository.saveKline15m(List.of(k15), 300L));
        assertEquals(1, spotRepository.saveKline60m(List.of(k60), 400L));
        assertEquals(1, spotRepository.saveKline240m(List.of(k240), 500L));
        assertEquals(1, spotRepository.saveKline1d(List.of(kd), 600L));
        assertEquals(1, spotRepository.saveTicker(List.of(tickers), 700L));

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var from5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var from15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var from60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var from240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var fromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);

        assertEquals(1, TestUtils.await(collector.getKline1m(BybitCryptoCollector.Type.BYBIT_SPOT, from1,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline5m(BybitCryptoCollector.Type.BYBIT_SPOT, from5,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline15m(BybitCryptoCollector.Type.BYBIT_SPOT, from15,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline60m(BybitCryptoCollector.Type.BYBIT_SPOT, from60,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline240m(BybitCryptoCollector.Type.BYBIT_SPOT, from240,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline1d(BybitCryptoCollector.Type.BYBIT_SPOT, fromD,
                OffsetDateTime.now(ZoneOffset.UTC))).size());

        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(1, TestUtils.await(collector.getTicker(BybitCryptoCollector.Type.BYBIT_SPOT, tickersFrom,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
    }

    @Test
    void shouldCollectSpotDataAndUpdateOffsets() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);

        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k1), 100L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k5), 200L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k15), 300L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k60), 400L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k240), 500L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, kd), 600L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, tickers), 700L));

        TestUtils.await(collector.stop());

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var from5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var from15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var from60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var from240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var fromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);

        assertEquals(1, spotRepository.getKline1m(from1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline5m(from5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline15m(from15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline60m(from60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline240m(from240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline1d(fromD, OffsetDateTime.now(ZoneOffset.UTC)).size());

        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(1, spotRepository.getTicker(tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitCryptoStream());
        assertEquals(700L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldGetCollectLinearData() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);

        assertEquals(1, linearRepository.saveKline1m(List.of(k1), 100L));
        assertEquals(1, linearRepository.saveKline5m(List.of(k5), 200L));
        assertEquals(1, linearRepository.saveKline15m(List.of(k15), 300L));
        assertEquals(1, linearRepository.saveKline60m(List.of(k60), 400L));
        assertEquals(1, linearRepository.saveKline240m(List.of(k240), 500L));
        assertEquals(1, linearRepository.saveKline1d(List.of(kd), 600L));
        assertEquals(1, linearRepository.saveTicker(List.of(tickers), 700L));

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var from5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var from15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var from60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var from240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var fromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);

        assertEquals(1, TestUtils.await(collector.getKline1m(BybitCryptoCollector.Type.BYBIT_LINEAR, from1,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline5m(BybitCryptoCollector.Type.BYBIT_LINEAR, from5,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline15m(BybitCryptoCollector.Type.BYBIT_LINEAR, from15,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline60m(BybitCryptoCollector.Type.BYBIT_LINEAR, from60,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline240m(BybitCryptoCollector.Type.BYBIT_LINEAR, from240,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(collector.getKline1d(BybitCryptoCollector.Type.BYBIT_LINEAR, fromD,
                OffsetDateTime.now(ZoneOffset.UTC))).size());

        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(1, TestUtils.await(collector.getTicker(BybitCryptoCollector.Type.BYBIT_LINEAR, tickersFrom,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
    }

    @Test
    void shouldCollectLinearDataAndUpdateOffsets() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);

        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, k1), 800L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, k5), 900L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, k15), 1000L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, k60), 1100L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, k240), 1200L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, kd), 1300L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, tickers), 1400L));

        TestUtils.await(collector.stop());

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var from5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var from15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var from60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var from240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var fromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);

        assertEquals(1, linearRepository.getKline1m(from1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline5m(from5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline15m(from15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline60m(from60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline240m(from240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline1d(fromD, OffsetDateTime.now(ZoneOffset.UTC)).size());

        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(1, linearRepository.getTicker(tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitCryptoStream());
        assertEquals(1400L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldAdvanceOffsetWithoutData() throws Exception {
        final var ob1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1);
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, ob1), 1500L));

        TestUtils.await(collector.stop());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitCryptoStream());
        assertEquals(1500L, offset.isPresent() ? offset.getAsLong() : 0L);

        final var from = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1);
        final var to = OffsetDateTime.now(ZoneOffset.UTC);
        assertEquals(0, spotRepository.getKline1m(from, to).size());
        assertEquals(0, spotRepository.getTicker(from, to).size());

        TestUtils.await(collector.start());
    }
}
