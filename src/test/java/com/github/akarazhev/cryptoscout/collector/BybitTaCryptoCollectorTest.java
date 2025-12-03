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

import com.github.akarazhev.cryptoscout.collector.db.BybitTaLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaSpotRepository;
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

import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getOrderBookLevelsCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getRowsCount;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Symbol.BTC_USDT;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitTaCryptoCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitTaSpotRepository taSpotRepository;
    private static BybitTaLinearRepository taLinearRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitTaCryptoCollector collector;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        taSpotRepository = BybitTaSpotRepository.create(reactor, dataSource);
        taLinearRepository = BybitTaLinearRepository.create(reactor, dataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        collector = BybitTaCryptoCollector.create(reactor, executor, streamOffsetsRepository,
                taSpotRepository, taLinearRepository);
        TestUtils.await(collector.start());
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                TA_SPOT_ORDER_BOOK_1_TABLE,
                TA_SPOT_ORDER_BOOK_50_TABLE,
                TA_SPOT_ORDER_BOOK_200_TABLE,
                TA_SPOT_ORDER_BOOK_1000_TABLE,
                TA_SPOT_PUBLIC_TRADE_TABLE,

                TA_LINEAR_ORDER_BOOK_1_TABLE,
                TA_LINEAR_ORDER_BOOK_50_TABLE,
                TA_LINEAR_ORDER_BOOK_200_TABLE,
                TA_LINEAR_ORDER_BOOK_1000_TABLE,
                TA_LINEAR_PUBLIC_TRADE_TABLE,
                TA_LINEAR_ALL_LIQUIDATION_TABLE,

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
    void shouldGetCollectSpotTaData() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1000);

        assertEquals(getRowsCount(pt), taSpotRepository.savePublicTrade(List.of(pt), 100L));
        assertEquals(getOrderBookLevelsCount(ob1), taSpotRepository.saveOrderBook1(List.of(ob1), 200L));
        assertEquals(getOrderBookLevelsCount(ob50), taSpotRepository.saveOrderBook50(List.of(ob50), 3000L));
        assertEquals(getOrderBookLevelsCount(ob200), taSpotRepository.saveOrderBook200(List.of(ob200), 400L));
        assertEquals(getOrderBookLevelsCount(ob1000), taSpotRepository.saveOrderBook1000(List.of(ob1000), 500L));

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), TestUtils.await(collector.getPublicTrade(BybitTaCryptoCollector.Type.BYBIT_TA_SPOT,
                BTC_USDT, fromPt, fromPt)).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(getOrderBookLevelsCount(ob1),
                TestUtils.await(collector.getOrderBook1(BybitTaCryptoCollector.Type.BYBIT_TA_SPOT, BTC_USDT, from1, from1)).size());
        assertEquals(getOrderBookLevelsCount(ob50),
                TestUtils.await(collector.getOrderBook50(BybitTaCryptoCollector.Type.BYBIT_TA_SPOT, BTC_USDT, from50, from50)).size());
        assertEquals(getOrderBookLevelsCount(ob200),
                TestUtils.await(collector.getOrderBook200(BybitTaCryptoCollector.Type.BYBIT_TA_SPOT, BTC_USDT, from200, from200)).size());
        assertEquals(getOrderBookLevelsCount(ob1000),
                TestUtils.await(collector.getOrderBook1000(BybitTaCryptoCollector.Type.BYBIT_TA_SPOT, BTC_USDT, from1000, from1000)).size());
    }

    @Test
    void shouldCollectSpotTaDataAndUpdateOffsets() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1000);

        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, pt), 100L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, ob1), 200L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, ob50), 300L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, ob200), 400L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, ob1000), 500L));

        TestUtils.await(collector.stop());

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), taSpotRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(getOrderBookLevelsCount(ob1), taSpotRepository.getOrderBook1(BTC_USDT, from1, from1).size());
        assertEquals(getOrderBookLevelsCount(ob50), taSpotRepository.getOrderBook50(BTC_USDT, from50, from50).size());
        assertEquals(getOrderBookLevelsCount(ob200), taSpotRepository.getOrderBook200(BTC_USDT, from200, from200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), taSpotRepository.getOrderBook1000(BTC_USDT, from1000, from1000).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitTaCryptoStream());
        assertEquals(500L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldGetCollectLinearTaData() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var al = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ALL_LIQUIDATION);

        assertEquals(getRowsCount(pt), taLinearRepository.savePublicTrade(List.of(pt), 100L));
        assertEquals(getOrderBookLevelsCount(ob1), taLinearRepository.saveOrderBook1(List.of(ob1), 200L));
        assertEquals(getOrderBookLevelsCount(ob50), taLinearRepository.saveOrderBook50(List.of(ob50), 3000L));
        assertEquals(getOrderBookLevelsCount(ob200), taLinearRepository.saveOrderBook200(List.of(ob200), 400L));
        assertEquals(getOrderBookLevelsCount(ob1000), taLinearRepository.saveOrderBook1000(List.of(ob1000), 500L));
        assertEquals(1, taLinearRepository.saveAllLiquidation(List.of(al), 600L));

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), TestUtils.await(collector.getPublicTrade(BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR,
                BTC_USDT, fromPt, fromPt)).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(getOrderBookLevelsCount(ob1),
                TestUtils.await(collector.getOrderBook1(BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR, BTC_USDT, from1, from1)).size());
        assertEquals(getOrderBookLevelsCount(ob50),
                TestUtils.await(collector.getOrderBook50(BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR, BTC_USDT, from50, from50)).size());
        assertEquals(getOrderBookLevelsCount(ob200),
                TestUtils.await(collector.getOrderBook200(BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR, BTC_USDT, from200, from200)).size());
        assertEquals(getOrderBookLevelsCount(ob1000),
                TestUtils.await(collector.getOrderBook1000(BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR, BTC_USDT, from1000, from1000)).size());
        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);
        assertEquals(1, TestUtils.await(collector.getAllLiquidation(BTC_USDT, fromAl, fromAl)).size());
    }

    @Test
    void shouldCollectLinearTaDataAndUpdateOffsets() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var al = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ALL_LIQUIDATION);

        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, pt), 600L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, ob1), 700L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, ob50), 800L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, ob200), 900L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, ob1000), 1000L));
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PML, al), 1100L));

        TestUtils.await(collector.stop());

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), taLinearRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);
        assertEquals(getOrderBookLevelsCount(ob1), taLinearRepository.getOrderBook1(BTC_USDT, from1, from1).size());
        assertEquals(getOrderBookLevelsCount(ob50), taLinearRepository.getOrderBook50(BTC_USDT, from50, from50).size());
        assertEquals(getOrderBookLevelsCount(ob200), taLinearRepository.getOrderBook200(BTC_USDT, from200, from200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), taLinearRepository.getOrderBook1000(BTC_USDT, from1000, from1000).size());

        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);
        assertEquals(1, taLinearRepository.getAllLiquidation(BTC_USDT, fromAl, fromAl).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitTaCryptoStream());
        assertEquals(1100L, offset.isPresent() ? offset.getAsLong() : 0L);

        TestUtils.await(collector.start());
    }

    @Test
    void shouldAdvanceOffsetWithoutData() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        TestUtils.await(collector.save(Payload.of(Provider.BYBIT, Source.PMST, k1), 1200L));

        TestUtils.await(collector.stop());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitCryptoStream());
        assertEquals(1200L, offset.isPresent() ? offset.getAsLong() : 0L);

        assertTableCount(TA_SPOT_PUBLIC_TRADE_TABLE, 0);
        assertTableCount(TA_LINEAR_PUBLIC_TRADE_TABLE, 0);

        TestUtils.await(collector.start());
    }
}
