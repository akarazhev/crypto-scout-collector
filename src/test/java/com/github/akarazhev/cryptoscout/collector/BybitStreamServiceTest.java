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

import static com.github.akarazhev.cryptoscout.collector.BybitStreamService.Type.BYBIT_LINEAR;
import static com.github.akarazhev.cryptoscout.collector.BybitStreamService.Type.BYBIT_SPOT;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getOrderBookLevelsCount;
import static com.github.akarazhev.cryptoscout.collector.PayloadParser.getRowsCount;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ALL_LIQUIDATION_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.LINEAR_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SPOT_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Symbol.BTC_USDT;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class BybitStreamServiceTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitStreamService bybitStreamService;

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
        bybitStreamService = BybitStreamService.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        TestUtils.await(bybitStreamService.start());
    }

    @BeforeEach
    void resetState() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                LINEAR_KLINE_1M_TABLE,
                LINEAR_KLINE_5M_TABLE,
                LINEAR_KLINE_15M_TABLE,
                LINEAR_KLINE_60M_TABLE,
                LINEAR_KLINE_240M_TABLE,
                LINEAR_KLINE_1D_TABLE,
                LINEAR_TICKERS_TABLE,
                LINEAR_PUBLIC_TRADE_TABLE,
                LINEAR_ORDER_BOOK_1_TABLE,
                LINEAR_ORDER_BOOK_50_TABLE,
                LINEAR_ORDER_BOOK_200_TABLE,
                LINEAR_ORDER_BOOK_1000_TABLE,
                LINEAR_ALL_LIQUIDATION_TABLE,

                SPOT_KLINE_1M_TABLE,
                SPOT_KLINE_5M_TABLE,
                SPOT_KLINE_15M_TABLE,
                SPOT_KLINE_60M_TABLE,
                SPOT_KLINE_240M_TABLE,
                SPOT_KLINE_1D_TABLE,
                SPOT_TICKERS_TABLE,
                SPOT_PUBLIC_TRADE_TABLE,
                SPOT_ORDER_BOOK_1_TABLE,
                SPOT_ORDER_BOOK_50_TABLE,
                SPOT_ORDER_BOOK_200_TABLE,
                SPOT_ORDER_BOOK_1000_TABLE,

                CMC_FGI_TABLE,
                STREAM_OFFSETS_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> bybitStreamService.stop()
                .whenComplete(() -> dataSource.stop()
                        .whenComplete(() -> reactor.breakEventloop())));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void spotDataSavedAndRetrieved() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        final var pt = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1000);

        assertEquals(getRowsCount(k1), spotRepository.saveKline1m(List.of(k1), 100L));
        assertEquals(getRowsCount(k5), spotRepository.saveKline5m(List.of(k5), 200L));
        assertEquals(getRowsCount(k15), spotRepository.saveKline15m(List.of(k15), 300L));
        assertEquals(getRowsCount(k60), spotRepository.saveKline60m(List.of(k60), 400L));
        assertEquals(getRowsCount(k240), spotRepository.saveKline240m(List.of(k240), 500L));
        assertEquals(getRowsCount(kd), spotRepository.saveKline1d(List.of(kd), 600L));
        assertEquals(1, spotRepository.saveTicker(List.of(tickers), 700L));
        assertEquals(getRowsCount(pt), spotRepository.savePublicTrade(List.of(pt), 800L));
        assertEquals(getOrderBookLevelsCount(ob1), spotRepository.saveOrderBook1(List.of(ob1), 900L));
        assertEquals(getOrderBookLevelsCount(ob50), spotRepository.saveOrderBook50(List.of(ob50), 1000L));
        assertEquals(getOrderBookLevelsCount(ob200), spotRepository.saveOrderBook200(List.of(ob200), 1100L));
        assertEquals(getOrderBookLevelsCount(ob1000), spotRepository.saveOrderBook1000(List.of(ob1000), 1200L));

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);
        final var kFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var kFrom5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var kFrom15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var kFrom60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var kFrom240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var kFromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);

        assertEquals(getRowsCount(pt), TestUtils.await(bybitStreamService.getPublicTrade(BYBIT_SPOT, BTC_USDT, fromPt,
                fromPt)).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline1m(BYBIT_SPOT, BTC_USDT, kFrom1,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline5m(BYBIT_SPOT, BTC_USDT, kFrom5,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline15m(BYBIT_SPOT, BTC_USDT, kFrom15,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline60m(BYBIT_SPOT, BTC_USDT, kFrom60,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline240m(BYBIT_SPOT, BTC_USDT, kFrom240,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline1d(BYBIT_SPOT, BTC_USDT, kFromD,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getTicker(BYBIT_SPOT, BTC_USDT, tickersFrom,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(getOrderBookLevelsCount(ob1), TestUtils.await(bybitStreamService.getOrderBook1(BYBIT_SPOT,
                BTC_USDT, oFrom1, oFrom1)).size());
        assertEquals(getOrderBookLevelsCount(ob50), TestUtils.await(bybitStreamService.getOrderBook50(BYBIT_SPOT,
                BTC_USDT, oFrom50, oFrom50)).size());
        assertEquals(getOrderBookLevelsCount(ob200), TestUtils.await(bybitStreamService.getOrderBook200(BYBIT_SPOT,
                BTC_USDT, oFrom200, oFrom200)).size());
        assertEquals(getOrderBookLevelsCount(ob1000), TestUtils.await(bybitStreamService.getOrderBook1000(BYBIT_SPOT,
                BTC_USDT, oFrom1000, oFrom1000)).size());
    }

    @Test
    void spotDataSavedViaPayloadAndOffsetsUpdated() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        final var pt = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1000);

        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, k1), 100L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, k5), 200L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, k15), 300L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, k60), 400L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, k240), 500L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, kd), 600L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, tickers), 700L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, pt), 800L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, ob1), 900L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, ob50), 1000L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, ob200), 1100L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PMST, ob1000), 1200L));
        TestUtils.await(bybitStreamService.stop());

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        final var kFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var kFrom5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var kFrom15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var kFrom60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var kFrom240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var kFromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);
        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(1, spotRepository.getKline1m(BTC_USDT, kFrom1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline5m(BTC_USDT, kFrom5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline15m(BTC_USDT, kFrom15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline60m(BTC_USDT, kFrom60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline240m(BTC_USDT, kFrom240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline1d(BTC_USDT, kFromD, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getTicker(BTC_USDT, tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(getRowsCount(pt), spotRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());
        assertEquals(getOrderBookLevelsCount(ob1), spotRepository.getOrderBook1(BTC_USDT, oFrom1, oFrom1).size());
        assertEquals(getOrderBookLevelsCount(ob50), spotRepository.getOrderBook50(BTC_USDT, oFrom50, oFrom50).size());
        assertEquals(getOrderBookLevelsCount(ob200), spotRepository.getOrderBook200(BTC_USDT, oFrom200, oFrom200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), spotRepository.getOrderBook1000(BTC_USDT, oFrom1000, oFrom1000).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitStream());
        assertEquals(1200L, offset.isPresent() ? offset.getAsLong() : 0L);
        TestUtils.await(bybitStreamService.start());
    }

    @Test
    void linearDataSavedAndRetrieved() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);

        assertEquals(1, linearRepository.saveKline1m(List.of(k1), 100L));
        assertEquals(1, linearRepository.saveKline5m(List.of(k5), 200L));
        assertEquals(1, linearRepository.saveKline15m(List.of(k15), 300L));
        assertEquals(1, linearRepository.saveKline60m(List.of(k60), 400L));
        assertEquals(1, linearRepository.saveKline240m(List.of(k240), 500L));
        assertEquals(1, linearRepository.saveKline1d(List.of(kd), 600L));
        assertEquals(1, linearRepository.saveTicker(List.of(tickers), 700L));
        assertEquals(getRowsCount(pt), linearRepository.savePublicTrade(List.of(pt), 800L));
        assertEquals(getOrderBookLevelsCount(ob1), linearRepository.saveOrderBook1(List.of(ob1), 900L));
        assertEquals(getOrderBookLevelsCount(ob50), linearRepository.saveOrderBook50(List.of(ob50), 1000L));
        assertEquals(getOrderBookLevelsCount(ob200), linearRepository.saveOrderBook200(List.of(ob200), 1100L));
        assertEquals(getOrderBookLevelsCount(ob1000), linearRepository.saveOrderBook1000(List.of(ob1000), 1200L));
        assertEquals(1, linearRepository.saveAllLiquidation(List.of(al), 1300L));

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        final var kFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var kFrom5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var kFrom15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var kFrom60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var kFrom240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var kFromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);
        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(1, TestUtils.await(bybitStreamService.getKline1m(BYBIT_LINEAR, BTC_USDT, kFrom1,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline5m(BYBIT_LINEAR, BTC_USDT, kFrom5,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline15m(BYBIT_LINEAR, BTC_USDT, kFrom15,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline60m(BYBIT_LINEAR, BTC_USDT, kFrom60,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline240m(BYBIT_LINEAR, BTC_USDT, kFrom240,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getKline1d(BYBIT_LINEAR, BTC_USDT, kFromD,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(1, TestUtils.await(bybitStreamService.getTicker(BYBIT_LINEAR, BTC_USDT, tickersFrom,
                OffsetDateTime.now(ZoneOffset.UTC))).size());
        assertEquals(getRowsCount(pt), TestUtils.await(bybitStreamService.getPublicTrade(BYBIT_LINEAR,
                BTC_USDT, fromPt, fromPt)).size());
        assertEquals(getOrderBookLevelsCount(ob1), TestUtils.await(bybitStreamService.getOrderBook1(BYBIT_LINEAR,
                BTC_USDT, oFrom1, oFrom1)).size());
        assertEquals(getOrderBookLevelsCount(ob50), TestUtils.await(bybitStreamService.getOrderBook50(BYBIT_LINEAR,
                BTC_USDT, oFrom50, oFrom50)).size());
        assertEquals(getOrderBookLevelsCount(ob200), TestUtils.await(bybitStreamService.getOrderBook200(BYBIT_LINEAR,
                BTC_USDT, oFrom200, oFrom200)).size());
        assertEquals(getOrderBookLevelsCount(ob1000), TestUtils.await(bybitStreamService.getOrderBook1000(BYBIT_LINEAR,
                BTC_USDT, oFrom1000, oFrom1000)).size());
        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);
        assertEquals(1, TestUtils.await(bybitStreamService.getAllLiquidation(BTC_USDT, fromAl, fromAl)).size());
    }

    @Test
    void linearDataSavedViaPayloadAndOffsetsUpdated() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        final var ob1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        final var ob50 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        final var ob200 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        final var ob1000 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);

        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, k1), 100L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, k5), 200L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, k15), 300L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, k60), 400L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, k240), 500L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, kd), 600L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, tickers), 700L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, pt), 800L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, ob1), 900L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, ob50), 1000L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, ob200), 1100L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, ob1000), 1200L));
        TestUtils.await(bybitStreamService.save(Payload.of(Provider.BYBIT, Source.PML, al), 1300L));
        TestUtils.await(bybitStreamService.stop());

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        final var kFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var kFrom5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var kFrom15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var kFrom60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var kFrom240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var kFromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);
        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), linearRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(1, linearRepository.getKline1m(BTC_USDT, kFrom1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline5m(BTC_USDT, kFrom5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline15m(BTC_USDT, kFrom15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline60m(BTC_USDT, kFrom60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline240m(BTC_USDT, kFrom240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline1d(BTC_USDT, kFromD, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getTicker(BTC_USDT, tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(getOrderBookLevelsCount(ob1), linearRepository.getOrderBook1(BTC_USDT, oFrom1, oFrom1).size());
        assertEquals(getOrderBookLevelsCount(ob50), linearRepository.getOrderBook50(BTC_USDT, oFrom50, oFrom50).size());
        assertEquals(getOrderBookLevelsCount(ob200), linearRepository.getOrderBook200(BTC_USDT, oFrom200, oFrom200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), linearRepository.getOrderBook1000(BTC_USDT, oFrom1000, oFrom1000).size());

        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);
        assertEquals(1, linearRepository.getAllLiquidation(BTC_USDT, fromAl, fromAl).size());

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitStream());
        assertEquals(1300L, offset.isPresent() ? offset.getAsLong() : 0L);
        TestUtils.await(bybitStreamService.start());
    }
}
