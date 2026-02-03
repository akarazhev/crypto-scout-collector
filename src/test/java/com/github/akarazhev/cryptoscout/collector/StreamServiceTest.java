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

import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.CryptoScoutRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.test.DBUtils;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.cryptoscout.test.StreamTestPublisher;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.CRYPTO_SCOUT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Symbol.BTC_USDT;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class StreamServiceTest {
    private static ExecutorService executor;
    private static Eventloop reactor;

    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static CollectorDataSource collectorDataSource;
    private static CryptoScoutRepository cryptoScoutRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;

    private static BybitStreamService bybitStreamService;
    private static CryptoScoutService cryptoScoutService;
    private static StreamService streamService;

    private static StreamTestPublisher bybitStreamTestPublisher;
    private static StreamTestPublisher cryptoScoutStreamTestPublisher;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        collectorDataSource = CollectorDataSource.create(reactor, executor);
        linearRepository = BybitLinearRepository.create(reactor, collectorDataSource);
        spotRepository = BybitSpotRepository.create(reactor, collectorDataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, collectorDataSource);
        cryptoScoutRepository = CryptoScoutRepository.create(reactor, collectorDataSource);

        bybitStreamService = BybitStreamService.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        cryptoScoutService = CryptoScoutService.create(reactor, executor, streamOffsetsRepository,
                cryptoScoutRepository);

        streamService = StreamService.create(reactor, executor, streamOffsetsRepository, bybitStreamService,
                cryptoScoutService);

        final var environment = AmqpConfig.getEnvironment();
        bybitStreamTestPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitStream());
        cryptoScoutStreamTestPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpCryptoScoutStream());
        TestUtils.await(bybitStreamService.start(), cryptoScoutService.start(), streamService.start(),
                bybitStreamTestPublisher.start(), cryptoScoutStreamTestPublisher.start());
    }

    @BeforeEach
    void resetState() {
        DBUtils.deleteFromTables(collectorDataSource.getDataSource(),
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

                CMC_FGI_TABLE,
                CMC_KLINE_1D_TABLE,
                CMC_KLINE_1W_TABLE,
                STREAM_OFFSETS_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> bybitStreamTestPublisher.stop()
                .whenComplete(() -> cryptoScoutStreamTestPublisher.stop()
                        .whenComplete(() -> streamService.stop()
                                .whenComplete(() -> bybitStreamService.stop()
                                        .whenComplete(() -> cryptoScoutService.stop()
                                                .whenComplete(() -> collectorDataSource.stop()
                                                        .whenComplete(() -> reactor.breakEventloop()
                                                        )))))));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void cmcFgiDataConsumedAndPersisted() throws Exception {
        final var fgi = MockData.get(CRYPTO_SCOUT, MockData.Type.FGI);
        cryptoScoutStreamTestPublisher.publish(Payload.of(Provider.CMC, Source.FGI, fgi));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(cryptoScoutService.stop());

        final var odt = toOdt(fgi.get(UPDATE_TIME));
        assertEquals(1, cryptoScoutRepository.getFgi(odt, odt).size());
        assertTableCount(CMC_FGI_TABLE, 1);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void cmcKline1wDataConsumedAndPersisted() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_W);
        cryptoScoutStreamTestPublisher.publish(Payload.of(Provider.CMC, Source.BTC_USD_1W, kline));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(cryptoScoutService.stop());

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, cryptoScoutRepository.getKline1w(symbol, from, from).size());
        assertTableCount(CMC_KLINE_1W_TABLE, 1);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void cmcKline1dDataConsumedAndPersisted() throws Exception {
        final var kline = MockData.get(CRYPTO_SCOUT, MockData.Type.KLINE_D);
        cryptoScoutStreamTestPublisher.publish(Payload.of(Provider.CMC, Source.BTC_USD_1D, kline));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(cryptoScoutService.stop());

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);
        assertEquals(1, cryptoScoutRepository.getKline1d(symbol, from, from).size());
        assertTableCount(CMC_KLINE_1D_TABLE, 1);
        TestUtils.await(cryptoScoutService.start());
    }

    @Test
    void bybitSpotDataConsumedAndPersisted() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k1));
        Thread.sleep(Duration.ofSeconds(1));
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k5));
        Thread.sleep(Duration.ofSeconds(1));
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k15));
        Thread.sleep(Duration.ofSeconds(1));
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k60));
        Thread.sleep(Duration.ofSeconds(1));
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k240));
        Thread.sleep(Duration.ofSeconds(1));
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, kd));
        Thread.sleep(Duration.ofSeconds(1));
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, tickers));
        Thread.sleep(Duration.ofSeconds(1));
        final var pt = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.PUBLIC_TRADE);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, pt));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob1));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob50 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_50);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob50));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob200 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_200);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob200));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1000 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1000);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob1000));
        Thread.sleep(Duration.ofSeconds(1));
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
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(getRowsCount(pt), spotRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());
        assertEquals(1, spotRepository.getKline1m(BTC_USDT, kFrom1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline5m(BTC_USDT, kFrom5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline15m(BTC_USDT, kFrom15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline60m(BTC_USDT, kFrom60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline240m(BTC_USDT, kFrom240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, spotRepository.getKline1d(BTC_USDT, kFromD, OffsetDateTime.now(ZoneOffset.UTC)).size());

        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        assertEquals(1, spotRepository.getTicker(BTC_USDT, tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());

        assertEquals(getOrderBookLevelsCount(ob1), spotRepository.getOrderBook1(BTC_USDT, oFrom1, oFrom1).size());
        assertEquals(getOrderBookLevelsCount(ob50), spotRepository.getOrderBook50(BTC_USDT, oFrom50, oFrom50).size());
        assertEquals(getOrderBookLevelsCount(ob200), spotRepository.getOrderBook200(BTC_USDT, oFrom200, oFrom200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), spotRepository.getOrderBook1000(BTC_USDT, oFrom1000, oFrom1000).size());
        TestUtils.await(bybitStreamService.start());
    }

    @Test
    void bybitLinearDataConsumedAndPersisted() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k1));
        Thread.sleep(Duration.ofSeconds(1));
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k5));
        Thread.sleep(Duration.ofSeconds(1));
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k15));
        Thread.sleep(Duration.ofSeconds(1));
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k60));
        Thread.sleep(Duration.ofSeconds(1));
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k240));
        Thread.sleep(Duration.ofSeconds(1));
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, kd));
        Thread.sleep(Duration.ofSeconds(1));
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, tickers));
        Thread.sleep(Duration.ofSeconds(1));
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, pt));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob1));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob50 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob50));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob200 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob200));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1000 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob1000));
        Thread.sleep(Duration.ofSeconds(1));
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);
        bybitStreamTestPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, al));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(bybitStreamService.stop());

        final var k1Start = ((Map<?, ?>) ((List<?>) k1.get(DATA)).getFirst()).get(START);
        final var k5Start = ((Map<?, ?>) ((List<?>) k5.get(DATA)).getFirst()).get(START);
        final var k15Start = ((Map<?, ?>) ((List<?>) k15.get(DATA)).getFirst()).get(START);
        final var k60Start = ((Map<?, ?>) ((List<?>) k60.get(DATA)).getFirst()).get(START);
        final var k240Start = ((Map<?, ?>) ((List<?>) k240.get(DATA)).getFirst()).get(START);
        final var kdStart = ((Map<?, ?>) ((List<?>) kd.get(DATA)).getFirst()).get(START);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        final var tickersFrom = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tickers.get(TS)), ZoneOffset.UTC);
        final var kFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k1Start), ZoneOffset.UTC);
        final var kFrom5 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k5Start), ZoneOffset.UTC);
        final var kFrom15 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k15Start), ZoneOffset.UTC);
        final var kFrom60 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k60Start), ZoneOffset.UTC);
        final var kFrom240 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) k240Start), ZoneOffset.UTC);
        final var kFromD = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) kdStart), ZoneOffset.UTC);
        final var oFrom1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var oFrom50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var oFrom200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var oFrom1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);
        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);

        assertEquals(1, linearRepository.getKline1m(BTC_USDT, kFrom1, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline5m(BTC_USDT, kFrom5, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline15m(BTC_USDT, kFrom15, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline60m(BTC_USDT, kFrom60, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline240m(BTC_USDT, kFrom240, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getKline1d(BTC_USDT, kFromD, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(getRowsCount(pt), linearRepository.getPublicTrade(BTC_USDT, fromPt, fromPt).size());
        assertEquals(getOrderBookLevelsCount(ob1), linearRepository.getOrderBook1(BTC_USDT, oFrom1, oFrom1).size());
        assertEquals(getOrderBookLevelsCount(ob50), linearRepository.getOrderBook50(BTC_USDT, oFrom50, oFrom50).size());
        assertEquals(getOrderBookLevelsCount(ob200), linearRepository.getOrderBook200(BTC_USDT, oFrom200, oFrom200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), linearRepository.getOrderBook1000(BTC_USDT, oFrom1000, oFrom1000).size());
        assertEquals(1, linearRepository.getTicker(BTC_USDT, tickersFrom, OffsetDateTime.now(ZoneOffset.UTC)).size());
        assertEquals(1, linearRepository.getAllLiquidation(BTC_USDT, fromAl, fromAl).size());
        TestUtils.await(bybitStreamService.start());
    }
}
