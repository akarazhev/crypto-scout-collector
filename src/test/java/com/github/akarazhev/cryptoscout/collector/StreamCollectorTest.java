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
import com.github.akarazhev.cryptoscout.collector.db.BybitParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CmcParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.CMC_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.BYBIT_PARSER;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.CMC_PARSER;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class StreamCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;

    private static CollectorDataSource dataSource;
    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitParserRepository bybitParserRepository;
    private static BybitTaSpotRepository taSpotRepository;
    private static BybitTaLinearRepository taLinearRepository;
    private static CmcParserRepository cmcParserRepository;

    private static BybitCryptoCollector bybitCryptoCollector;
    private static BybitParserCollector bybitParserCollector;
    private static BybitTaCryptoCollector bybitTaCryptoCollector;
    private static CmcParserCollector cmcParserCollector;

    private static StreamCollector streamCollector;

    private static StreamTestPublisher bybitCryptoStreamPublisher;
    private static StreamTestPublisher bybitParserStreamPublisher;
    private static StreamTestPublisher bybitTaCryptoStreamPublisher;
    private static StreamTestPublisher cmcParserStreamPublisher;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        dataSource = CollectorDataSource.create(reactor, executor);
        linearRepository = BybitLinearRepository.create(reactor, dataSource);
        spotRepository = BybitSpotRepository.create(reactor, dataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        bybitParserRepository = BybitParserRepository.create(reactor, dataSource);
        taSpotRepository = BybitTaSpotRepository.create(reactor, dataSource);
        taLinearRepository = BybitTaLinearRepository.create(reactor, dataSource);
        cmcParserRepository = CmcParserRepository.create(reactor, dataSource);

        bybitCryptoCollector = BybitCryptoCollector.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        bybitParserCollector = BybitParserCollector.create(reactor, executor, streamOffsetsRepository,
                bybitParserRepository);
        bybitTaCryptoCollector = BybitTaCryptoCollector.create(reactor, executor, streamOffsetsRepository,
                taSpotRepository, taLinearRepository);
        cmcParserCollector = CmcParserCollector.create(reactor, executor, streamOffsetsRepository, cmcParserRepository);

        streamCollector = StreamCollector.create(reactor, executor, streamOffsetsRepository, bybitCryptoCollector,
                bybitTaCryptoCollector, bybitParserCollector, cmcParserCollector);

        final var environment = AmqpConfig.getEnvironment();
        bybitCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitCryptoStream());
        bybitParserStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitParserStream());
        bybitTaCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitTaCryptoStream());
        cmcParserStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpCmcParserStream());
        TestUtils.await(bybitCryptoCollector.start(), bybitParserCollector.start(), bybitTaCryptoCollector.start(),
                cmcParserCollector.start(), streamCollector.start(), bybitCryptoStreamPublisher.start(),
                bybitParserStreamPublisher.start(), bybitTaCryptoStreamPublisher.start(),
                cmcParserStreamPublisher.start());
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                BYBIT_LPL_TABLE,

                SPOT_KLINE_1M_TABLE,
                SPOT_KLINE_5M_TABLE,
                SPOT_KLINE_15M_TABLE,
                SPOT_KLINE_60M_TABLE,
                SPOT_KLINE_240M_TABLE,
                SPOT_KLINE_1D_TABLE,
                SPOT_TICKERS_TABLE,

                TA_SPOT_PUBLIC_TRADE_TABLE,
                TA_SPOT_ORDER_BOOK_1_TABLE,
                TA_SPOT_ORDER_BOOK_50_TABLE,
                TA_SPOT_ORDER_BOOK_200_TABLE,
                TA_SPOT_ORDER_BOOK_1000_TABLE,

                LINEAR_KLINE_1M_TABLE,
                LINEAR_KLINE_5M_TABLE,
                LINEAR_KLINE_15M_TABLE,
                LINEAR_KLINE_60M_TABLE,
                LINEAR_KLINE_240M_TABLE,
                LINEAR_KLINE_1D_TABLE,
                LINEAR_TICKERS_TABLE,

                TA_LINEAR_PUBLIC_TRADE_TABLE,
                TA_LINEAR_ORDER_BOOK_1_TABLE,
                TA_LINEAR_ORDER_BOOK_50_TABLE,
                TA_LINEAR_ORDER_BOOK_200_TABLE,
                TA_LINEAR_ORDER_BOOK_1000_TABLE,
                TA_LINEAR_ALL_LIQUIDATION_TABLE,

                CMC_FGI_TABLE,
                CMC_KLINE_1D_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> bybitCryptoStreamPublisher.stop()
                .whenComplete(() -> bybitParserStreamPublisher.stop()
                        .whenComplete(() -> bybitTaCryptoStreamPublisher.stop()
                                .whenComplete(() -> cmcParserStreamPublisher.stop()
                                        .whenComplete(() -> streamCollector.stop()
                                                .whenComplete(() -> bybitCryptoCollector.stop()
                                                        .whenComplete(() -> bybitParserCollector.stop()
                                                                .whenComplete(() -> bybitTaCryptoCollector.stop()
                                                                        .whenComplete(() -> cmcParserCollector.stop()
                                                                                .whenComplete(() -> dataSource.stop()
                                                                                        .whenComplete(() -> reactor.breakEventloop()
                                                                                        )))))))))));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    @Test
    void testShouldBybitParserDataBeConsumed() throws Exception {
        final var lpl = MockData.get(BYBIT_PARSER, MockData.Type.LPL);
        bybitParserStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.LPL, lpl));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(bybitParserCollector.stop());

        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) lpl.get(STAKE_BEGIN_TIME)), ZoneOffset.UTC);
        assertEquals(1, bybitParserRepository.getLpl(odt).size());
        assertTableCount(BYBIT_LPL_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpBybitParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : -1L);

        TestUtils.await(bybitParserCollector.start());
    }

    @Test
    void testShouldCmcParserDataBeConsumed() throws Exception {
        final var fgi = MockData.get(CMC_PARSER, MockData.Type.FGI);
        cmcParserStreamPublisher.publish(Payload.of(Provider.CMC, Source.FGI, fgi));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(cmcParserCollector.stop());

        assertEquals(1, cmcParserRepository.getFgi(toOdt(fgi.get(UPDATE_TIME))).size());
        assertTableCount(CMC_FGI_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : -1L);

        TestUtils.await(cmcParserCollector.start());
    }

    @Test
    void testShouldCmcParserKline1dDataBeConsumed() throws Exception {
        final var kline = MockData.get(CMC_PARSER, MockData.Type.KLINE_D);
        cmcParserStreamPublisher.publish(Payload.of(Provider.CMC, Source.BTC_USD_1D, kline));
        Thread.sleep(Duration.ofSeconds(1));
        TestUtils.await(cmcParserCollector.stop());

        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).get(0)).get(QUOTE)).get(TIMESTAMP));
        assertEquals(1, cmcParserRepository.getKline1d(from, from).size());
        assertTableCount(CMC_KLINE_1D_TABLE, 1);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : -1L);

        TestUtils.await(cmcParserCollector.start());
    }

    @Test
    void testShouldBybitTaSpotCryptoDataBeConsumed() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.PUBLIC_TRADE);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, pt));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob1));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_50);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob50));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_200);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob200));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1000);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, ob1000));
        Thread.sleep(Duration.ofSeconds(1));

        TestUtils.await(bybitTaCryptoCollector.stop());

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), taSpotRepository.getPublicTrade(fromPt, fromPt).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);

        assertEquals(getOrderBookLevelsCount(ob1), taSpotRepository.getOrderBook1(from1, from1).size());
        assertEquals(getOrderBookLevelsCount(ob50), taSpotRepository.getOrderBook50(from50, from50).size());
        assertEquals(getOrderBookLevelsCount(ob200), taSpotRepository.getOrderBook200(from200, from200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), taSpotRepository.getOrderBook1000(from1000, from1000).size());

        TestUtils.await(bybitTaCryptoCollector.start());
    }

    @Test
    void testShouldBybitTaLinearCryptoDataBeConsumed() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.PUBLIC_TRADE);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, pt));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob1));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob50 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_50);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob50));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob200 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_200);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob200));
        Thread.sleep(Duration.ofSeconds(1));
        final var ob1000 = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1000);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, ob1000));
        Thread.sleep(Duration.ofSeconds(1));
        final var al = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ALL_LIQUIDATION);
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, al));
        Thread.sleep(Duration.ofSeconds(1));

        TestUtils.await(bybitTaCryptoCollector.stop());

        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var fromPt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);
        assertEquals(getRowsCount(pt), taLinearRepository.getPublicTrade(fromPt, fromPt).size());

        final var from1 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1.get(CTS)), ZoneOffset.UTC);
        final var from50 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob50.get(CTS)), ZoneOffset.UTC);
        final var from200 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob200.get(CTS)), ZoneOffset.UTC);
        final var from1000 = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob1000.get(CTS)), ZoneOffset.UTC);
        assertEquals(getOrderBookLevelsCount(ob1), taLinearRepository.getOrderBook1(from1, from1).size());
        assertEquals(getOrderBookLevelsCount(ob50), taLinearRepository.getOrderBook50(from50, from50).size());
        assertEquals(getOrderBookLevelsCount(ob200), taLinearRepository.getOrderBook200(from200, from200).size());
        assertEquals(getOrderBookLevelsCount(ob1000), taLinearRepository.getOrderBook1000(from1000, from1000).size());

        final var tAl = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var fromAl = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) tAl), ZoneOffset.UTC);
        assertEquals(1, taLinearRepository.getAllLiquidation(fromAl, fromAl).size());

        TestUtils.await(bybitTaCryptoCollector.start());
    }

    @Test
    void testShouldBybitSpotCryptoDataBeConsumed() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k1));
        Thread.sleep(Duration.ofSeconds(1));
        final var k5 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k5));
        Thread.sleep(Duration.ofSeconds(1));
        final var k15 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k15));
        Thread.sleep(Duration.ofSeconds(1));
        final var k60 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k60));
        Thread.sleep(Duration.ofSeconds(1));
        final var k240 = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, k240));
        Thread.sleep(Duration.ofSeconds(1));
        final var kd = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, kd));
        Thread.sleep(Duration.ofSeconds(1));
        final var tickers = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST, tickers));
        Thread.sleep(Duration.ofSeconds(1));

        TestUtils.await(bybitCryptoCollector.stop());

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

        TestUtils.await(bybitCryptoCollector.start());
    }

    @Test
    void testShouldBybitLinearCryptoDataBeConsumed() throws Exception {
        final var k1 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k1));
        Thread.sleep(Duration.ofSeconds(1));
        final var k5 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k5));
        Thread.sleep(Duration.ofSeconds(1));
        final var k15 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k15));
        Thread.sleep(Duration.ofSeconds(1));
        final var k60 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k60));
        Thread.sleep(Duration.ofSeconds(1));
        final var k240 = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, k240));
        Thread.sleep(Duration.ofSeconds(1));
        final var kd = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, kd));
        Thread.sleep(Duration.ofSeconds(1));
        final var tickers = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PML, tickers));
        Thread.sleep(Duration.ofSeconds(1));

        TestUtils.await(bybitCryptoCollector.stop());

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

        TestUtils.await(bybitCryptoCollector.start());
    }
}
