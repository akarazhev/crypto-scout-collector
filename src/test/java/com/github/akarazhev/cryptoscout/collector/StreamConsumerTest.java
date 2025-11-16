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
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.BYBIT_PARSER;
import static com.github.akarazhev.cryptoscout.test.MockData.Source.CMC_PARSER;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.DATA_LIST;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class StreamConsumerTest {
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

    private static StreamConsumer streamConsumer;

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

        streamConsumer = StreamConsumer.create(reactor, executor, streamOffsetsRepository, bybitCryptoCollector,
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
                cmcParserCollector.start(), streamConsumer.start(), bybitCryptoStreamPublisher.start(),
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

                CMC_FGI_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> bybitCryptoStreamPublisher.stop()
                .whenComplete(() -> bybitParserStreamPublisher.stop()
                        .whenComplete(() -> bybitTaCryptoStreamPublisher.stop()
                                .whenComplete(() -> cmcParserStreamPublisher.stop()
                                        .whenComplete(() -> streamConsumer.stop()
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

        final var ts = ((Map<?, ?>) ((List<?>) fgi.get(DATA_LIST)).getFirst()).get(TIMESTAMP);
        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong((String) ts)), ZoneOffset.UTC);
        assertEquals(1, cmcParserRepository.getFgi(odt).size());
        assertTableCount(CMC_FGI_TABLE, 30);

        final var offset = streamOffsetsRepository.getOffset(AmqpConfig.getAmqpCmcParserStream());
        assertEquals(0L, offset.isPresent() ? offset.getAsLong() : -1L);

        TestUtils.await(cmcParserCollector.start());
    }

    @Test
    void testShouldBybitTaSpotCryptoDataBeConsumed() throws Exception {
        // TODO:
    }

    @Test
    void testShouldBybitTaLinearCryptoDataBeConsumed() throws Exception {
        // TODO:
    }

    @Test
    void testShouldBybitSpotCryptoDataBeConsumed() throws Exception {
        // TODO:
    }

    @Test
    void testShouldBybitLinearCryptoDataBeConsumed() throws Exception {
        // TODO:
    }
}
