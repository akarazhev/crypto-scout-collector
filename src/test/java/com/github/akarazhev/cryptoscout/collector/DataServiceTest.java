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
import com.github.akarazhev.cryptoscout.collector.db.CryptoScoutRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.test.AmqpTestConsumer;
import com.github.akarazhev.cryptoscout.test.AmqpTestPublisher;
import com.github.akarazhev.cryptoscout.test.DBUtils;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.jcryptolib.stream.Message;
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
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_15M;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_1D;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_1M;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_240M;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_5M;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_KLINE_60M;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_TICKER;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_ALL_LIQUIDATION;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_ORDER_BOOK_1;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_ORDER_BOOK_1000;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_ORDER_BOOK_200;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_ORDER_BOOK_50;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.BYBIT_GET_PUBLIC_TRADE;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.CRYPTO_SCOUT_GET_FGI;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.CRYPTO_SCOUT_GET_KLINE_1D;
import static com.github.akarazhev.cryptoscout.collector.Constants.Method.CRYPTO_SCOUT_GET_KLINE_1W;
import static com.github.akarazhev.cryptoscout.collector.Constants.Source.ANALYST;
import static com.github.akarazhev.cryptoscout.collector.Constants.Source.COLLECTOR;
import static com.github.akarazhev.cryptoscout.collector.DataServiceTest.Config.ANALYST_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.collector.DataServiceTest.Config.CHATBOT_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.collector.DataServiceTest.Config.COLLECTOR_CONSUMER_CLIENT_NAME;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1D_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_KLINE_1W_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.START;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.T;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Symbol.BTC_USDT;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTE;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.QUOTES;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.SYMBOL;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.TIMESTAMP;
import static com.github.akarazhev.jcryptolib.cmc.Constants.Response.UPDATE_TIME;
import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Cmc.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.test.Assertions.assertTableCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class DataServiceTest {
    private static ExecutorService executor;
    private static Eventloop reactor;

    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static CollectorDataSource collectorDataSource;
    private static CryptoScoutRepository cryptoScoutRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;

    private static BybitStreamService bybitStreamService;
    private static CryptoScoutService cryptoScoutService;
    private static DataService dataService;

    private static AmqpPublisher chatbotPublisher;
    private static AmqpPublisher analystPublisher;
    private static AmqpConsumer collectorConsumer;

    private static AmqpTestPublisher collectorQueuePublisher;
    private static AmqpTestConsumer analystQueueConsumer;
    private static AmqpTestConsumer chatbotQueueConsumer;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        collectorDataSource = CollectorDataSource.create(reactor, executor);
        linearRepository = BybitLinearRepository.create(reactor, collectorDataSource);
        spotRepository = BybitSpotRepository.create(reactor, collectorDataSource);
        cryptoScoutRepository = CryptoScoutRepository.create(reactor, collectorDataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, collectorDataSource);

        bybitStreamService = BybitStreamService.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        cryptoScoutService = CryptoScoutService.create(reactor, executor, streamOffsetsRepository, cryptoScoutRepository);

        chatbotPublisher = AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                CHATBOT_PUBLISHER_CLIENT_NAME, AmqpConfig.getAmqpChatbotQueue());
        analystPublisher = AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                ANALYST_PUBLISHER_CLIENT_NAME, AmqpConfig.getAmqpAnalystQueue());
        dataService = DataService.create(bybitStreamService, cryptoScoutService, chatbotPublisher,
                analystPublisher);
        collectorConsumer = AmqpConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                COLLECTOR_CONSUMER_CLIENT_NAME, AmqpConfig.getAmqpCollectorQueue());
        collectorConsumer.getStreamSupplier().streamTo(dataService.getStreamConsumer());

        analystQueueConsumer = AmqpTestConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpAnalystQueue());
        chatbotQueueConsumer = AmqpTestConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpChatbotQueue());
        collectorQueuePublisher = AmqpTestPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpCollectorQueue());
        TestUtils.await(collectorQueuePublisher.start(), chatbotPublisher.start(), analystPublisher.start(),
                collectorConsumer.start(), bybitStreamService.start(), cryptoScoutService.start());
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

        analystQueueConsumer.stop();
        chatbotQueueConsumer.stop();
    }

    @Test
    void cmcFgiRequestReturnsResponse() throws Exception {
        final var fgi = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.FGI);
        assertEquals(1, cryptoScoutRepository.saveFgi(List.of(fgi), 100L));
        assertTableCount(CMC_FGI_TABLE, 1);
        final var odt = toOdt(fgi.get(UPDATE_TIME));

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, CRYPTO_SCOUT_GET_FGI),
                                new Object[]{odt, odt}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(CRYPTO_SCOUT_GET_FGI, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void cmcKline1dRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_D);
        assertEquals(1, cryptoScoutRepository.saveKline1d(List.of(kline), 101L));
        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, CRYPTO_SCOUT_GET_KLINE_1D),
                                new Object[]{symbol, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(CRYPTO_SCOUT_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void cmcKline1wRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.CRYPTO_SCOUT, MockData.Type.KLINE_W);
        assertEquals(1, cryptoScoutRepository.saveKline1w(List.of(kline), 102L));
        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, CRYPTO_SCOUT_GET_KLINE_1W),
                                new Object[]{symbol, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(CRYPTO_SCOUT_GET_KLINE_1W, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline1mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        assertEquals(1, spotRepository.saveKline1m(List.of(kline), 104L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_1M),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_1M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline5mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        assertEquals(1, spotRepository.saveKline5m(List.of(kline), 105L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_5M),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_5M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline15mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        assertEquals(1, spotRepository.saveKline15m(List.of(kline), 106L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_15M),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_15M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline60mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        assertEquals(1, spotRepository.saveKline60m(List.of(kline), 107L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_60M),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_60M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline240mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        assertEquals(1, spotRepository.saveKline240m(List.of(kline), 108L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_240M),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_240M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotKline1dRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        assertEquals(1, spotRepository.saveKline1d(List.of(kline), 109L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_1D),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotTickerRequestReturnsResponse() throws Exception {
        final var ticker = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        assertEquals(1, spotRepository.saveTicker(List.of(ticker), 110L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ticker.get(TS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_TICKER),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_TICKER, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline1mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        assertEquals(1, linearRepository.saveKline1m(List.of(kline), 111L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_1M),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_1M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline5mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        assertEquals(1, linearRepository.saveKline5m(List.of(kline), 112L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_5M),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_5M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline15mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        assertEquals(1, linearRepository.saveKline15m(List.of(kline), 113L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_15M),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_15M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline60mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        assertEquals(1, linearRepository.saveKline60m(List.of(kline), 114L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_60M),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_60M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline240mRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        assertEquals(1, linearRepository.saveKline240m(List.of(kline), 115L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_240M),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_240M, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearKline1dRequestReturnsResponse() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        assertEquals(1, linearRepository.saveKline1d(List.of(kline), 116L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_KLINE_1D),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearTickerRequestReturnsResponse() throws Exception {
        final var ticker = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        assertEquals(1, linearRepository.saveTicker(List.of(ticker), 117L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ticker.get(TS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_TICKER),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_TICKER, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotOrderBook1RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1);
        spotRepository.saveOrderBook1(List.of(ob), 118L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_1),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_1, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotOrderBook50RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_50);
        spotRepository.saveOrderBook50(List.of(ob), 119L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_50),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_50, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotOrderBook200RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_200);
        spotRepository.saveOrderBook200(List.of(ob), 120L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_200),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_200, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotOrderBook1000RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1000);
        spotRepository.saveOrderBook1000(List.of(ob), 121L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_1000),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_1000, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitSpotPublicTradeRequestReturnsResponse() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.PUBLIC_TRADE);
        spotRepository.savePublicTrade(List.of(pt), 122L);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_PUBLIC_TRADE),
                                new Object[]{BYBIT_SPOT.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_PUBLIC_TRADE, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearOrderBook1RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1);
        linearRepository.saveOrderBook1(List.of(ob), 123L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_1),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_1, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearOrderBook50RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_50);
        linearRepository.saveOrderBook50(List.of(ob), 124L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_50),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_50, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearOrderBook200RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_200);
        linearRepository.saveOrderBook200(List.of(ob), 125L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_200),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_200, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearOrderBook1000RequestReturnsResponse() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ORDER_BOOK_1000);
        linearRepository.saveOrderBook1000(List.of(ob), 126L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ORDER_BOOK_1000),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ORDER_BOOK_1000, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearPublicTradeRequestReturnsResponse() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.PUBLIC_TRADE);
        linearRepository.savePublicTrade(List.of(pt), 127L);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_PUBLIC_TRADE),
                                new Object[]{BYBIT_LINEAR.name(), BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_PUBLIC_TRADE, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void bybitLinearAllLiquidationRequestReturnsResponse() throws Exception {
        final var al = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.ALL_LIQUIDATION);
        linearRepository.saveAllLiquidation(List.of(al), 128L);
        final var t = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                        AmqpConfig.getAmqpCollectorRoutingKey(),
                        Message.of(Message.Command.of(Message.Type.REQUEST, ANALYST, BYBIT_GET_ALL_LIQUIDATION),
                                new Object[]{BTC_USDT, from, from}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(COLLECTOR, message.command().source());
        assertEquals(BYBIT_GET_ALL_LIQUIDATION, message.command().method());
        assertNotNull(message.value());
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> analystQueueConsumer.stop()
                .whenComplete(() -> chatbotQueueConsumer.stop()
                        .whenComplete(() -> collectorQueuePublisher.stop()
                                .whenComplete(() -> chatbotPublisher.stop()
                                        .whenComplete(() -> analystPublisher.stop()
                                                .whenComplete(() -> collectorConsumer.stop()
                                                        .whenComplete(() -> bybitStreamService.stop()
                                                                .whenComplete(() -> cryptoScoutService.stop()
                                                                        .whenComplete(() -> collectorDataSource.stop()
                                                                                .whenComplete(() -> reactor.breakEventloop()
                                                                                ))))))))));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }

    final static class Config {
        private Config() {
            throw new UnsupportedOperationException();
        }

        static final String COLLECTOR_CONSUMER_CLIENT_NAME = "collector-consumer";
        static final String ANALYST_PUBLISHER_CLIENT_NAME = "analyst-publisher";
        static final String CHATBOT_PUBLISHER_CLIENT_NAME = "chatbot-publisher";
    }
}
