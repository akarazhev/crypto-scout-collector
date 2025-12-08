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
import com.github.akarazhev.cryptoscout.test.AmqpTestConsumer;
import com.github.akarazhev.cryptoscout.test.AmqpTestPublisher;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.jcryptolib.stream.Message;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.Constants.Config.ANALYST_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.collector.Constants.Config.CHATBOT_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.collector.Constants.Config.COLLECTOR_CONSUMER_CLIENT_NAME;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CTS;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.STAKE_BEGIN_TIME;
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

final class DataCollectorTest {
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

    private static AmqpPublisher chatbotPublisher;
    private static AmqpPublisher analystPublisher;
    private static DataCollector dataCollector;
    private static AmqpConsumer collectorConsumer;

    private static AmqpTestPublisher collectorQueuePublisher;
    private static AmqpTestConsumer analystQueueConsumer;
    private static AmqpTestConsumer chatbotQueueConsumer;

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

        chatbotPublisher = AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                CHATBOT_PUBLISHER_CLIENT_NAME, AmqpConfig.getAmqpChatbotQueue());
        analystPublisher = AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                ANALYST_PUBLISHER_CLIENT_NAME, AmqpConfig.getAmqpAnalystQueue());

        dataCollector = DataCollector.create(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector,
                bybitParserCollector, cmcParserCollector, chatbotPublisher, analystPublisher);
        collectorConsumer = AmqpConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                COLLECTOR_CONSUMER_CLIENT_NAME, AmqpConfig.getAmqpCollectorQueue(), dataCollector::handleMessage);

        analystQueueConsumer = AmqpTestConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpAnalystQueue());
        chatbotQueueConsumer = AmqpTestConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpChatbotQueue());
        collectorQueuePublisher = AmqpTestPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                AmqpConfig.getAmqpCollectorQueue());
        TestUtils.await(collectorQueuePublisher.start(), chatbotPublisher.start(), analystPublisher.start(),
                collectorConsumer.start(), bybitCryptoCollector.start(), bybitParserCollector.start(),
                bybitTaCryptoCollector.start(), cmcParserCollector.start());
    }

    @Test
    void testShouldCmcParserDataBeProcessed() throws Exception {
        final var fgi = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.FGI);
        assertEquals(1, cmcParserRepository.saveFgi(List.of(fgi), 100L));
        assertTableCount(CMC_FGI_TABLE, 1);
        final var odt = toOdt(fgi.get(UPDATE_TIME));

        TestUtils.await(collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                        DataCollector.Method.CMC_PARSER_GET_FGI), new Object[]{odt, odt}))
                .whenComplete(analystQueueConsumer::start));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.CMC_PARSER_GET_FGI, message.command().method());
        assertNotNull(message.value());
    }

    @Test
    void testShouldCmcParserKline1dDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.KLINE_D);
        assertEquals(1, cmcParserRepository.saveKline1d(List.of(kline), 101L));
        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                        DataCollector.Method.CMC_PARSER_GET_KLINE_1D), new Object[]{symbol, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.CMC_PARSER_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
        assertEquals(cmcParserRepository.getKline1d(symbol, from, from), message.value());
    }

    @Test
    void testShouldCmcParserKline1wDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.CMC_PARSER, MockData.Type.KLINE_W);
        assertEquals(1, cmcParserRepository.saveKline1w(List.of(kline), 102L));
        final var from = toOdt(((Map<?, ?>) ((Map<?, ?>) ((List<?>) kline.get(QUOTES)).getFirst()).get(QUOTE)).get(TIMESTAMP));
        final var symbol = (String) kline.get(SYMBOL);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                        DataCollector.Method.CMC_PARSER_GET_KLINE_1W), new Object[]{symbol, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.CMC_PARSER_GET_KLINE_1W, message.command().method());
        assertNotNull(message.value());
        assertEquals(cmcParserRepository.getKline1w(symbol, from, from), message.value());
    }

    @Test
    void testShouldBybitParserLplDataBeProcessed() throws Exception {
        final var lpl = MockData.get(MockData.Source.BYBIT_PARSER, MockData.Type.LPL);
        assertEquals(1, bybitParserRepository.saveLpl(List.of(lpl), 103L));
        final var odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) lpl.get(STAKE_BEGIN_TIME)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                        DataCollector.Method.BYBIT_PARSER_GET_LPL), new Object[]{odt}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_PARSER_GET_LPL, message.command().method());
        assertNotNull(message.value());
        assertEquals(bybitParserRepository.getLpl(odt), message.value());
    }

    @Test
    void testShouldBybitSpotKline1mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_1);
        assertEquals(1, spotRepository.saveKline1m(List.of(kline), 104L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_1M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_1M, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline1m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotKline5mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_5);
        assertEquals(1, spotRepository.saveKline5m(List.of(kline), 105L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_5M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_5M, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline5m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotKline15mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_15);
        assertEquals(1, spotRepository.saveKline15m(List.of(kline), 106L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_15M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_15M, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline15m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotKline60mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_60);
        assertEquals(1, spotRepository.saveKline60m(List.of(kline), 107L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_60M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_60M, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline60m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotKline240mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_240);
        assertEquals(1, spotRepository.saveKline240m(List.of(kline), 108L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_240M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_240M, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline240m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotKline1dDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.KLINE_D);
        assertEquals(1, spotRepository.saveKline1d(List.of(kline), 109L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_1D),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getKline1d(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitSpotTickerDataBeProcessed() throws Exception {
        final var ticker = MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS);
        assertEquals(1, spotRepository.saveTicker(List.of(ticker), 110L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ticker.get(TS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_TICKER),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_TICKER, message.command().method());
        assertNotNull(message.value());
        assertEquals(spotRepository.getTicker(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline1mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_1);
        assertEquals(1, linearRepository.saveKline1m(List.of(kline), 111L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_1M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_1M, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline1m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline5mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_5);
        assertEquals(1, linearRepository.saveKline5m(List.of(kline), 112L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_5M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_5M, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline5m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline15mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_15);
        assertEquals(1, linearRepository.saveKline15m(List.of(kline), 113L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_15M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_15M, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline15m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline60mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_60);
        assertEquals(1, linearRepository.saveKline60m(List.of(kline), 114L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_60M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_60M, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline60m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline240mDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_240);
        assertEquals(1, linearRepository.saveKline240m(List.of(kline), 115L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_240M),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_240M, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline240m(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearKline1dDataBeProcessed() throws Exception {
        final var kline = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.KLINE_D);
        assertEquals(1, linearRepository.saveKline1d(List.of(kline), 116L));
        final var start = ((Map<?, ?>) ((List<?>) kline.get(DATA)).getFirst()).get(START);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) start), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_KLINE_1D),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_KLINE_1D, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getKline1d(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitLinearTickerDataBeProcessed() throws Exception {
        final var ticker = MockData.get(MockData.Source.BYBIT_LINEAR, MockData.Type.TICKERS);
        assertEquals(1, linearRepository.saveTicker(List.of(ticker), 117L));
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ticker.get(TS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_GET_TICKER),
                        new Object[]{BybitCryptoCollector.Type.BYBIT_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_GET_TICKER, message.command().method());
        assertNotNull(message.value());
        assertEquals(linearRepository.getTicker(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaSpotOrderBook1DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1);
        taSpotRepository.saveOrderBook1(List.of(ob), 118L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1, message.command().method());
        assertNotNull(message.value());
        assertEquals(taSpotRepository.getOrderBook1(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaSpotOrderBook50DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_50);
        taSpotRepository.saveOrderBook50(List.of(ob), 119L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_50),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_50, message.command().method());
        assertNotNull(message.value());
        assertEquals(taSpotRepository.getOrderBook50(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaSpotOrderBook200DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_200);
        taSpotRepository.saveOrderBook200(List.of(ob), 120L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_200),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_200, message.command().method());
        assertNotNull(message.value());
        assertEquals(taSpotRepository.getOrderBook200(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaSpotOrderBook1000DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.ORDER_BOOK_1000);
        taSpotRepository.saveOrderBook1000(List.of(ob), 121L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1000),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1000, message.command().method());
        assertNotNull(message.value());
        assertEquals(taSpotRepository.getOrderBook1000(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaSpotPublicTradeDataBeProcessed() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_SPOT, MockData.Type.PUBLIC_TRADE);
        taSpotRepository.savePublicTrade(List.of(pt), 122L);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_PUBLIC_TRADE),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_SPOT.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_PUBLIC_TRADE, message.command().method());
        assertNotNull(message.value());
        assertEquals(taSpotRepository.getPublicTrade(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearOrderBook1DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1);
        taLinearRepository.saveOrderBook1(List.of(ob), 123L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getOrderBook1(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearOrderBook50DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_50);
        taLinearRepository.saveOrderBook50(List.of(ob), 124L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_50),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_50, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getOrderBook50(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearOrderBook200DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_200);
        taLinearRepository.saveOrderBook200(List.of(ob), 125L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_200),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_200, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getOrderBook200(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearOrderBook1000DataBeProcessed() throws Exception {
        final var ob = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ORDER_BOOK_1000);
        taLinearRepository.saveOrderBook1000(List.of(ob), 126L);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) ob.get(CTS)), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1000),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ORDER_BOOK_1000, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getOrderBook1000(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearPublicTradeDataBeProcessed() throws Exception {
        final var pt = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.PUBLIC_TRADE);
        taLinearRepository.savePublicTrade(List.of(pt), 127L);
        final var t = ((Map<?, ?>) ((List<?>) pt.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_PUBLIC_TRADE),
                        new Object[]{BybitTaCryptoCollector.Type.BYBIT_TA_LINEAR.name(), BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_PUBLIC_TRADE, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getPublicTrade(BTC_USDT, from, from), message.value());
    }

    @Test
    void testShouldBybitTaLinearAllLiquidationDataBeProcessed() throws Exception {
        final var al = MockData.get(MockData.Source.BYBIT_TA_LINEAR, MockData.Type.ALL_LIQUIDATION);
        taLinearRepository.saveAllLiquidation(List.of(al), 128L);
        final var t = ((Map<?, ?>) ((List<?>) al.get(DATA)).getFirst()).get(T);
        final var from = OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) t), ZoneOffset.UTC);

        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(),
                AmqpConfig.getAmqpCollectorRoutingKey(),
                Message.of(Message.Command.of(Message.Type.REQUEST, DataCollector.Source.ANALYST,
                                DataCollector.Method.BYBIT_TA_GET_ALL_LIQUIDATION),
                        new Object[]{BTC_USDT, from, from}));
        final var message = TestUtils.await(analystQueueConsumer.getMessage());

        assertNotNull(message);
        assertEquals(Message.Type.RESPONSE, message.command().type());
        assertEquals(DataCollector.Source.COLLECTOR, message.command().source());
        assertEquals(DataCollector.Method.BYBIT_TA_GET_ALL_LIQUIDATION, message.command().method());
        assertNotNull(message.value());
        assertEquals(taLinearRepository.getAllLiquidation(BTC_USDT, from, from), message.value());
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> analystQueueConsumer.stop()
                .whenComplete(() -> chatbotQueueConsumer.stop()
                        .whenComplete(() -> collectorQueuePublisher.stop()
                                .whenComplete(() -> chatbotPublisher.stop()
                                        .whenComplete(() -> analystPublisher.stop()
                                                .whenComplete(() -> collectorConsumer.stop()
                                                        .whenComplete(() -> bybitCryptoCollector.stop()
                                                                .whenComplete(() -> bybitParserCollector.stop()
                                                                        .whenComplete(() -> bybitTaCryptoCollector.stop()
                                                                                .whenComplete(() -> cmcParserCollector.stop()
                                                                                        .whenComplete(() -> dataSource.stop()
                                                                                                .whenComplete(() -> reactor.breakEventloop()
                                                                                                ))))))))))));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }
}
