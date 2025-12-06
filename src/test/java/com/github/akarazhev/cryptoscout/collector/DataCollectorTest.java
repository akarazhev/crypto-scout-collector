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
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.rabbitmq.client.ConnectionFactory;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private static DataCollector dataCollector;

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

        dataCollector = DataCollector.create(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector,
                bybitParserCollector, cmcParserCollector);

        final var factory = new ConnectionFactory();
        factory.setHost(AmqpConfig.getAmqpRabbitmqHost());
        factory.setPort(AmqpConfig.getAmqpRabbitmqPort());
        factory.setUsername(AmqpConfig.getAmqpRabbitmqUsername());
        factory.setPassword(AmqpConfig.getAmqpRabbitmqPassword());

        analystQueueConsumer = AmqpTestConsumer.create(reactor, executor, factory, AmqpConfig.getAmqpAnalystQueue());
        chatbotQueueConsumer = AmqpTestConsumer.create(reactor, executor, factory, AmqpConfig.getAmqpChatbotQueue());
        collectorQueuePublisher = AmqpTestPublisher.create(reactor, executor, factory, AmqpConfig.getAmqpCollectorQueue());
        TestUtils.await(analystQueueConsumer.start(), chatbotQueueConsumer.start(), collectorQueuePublisher.start(),
                bybitCryptoCollector.start(), bybitParserCollector.start(), bybitTaCryptoCollector.start(),
                cmcParserCollector.start(), dataCollector.start());
    }

    @Test
    void testPublishCommand() {
//        collectorQueuePublisher.publish(AmqpConfig.getAmqpCryptoScoutExchange(), AmqpConfig.getAmqpCollectorRoutingKey(),
//                Command.of(0, 0, new OffsetDateTime[]{}, 0));
//        final var command = TestUtils.await(analystQueueConsumer.getCommand());
//        assertNotNull(command);
//        assertEquals(0, command.id());
//        assertEquals(0, command.from());
//        assertEquals(0, command.size());
//        assertNotNull(command.value());
//        assertEquals(data, command.value());
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> analystQueueConsumer.stop()
                .whenComplete(() -> chatbotQueueConsumer.stop()
                        .whenComplete(() -> collectorQueuePublisher.stop()
                                .whenComplete(() -> bybitCryptoCollector.stop()
                                        .whenComplete(() -> bybitParserCollector.stop()
                                                .whenComplete(() -> bybitTaCryptoCollector.stop()
                                                        .whenComplete(() -> cmcParserCollector.stop()
                                                                .whenComplete(() -> dataCollector.stop()
                                                                        .whenComplete(() -> dataSource.stop()
                                                                                .whenComplete(() -> reactor.breakEventloop()
                                                                                ))))))))));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }
}
