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
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.cryptoscout.test.StreamTestConsumer;
import com.github.akarazhev.cryptoscout.test.StreamTestPublisher;
import com.rabbitmq.stream.Environment;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    private static Environment environment;
    private static StreamTestPublisher bybitCryptoStreamPublisher;
    private static StreamTestPublisher bybitTaCryptoStreamPublisher;
    private static StreamTestPublisher bybitParserStreamPublisher;
    private static StreamTestPublisher cmcParserStreamPublisher;
    private static StreamTestConsumer bybitCryptoStreamConsumer;
    private static StreamTestConsumer bybitTaCryptoStreamConsumer;
    private static StreamTestConsumer bybitParserStreamConsumer;
    private static StreamTestConsumer cmcParserStreamConsumer;

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
        TestUtils.await(bybitCryptoCollector.start(), bybitParserCollector.start(), bybitTaCryptoCollector.start(),
                cmcParserCollector.start());

        streamConsumer = StreamConsumer.create(reactor, executor, streamOffsetsRepository, bybitCryptoCollector,
                bybitTaCryptoCollector, bybitParserCollector, cmcParserCollector);
        TestUtils.await(streamConsumer.start());

        environment = AmqpConfig.getEnvironment();
        bybitCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitCryptoStream());
        bybitTaCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitTaCryptoStream());
        bybitParserStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitParserStream());
        cmcParserStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpCmcParserStream());
        TestUtils.await(bybitCryptoStreamPublisher.start(), bybitTaCryptoStreamPublisher.start(),
                bybitParserStreamPublisher.start(), cmcParserStreamPublisher.start());

        bybitCryptoStreamConsumer = StreamTestConsumer.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitCryptoStream());
        bybitTaCryptoStreamConsumer = StreamTestConsumer.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitTaCryptoStream());
        bybitParserStreamConsumer = StreamTestConsumer.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitParserStream());
        cmcParserStreamConsumer = StreamTestConsumer.create(reactor, executor, environment,
                AmqpConfig.getAmqpCmcParserStream());
        TestUtils.await(bybitCryptoStreamConsumer.start(), bybitTaCryptoStreamConsumer.start(),
                bybitParserStreamConsumer.start(), cmcParserStreamConsumer.start());
    }

    @BeforeEach
    void before() {

    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> bybitCryptoStreamPublisher.stop()
                .whenComplete(() -> bybitTaCryptoStreamPublisher.stop()
                        .whenComplete(() -> reactor.breakEventloop())));
        reactor.run();
        environment.close();
        executor.shutdown();
        PodmanCompose.down();
    }
}
