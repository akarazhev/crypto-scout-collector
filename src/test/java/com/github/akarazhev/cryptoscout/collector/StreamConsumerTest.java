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

import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.cryptoscout.test.MockData;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import com.github.akarazhev.cryptoscout.test.StreamTestPublisher;
import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.stream.Provider;
import com.github.akarazhev.jcryptolib.stream.Source;
import com.rabbitmq.stream.Environment;
import io.activej.eventloop.Eventloop;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class StreamConsumerTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static Environment environment;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitCryptoCollector bybitCryptoCollector;
    private static BybitTaCryptoCollector bybitTaCryptoCollector;
    private static BybitParserCollector bybitParserCollector;
    private static CmcParserCollector cmcParserCollector;
    private static StreamConsumer streamConsumer;
    private static StreamTestPublisher bybitCryptoStreamPublisher;
    private static StreamTestPublisher bybitTaCryptoStreamPublisher;

    @BeforeAll
    static void setup() throws InterruptedException {
        PodmanCompose.up();

        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder().withCurrentThread().build();

        environment = AmqpConfig.getEnvironment();
        bybitCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitCryptoStream());
        bybitCryptoStreamPublisher.start();
        Thread.sleep(10);

        bybitTaCryptoStreamPublisher = StreamTestPublisher.create(reactor, executor, environment,
                AmqpConfig.getAmqpBybitTaCryptoStream());
        bybitTaCryptoStreamPublisher.start();
        Thread.sleep(10);
    }

    @Test
    void test() throws Exception {
        bybitCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST,
                MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.TICKERS)));
        bybitTaCryptoStreamPublisher.publish(Payload.of(Provider.BYBIT, Source.PMST,
                MockData.get(MockData.Source.BYBIT_SPOT, MockData.Type.ORDER_BOOK_1)));
        // TODO:
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
