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

package com.github.akarazhev.cryptoscout.module;

import com.github.akarazhev.cryptoscout.collector.AmqpConsumer;
import com.github.akarazhev.cryptoscout.collector.AmqpPublisher;
import com.github.akarazhev.cryptoscout.collector.BybitTaCryptoCollector;
import com.github.akarazhev.cryptoscout.collector.DataCollector;
import com.github.akarazhev.cryptoscout.collector.StreamCollector;
import com.github.akarazhev.cryptoscout.collector.BybitCryptoCollector;
import com.github.akarazhev.cryptoscout.collector.BybitParserCollector;
import com.github.akarazhev.cryptoscout.collector.CmcParserCollector;
import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitTaSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CmcParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;

import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.module.Constants.Config.ANALYST_PUBLISHER;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.CHATBOT_PUBLISHER;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.COLLECTOR_CONSUMER;

public final class CollectorModule extends AbstractModule {

    private CollectorModule() {
    }

    public static CollectorModule create() {
        return new CollectorModule();
    }

    @Provides
    private CollectorDataSource collectorDataSource(final NioReactor reactor, final Executor executor) {
        return CollectorDataSource.create(reactor, executor);
    }

    @Provides
    private StreamOffsetsRepository streamOffsetsRepository(final NioReactor reactor,
                                                            final CollectorDataSource collectorDataSource) {
        return StreamOffsetsRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitParserRepository bybitParserRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return BybitParserRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private CmcParserRepository cmcParserRepository(final NioReactor reactor,
                                                    final CollectorDataSource collectorDataSource) {
        return CmcParserRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitSpotRepository bybitSpotRepository(final NioReactor reactor,
                                                    final CollectorDataSource collectorDataSource) {
        return BybitSpotRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitTaSpotRepository bybitTaSpotRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return BybitTaSpotRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitLinearRepository bybitLinearRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return BybitLinearRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitTaLinearRepository bybitTaLinearRepository(final NioReactor reactor,
                                                            final CollectorDataSource collectorDataSource) {
        return BybitTaLinearRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitCryptoCollector bybitCryptoCollector(final NioReactor reactor, final Executor executor,
                                                      final StreamOffsetsRepository streamOffsetsRepository,
                                                      final BybitSpotRepository bybitSpotRepository,
                                                      final BybitLinearRepository bybitLinearRepository) {
        return BybitCryptoCollector.create(reactor, executor, streamOffsetsRepository, bybitSpotRepository,
                bybitLinearRepository);
    }

    @Provides
    private BybitTaCryptoCollector bybitTaCryptoCollector(final NioReactor reactor, final Executor executor,
                                                          final StreamOffsetsRepository streamOffsetsRepository,
                                                          final BybitTaSpotRepository bybitTaSpotRepository,
                                                          final BybitTaLinearRepository bybitTaLinearRepository) {
        return BybitTaCryptoCollector.create(reactor, executor, streamOffsetsRepository, bybitTaSpotRepository,
                bybitTaLinearRepository);
    }

    @Provides
    private BybitParserCollector bybitParserCollector(final NioReactor reactor, final Executor executor,
                                                      final StreamOffsetsRepository streamOffsetsRepository,
                                                      final BybitParserRepository bybitParserRepository) {
        return BybitParserCollector.create(reactor, executor, streamOffsetsRepository, bybitParserRepository);
    }

    @Provides
    private CmcParserCollector cmcParserCollector(final NioReactor reactor, final Executor executor,
                                                  final StreamOffsetsRepository streamOffsetsRepository,
                                                  final CmcParserRepository cmcParserRepository) {
        return CmcParserCollector.create(reactor, executor, streamOffsetsRepository, cmcParserRepository);
    }

    @Provides
    @Eager
    private StreamCollector streamCollector(final NioReactor reactor, final Executor executor,
                                            final StreamOffsetsRepository streamOffsetsRepository,
                                            final BybitCryptoCollector bybitCryptoCollector,
                                            final BybitTaCryptoCollector bybitTaCryptoCollector,
                                            final BybitParserCollector bybitParserCollector,
                                            final CmcParserCollector cmcParserCollector) {
        return StreamCollector.create(reactor, executor, streamOffsetsRepository, bybitCryptoCollector,
                bybitTaCryptoCollector, bybitParserCollector, cmcParserCollector);
    }

    @Provides
    @Named(CHATBOT_PUBLISHER)
    @Eager
    private AmqpPublisher chatbotPublisher(final NioReactor reactor, final Executor executor) {
        return AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(), "chatbot-publisher",
                AmqpConfig.getAmqpChatbotQueue());
    }

    @Provides
    @Named(ANALYST_PUBLISHER)
    @Eager
    private AmqpPublisher analystPublisher(final NioReactor reactor, final Executor executor) {
        return AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(), "analyst-publisher",
                AmqpConfig.getAmqpAnalystQueue());
    }

    @Provides
    private DataCollector dataCollector(final NioReactor reactor,
                                        final Executor executor,
                                        final BybitCryptoCollector bybitCryptoCollector,
                                        final BybitTaCryptoCollector bybitTaCryptoCollector,
                                        final BybitParserCollector bybitParserCollector,
                                        final CmcParserCollector cmcParserCollector,
                                        @Named(CHATBOT_PUBLISHER) final AmqpPublisher chatbotPublisher,
                                        @Named(ANALYST_PUBLISHER) final AmqpPublisher analystPublisher) {
        return DataCollector.create(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector,
                bybitParserCollector, cmcParserCollector, chatbotPublisher, analystPublisher);
    }

    @Provides
    @Named(COLLECTOR_CONSUMER)
    @Eager
    private AmqpConsumer collectorConsumer(final NioReactor reactor, final Executor executor,
                                           final DataCollector dataCollector) {
        return AmqpConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(), "collector-consumer",
                AmqpConfig.getAmqpCollectorQueue(), dataCollector::handleMessage);
    }
}
