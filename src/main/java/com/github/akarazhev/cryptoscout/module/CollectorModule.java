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
import com.github.akarazhev.cryptoscout.collector.CryptoScoutService;
import com.github.akarazhev.cryptoscout.collector.DataService;
import com.github.akarazhev.cryptoscout.collector.StreamService;
import com.github.akarazhev.cryptoscout.collector.BybitStreamService;
import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CryptoScoutRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;

import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.module.Constants.Config.ANALYST_PUBLISHER;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.ANALYST_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.CHATBOT_PUBLISHER;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.CHATBOT_PUBLISHER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.COLLECTOR_CONSUMER;
import static com.github.akarazhev.cryptoscout.module.Constants.Config.COLLECTOR_CONSUMER_CLIENT_NAME;

public final class CollectorModule extends AbstractModule {

    private CollectorModule() {
    }

    public static CollectorModule create() {
        return new CollectorModule();
    }

    @Provides
    private BybitLinearRepository bybitLinearRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return BybitLinearRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitSpotRepository bybitSpotRepository(final NioReactor reactor,
                                                    final CollectorDataSource collectorDataSource) {
        return BybitSpotRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private CollectorDataSource collectorDataSource(final NioReactor reactor, final Executor executor) {
        return CollectorDataSource.create(reactor, executor);
    }

    @Provides
    private CryptoScoutRepository cryptoScoutRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return CryptoScoutRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private StreamOffsetsRepository streamOffsetsRepository(final NioReactor reactor,
                                                            final CollectorDataSource collectorDataSource) {
        return StreamOffsetsRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private BybitStreamService bybitStreamService(final NioReactor reactor, final Executor executor,
                                                  final StreamOffsetsRepository streamOffsetsRepository,
                                                  final BybitSpotRepository bybitSpotRepository,
                                                  final BybitLinearRepository bybitLinearRepository) {
        return BybitStreamService.create(reactor, executor, streamOffsetsRepository, bybitSpotRepository,
                bybitLinearRepository);
    }

    @Provides
    private CryptoScoutService cryptoScoutService(final NioReactor reactor, final Executor executor,
                                                  final StreamOffsetsRepository streamOffsetsRepository,
                                                  final CryptoScoutRepository cryptoScoutRepository) {
        return CryptoScoutService.create(reactor, executor, streamOffsetsRepository, cryptoScoutRepository);
    }

    @Provides
    private DataService dataService(final BybitStreamService bybitStreamService,
                                    final CryptoScoutService cryptoScoutService,
                                    @Named(CHATBOT_PUBLISHER) final AmqpPublisher chatbotPublisher,
                                    @Named(ANALYST_PUBLISHER) final AmqpPublisher analystPublisher) {
        return DataService.create(bybitStreamService, cryptoScoutService, chatbotPublisher, analystPublisher);
    }

    @Provides
    @Eager
    private StreamService streamService(final NioReactor reactor, final Executor executor,
                                        final StreamOffsetsRepository streamOffsetsRepository,
                                        final BybitStreamService bybitStreamService,
                                        final CryptoScoutService cryptoScoutService) {
        return StreamService.create(reactor, executor, streamOffsetsRepository, bybitStreamService, cryptoScoutService);
    }

    @Provides
    @Named(CHATBOT_PUBLISHER)
    @Eager
    private AmqpPublisher chatbotPublisher(final NioReactor reactor, final Executor executor) {
        return AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(), CHATBOT_PUBLISHER_CLIENT_NAME,
                AmqpConfig.getAmqpChatbotQueue());
    }

    @Provides
    @Named(ANALYST_PUBLISHER)
    @Eager
    private AmqpPublisher analystPublisher(final NioReactor reactor, final Executor executor) {
        return AmqpPublisher.create(reactor, executor, AmqpConfig.getConnectionFactory(), ANALYST_PUBLISHER_CLIENT_NAME,
                AmqpConfig.getAmqpAnalystQueue());
    }

    @Provides
    @Named(COLLECTOR_CONSUMER)
    @Eager
    private AmqpConsumer collectorConsumer(final NioReactor reactor, final Executor executor,
                                           final DataService dataService) {
        final var consumer = AmqpConsumer.create(reactor, executor, AmqpConfig.getConnectionFactory(),
                COLLECTOR_CONSUMER_CLIENT_NAME, AmqpConfig.getAmqpCollectorQueue());
        consumer.getStreamSupplier().streamTo(dataService.getStreamConsumer());
        return consumer;
    }
}
