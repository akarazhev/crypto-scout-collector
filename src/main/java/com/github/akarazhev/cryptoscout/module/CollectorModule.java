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
import com.github.akarazhev.cryptoscout.collector.BybitCryptoCollector;
import com.github.akarazhev.cryptoscout.collector.BybitParserCollector;
import com.github.akarazhev.cryptoscout.collector.CmcParserCollector;
import com.github.akarazhev.cryptoscout.collector.db.BybitLinearRepository;
import com.github.akarazhev.cryptoscout.collector.db.BybitParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository;
import com.github.akarazhev.cryptoscout.collector.db.CmcParserRepository;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.reactor.nio.NioReactor;

import java.util.concurrent.Executor;

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
    private BybitLinearRepository bybitLinearRepository(final NioReactor reactor,
                                                        final CollectorDataSource collectorDataSource) {
        return BybitLinearRepository.create(reactor, collectorDataSource);
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
    private AmqpConsumer amqpConsumer(final NioReactor reactor, final Executor executor,
                                      final StreamOffsetsRepository streamOffsetsRepository,
                                      final BybitCryptoCollector bybitCryptoCollector,
                                      final BybitParserCollector bybitParserCollector,
                                      final CmcParserCollector cmcParserCollector) {
        return AmqpConsumer.create(reactor, executor, streamOffsetsRepository, bybitCryptoCollector,
                bybitParserCollector, cmcParserCollector);
    }
}
