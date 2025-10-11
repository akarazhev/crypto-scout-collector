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
import com.github.akarazhev.cryptoscout.collector.CryptoBybitCollector;
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.MetricsBybitCollector;
import com.github.akarazhev.cryptoscout.collector.MetricsCmcCollector;
import com.github.akarazhev.cryptoscout.collector.db.CryptoBybitRepository;
import com.github.akarazhev.cryptoscout.collector.db.MetricsBybitRepository;
import com.github.akarazhev.cryptoscout.collector.db.MetricsCmcRepository;
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
    private CollectorDataSource jdbcDataSource(final NioReactor reactor, final Executor executor) {
        return CollectorDataSource.create(reactor, executor);
    }

    @Provides
    private MetricsBybitRepository metricsBybitRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return MetricsBybitRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private MetricsCmcRepository metricsCmcRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return MetricsCmcRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private CryptoBybitRepository cryptoBybitRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return CryptoBybitRepository.create(reactor, collectorDataSource);
    }

    @Provides
    private CryptoBybitCollector cryptoBybitCollector(final NioReactor reactor, final Executor executor,
                                                      final CryptoBybitRepository cryptoBybitRepository) {
        return CryptoBybitCollector.create(reactor, executor, cryptoBybitRepository);
    }

    @Provides
    private MetricsBybitCollector metricsBybitCollector(final NioReactor reactor, final Executor executor,
                                                        final MetricsBybitRepository metricsBybitRepository) {
        return MetricsBybitCollector.create(reactor, executor, metricsBybitRepository);
    }

    @Provides
    private MetricsCmcCollector metricsCmcCollector(final NioReactor reactor, final Executor executor,
                                                    final MetricsCmcRepository metricsCmcRepository) {
        return MetricsCmcCollector.create(reactor, executor, metricsCmcRepository);
    }

    @Provides
    @Eager
    private AmqpConsumer amqpConsumer(final NioReactor reactor, final Executor executor,
                                      final CryptoBybitCollector cryptoBybitCollector,
                                      final MetricsBybitCollector metricsBybitCollector,
                                      final MetricsCmcCollector metricsCmcCollector) {
        return AmqpConsumer.create(reactor, executor, cryptoBybitCollector, metricsBybitCollector, metricsCmcCollector);
    }
}
