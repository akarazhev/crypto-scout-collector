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

import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

public final class DataCollector extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataCollector.class);
    private final Executor executor;
    private final BybitCryptoCollector bybitCryptoCollector;
    private final BybitTaCryptoCollector bybitTaCryptoCollector;
    private final BybitParserCollector bybitParserCollector;
    private final CmcParserCollector cmcParserCollector;

    public static DataCollector create(final NioReactor reactor, final Executor executor,
                                       final BybitCryptoCollector bybitCryptoCollector,
                                       final BybitTaCryptoCollector bybitTaCryptoCollector,
                                       final BybitParserCollector bybitParserCollector,
                                       final CmcParserCollector cmcParserCollector) {
        return new DataCollector(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector, bybitParserCollector,
                cmcParserCollector);
    }

    private DataCollector(final NioReactor reactor, final Executor executor,
                          final BybitCryptoCollector bybitCryptoCollector,
                          final BybitTaCryptoCollector bybitTaCryptoCollector,
                          final BybitParserCollector bybitParserCollector,
                          final CmcParserCollector cmcParserCollector) {
        super(reactor);
        this.executor = executor;
        this.bybitCryptoCollector = bybitCryptoCollector;
        this.bybitTaCryptoCollector = bybitTaCryptoCollector;
        this.bybitParserCollector = bybitParserCollector;
        this.cmcParserCollector = cmcParserCollector;
    }

    @Override
    public Promise<Void> start() {
        return Promise.complete();
    }

    @Override
    public Promise<Void> stop() {
        return Promise.complete();
    }
}
