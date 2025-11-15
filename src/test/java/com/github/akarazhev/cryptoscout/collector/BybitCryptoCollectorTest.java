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
import com.github.akarazhev.cryptoscout.collector.db.CollectorDataSource;
import com.github.akarazhev.cryptoscout.collector.db.StreamOffsetsRepository;
import com.github.akarazhev.cryptoscout.test.DBUtils;
import com.github.akarazhev.cryptoscout.test.PodmanCompose;
import io.activej.eventloop.Eventloop;
import io.activej.promise.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.BYBIT_LPL_TABLE;
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
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ALL_LIQUIDATION_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_LINEAR_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1000_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_1_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_200_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_ORDER_BOOK_50_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.TA_SPOT_PUBLIC_TRADE_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.CMC.CMC_FGI_TABLE;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM_OFFSETS_TABLE;

final class BybitCryptoCollectorTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static CollectorDataSource dataSource;
    private static BybitLinearRepository linearRepository;
    private static BybitSpotRepository spotRepository;
    private static StreamOffsetsRepository streamOffsetsRepository;
    private static BybitCryptoCollector collector;

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.builder()
                .withCurrentThread()
                .build();
        dataSource = CollectorDataSource.create(reactor, executor);
        linearRepository = BybitLinearRepository.create(reactor, dataSource);
        spotRepository = BybitSpotRepository.create(reactor, dataSource);
        streamOffsetsRepository = StreamOffsetsRepository.create(reactor, dataSource);
        collector = BybitCryptoCollector.create(reactor, executor, streamOffsetsRepository, spotRepository,
                linearRepository);
        TestUtils.await(collector.start());
    }

    @BeforeEach
    void before() {
        DBUtils.deleteFromTables(dataSource.getDataSource(),
                LINEAR_KLINE_1M_TABLE,
                LINEAR_KLINE_5M_TABLE,
                LINEAR_KLINE_15M_TABLE,
                LINEAR_KLINE_60M_TABLE,
                LINEAR_KLINE_240M_TABLE,
                LINEAR_KLINE_1D_TABLE,
                LINEAR_TICKERS_TABLE,

                SPOT_KLINE_1M_TABLE,
                SPOT_KLINE_5M_TABLE,
                SPOT_KLINE_15M_TABLE,
                SPOT_KLINE_60M_TABLE,
                SPOT_KLINE_240M_TABLE,
                SPOT_KLINE_1D_TABLE,
                SPOT_TICKERS_TABLE,

                TA_LINEAR_PUBLIC_TRADE_TABLE,
                TA_LINEAR_ORDER_BOOK_1_TABLE,
                TA_LINEAR_ORDER_BOOK_50_TABLE,
                TA_LINEAR_ORDER_BOOK_200_TABLE,
                TA_LINEAR_ORDER_BOOK_1000_TABLE,
                TA_LINEAR_ALL_LIQUIDATION_TABLE,

                TA_SPOT_PUBLIC_TRADE_TABLE,
                TA_SPOT_ORDER_BOOK_1_TABLE,
                TA_SPOT_ORDER_BOOK_50_TABLE,
                TA_SPOT_ORDER_BOOK_200_TABLE,
                TA_SPOT_ORDER_BOOK_1000_TABLE,

                CMC_FGI_TABLE,
                BYBIT_LPL_TABLE,
                STREAM_OFFSETS_TABLE
        );
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() -> collector.stop()
                .whenComplete(() -> dataSource.stop()
                        .whenComplete(() -> reactor.breakEventloop())));
        reactor.run();
        executor.shutdown();
        PodmanCompose.down();
    }
}
