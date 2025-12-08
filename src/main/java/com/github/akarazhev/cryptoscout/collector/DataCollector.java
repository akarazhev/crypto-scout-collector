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

import com.github.akarazhev.cryptoscout.config.AmqpConfig;
import com.github.akarazhev.jcryptolib.stream.Message;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executor;

import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;

public final class DataCollector extends AbstractReactive {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataCollector.class);
    private final Executor executor;
    private final BybitCryptoCollector bybitCryptoCollector;
    private final BybitTaCryptoCollector bybitTaCryptoCollector;
    private final BybitParserCollector bybitParserCollector;
    private final CmcParserCollector cmcParserCollector;
    private final AmqpPublisher chatbotPublisher;
    private final AmqpPublisher analystPublisher;

    public static DataCollector create(final NioReactor reactor,
                                       final Executor executor,
                                       final BybitCryptoCollector bybitCryptoCollector,
                                       final BybitTaCryptoCollector bybitTaCryptoCollector,
                                       final BybitParserCollector bybitParserCollector,
                                       final CmcParserCollector cmcParserCollector,
                                       final AmqpPublisher chatbotPublisher,
                                       final AmqpPublisher analystPublisher) {
        return new DataCollector(reactor, executor, bybitCryptoCollector, bybitTaCryptoCollector, bybitParserCollector,
                cmcParserCollector, chatbotPublisher, analystPublisher);
    }

    private DataCollector(final NioReactor reactor,
                          final Executor executor,
                          final BybitCryptoCollector bybitCryptoCollector,
                          final BybitTaCryptoCollector bybitTaCryptoCollector,
                          final BybitParserCollector bybitParserCollector,
                          final CmcParserCollector cmcParserCollector,
                          final AmqpPublisher chatbotPublisher,
                          final AmqpPublisher analystPublisher) {
        super(reactor);
        this.executor = executor;
        this.bybitCryptoCollector = bybitCryptoCollector;
        this.bybitTaCryptoCollector = bybitTaCryptoCollector;
        this.bybitParserCollector = bybitParserCollector;
        this.cmcParserCollector = cmcParserCollector;
        this.chatbotPublisher = chatbotPublisher;
        this.analystPublisher = analystPublisher;
    }

    @SuppressWarnings("unchecked")
    public void handleMessage(final byte[] body) {
        Promise.ofBlocking(executor, () -> (Message<List<Object>>) JsonUtils.bytes2Object(body, Message.class))
                .whenResult(this::process)
                .whenException(e -> LOGGER.error("Failed to process message", e));
    }

    private void process(final Message<List<Object>> message) {
        final var command = message.command();
        switch (command.type()) {
            case Message.Type.REQUEST -> {
                switch (command.method()) {
                    case Method.CMC_PARSER_GET_KLINE_1D -> {
                        final var args = message.value();
                        cmcParserCollector.getKline1d((String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_KLINE_1D, klines));
                    }

                    case Method.CMC_PARSER_GET_KLINE_1W -> {
                        final var args = message.value();
                        cmcParserCollector.getKline1w((String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_KLINE_1W, klines));
                    }

                    case Method.CMC_PARSER_GET_FGI -> {
                        final var args = message.value();
                        cmcParserCollector.getFgi(toOdt(args.get(0)), toOdt(args.get(1))).
                                whenResult(fgis ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_FGI, fgis));
                    }

                    // BybitCryptoCollector methods
                    case Method.BYBIT_GET_KLINE_1M -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline1m(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_1M, klines));
                    }

                    case Method.BYBIT_GET_KLINE_5M -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline5m(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_5M, klines));
                    }

                    case Method.BYBIT_GET_KLINE_15M -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline15m(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_15M, klines));
                    }

                    case Method.BYBIT_GET_KLINE_60M -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline60m(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_60M, klines));
                    }

                    case Method.BYBIT_GET_KLINE_240M -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline240m(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_240M, klines));
                    }

                    case Method.BYBIT_GET_KLINE_1D -> {
                        final var args = message.value();
                        bybitCryptoCollector.getKline1d(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.BYBIT_GET_KLINE_1D, klines));
                    }

                    case Method.BYBIT_GET_TICKER -> {
                        final var args = message.value();
                        bybitCryptoCollector.getTicker(
                                BybitCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(tickers ->
                                        publishResponse(command.source(), Method.BYBIT_GET_TICKER, tickers));
                    }

                    // BybitTaCryptoCollector methods
                    case Method.BYBIT_TA_GET_ORDER_BOOK_1 -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getOrderBook1(
                                BybitTaCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_ORDER_BOOK_1, orderBooks));
                    }

                    case Method.BYBIT_TA_GET_ORDER_BOOK_50 -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getOrderBook50(
                                BybitTaCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_ORDER_BOOK_50, orderBooks));
                    }

                    case Method.BYBIT_TA_GET_ORDER_BOOK_200 -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getOrderBook200(
                                BybitTaCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_ORDER_BOOK_200, orderBooks));
                    }

                    case Method.BYBIT_TA_GET_ORDER_BOOK_1000 -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getOrderBook1000(
                                BybitTaCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_ORDER_BOOK_1000, orderBooks));
                    }

                    case Method.BYBIT_TA_GET_PUBLIC_TRADE -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getPublicTrade(
                                BybitTaCryptoCollector.Type.valueOf((String) args.get(0)),
                                (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(trades ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_PUBLIC_TRADE, trades));
                    }

                    case Method.BYBIT_TA_GET_ALL_LIQUIDATION -> {
                        final var args = message.value();
                        bybitTaCryptoCollector.getAllLiquidation(
                                (String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(liquidations ->
                                        publishResponse(command.source(), Method.BYBIT_TA_GET_ALL_LIQUIDATION, liquidations));
                    }

                    // BybitParserCollector methods
                    case Method.BYBIT_PARSER_GET_LPL -> {
                        final var args = message.value();
                        bybitParserCollector.getLpl(toOdt(args.getFirst())).
                                whenResult(lpls ->
                                        publishResponse(command.source(), Method.BYBIT_PARSER_GET_LPL, lpls));
                    }
                }
            }
        }
    }

    private <T> void publishResponse(final String source, final String method, final T data) {
        final var command = Message.Command.of(Message.Type.RESPONSE, Source.COLLECTOR, method);
        switch (source) {
            case Source.CHATBOT -> chatbotPublisher.publish(
                    AmqpConfig.getAmqpCryptoScoutExchange(),
                    AmqpConfig.getAmqpChatbotRoutingKey(),
                    Message.of(command, data)
            );

            case Source.ANALYST -> analystPublisher.publish(
                    AmqpConfig.getAmqpCryptoScoutExchange(),
                    AmqpConfig.getAmqpAnalystRoutingKey(),
                    Message.of(command, data)
            );

            default -> LOGGER.warn("Unknown source for response: {}", source);
        }
    }

    static class Method {
        private Method() {
            throw new UnsupportedOperationException();
        }

        // CmcParserCollector methods
        public static final String CMC_PARSER_GET_KLINE_1D = "cmcParser.getKline1d";
        public static final String CMC_PARSER_GET_KLINE_1W = "cmcParser.getKline1w";
        public static final String CMC_PARSER_GET_FGI = "cmcParser.getFgi";

        // BybitCryptoCollector methods
        public static final String BYBIT_GET_KLINE_1M = "bybit.getKline1m";
        public static final String BYBIT_GET_KLINE_5M = "bybit.getKline5m";
        public static final String BYBIT_GET_KLINE_15M = "bybit.getKline15m";
        public static final String BYBIT_GET_KLINE_60M = "bybit.getKline60m";
        public static final String BYBIT_GET_KLINE_240M = "bybit.getKline240m";
        public static final String BYBIT_GET_KLINE_1D = "bybit.getKline1d";
        public static final String BYBIT_GET_TICKER = "bybit.getTicker";

        // BybitTaCryptoCollector methods
        public static final String BYBIT_TA_GET_ORDER_BOOK_1 = "bybitTa.getOrderBook1";
        public static final String BYBIT_TA_GET_ORDER_BOOK_50 = "bybitTa.getOrderBook50";
        public static final String BYBIT_TA_GET_ORDER_BOOK_200 = "bybitTa.getOrderBook200";
        public static final String BYBIT_TA_GET_ORDER_BOOK_1000 = "bybitTa.getOrderBook1000";
        public static final String BYBIT_TA_GET_PUBLIC_TRADE = "bybitTa.getPublicTrade";
        public static final String BYBIT_TA_GET_ALL_LIQUIDATION = "bybitTa.getAllLiquidation";

        // BybitParserCollector methods
        public static final String BYBIT_PARSER_GET_LPL = "bybitParser.getLpl";
    }

    static class Source {
        private Source() {
            throw new UnsupportedOperationException();
        }

        public static final String COLLECTOR = "collector";
        public static final String ANALYST = "analyst";
        public static final String CHATBOT = "chatbot";
    }
}
