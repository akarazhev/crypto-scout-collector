/*
 * MIT License
 *
 * Copyright (c) 2026 Andrey Karazhev
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
import io.activej.datastream.consumer.AbstractStreamConsumer;
import io.activej.datastream.consumer.StreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.github.akarazhev.jcryptolib.util.TimeUtils.toOdt;

public final class DataService {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataService.class);
    private final BybitStreamService bybitStreamService;
    private final CryptoScoutService cryptoScoutService;
    private final AmqpPublisher chatbotPublisher;

    public static DataService create(final BybitStreamService bybitStreamService,
                                     final CryptoScoutService cryptoScoutService,
                                     final AmqpPublisher chatbotPublisher) {
        return new DataService(bybitStreamService, cryptoScoutService, chatbotPublisher);
    }

    private DataService(final BybitStreamService bybitStreamService,
                        final CryptoScoutService cryptoScoutService,
                        final AmqpPublisher chatbotPublisher) {
        this.bybitStreamService = bybitStreamService;
        this.cryptoScoutService = cryptoScoutService;
        this.chatbotPublisher = chatbotPublisher;
    }

    public StreamConsumer<byte[]> getStreamConsumer() {
        return new InternalStreamConsumer();
    }

    @SuppressWarnings("unchecked")
    private void consume(final byte[] body) {
        try {
            consume((Message<List<Object>>) JsonUtils.bytes2Object(body, Message.class));
        } catch (final Exception e) {
            LOGGER.error("Failed to process message", e);
        }
    }

    private void consume(final Message<List<Object>> message) {
        final var command = message.command();
        switch (command.type()) {
            case Message.Type.REQUEST -> {
                switch (command.method()) {
                    // CryptoScoutCollector methods
                    case Constants.Method.CRYPTO_SCOUT_GET_KLINE_1D -> {
                        final var args = message.value();
                        cryptoScoutService.getKline1d((String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.CRYPTO_SCOUT_GET_KLINE_1D, klines));
                    }

                    case Constants.Method.CRYPTO_SCOUT_GET_KLINE_1W -> {
                        final var args = message.value();
                        cryptoScoutService.getKline1w((String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.CRYPTO_SCOUT_GET_KLINE_1W, klines));
                    }

                    case Constants.Method.CRYPTO_SCOUT_GET_FGI -> {
                        final var args = message.value();
                        cryptoScoutService.getFgi(toOdt(args.get(0)), toOdt(args.get(1))).
                                whenResult(fgis ->
                                        publish(command.source(), Constants.Method.CRYPTO_SCOUT_GET_FGI, fgis));
                    }

                    // BybitCryptoCollector methods
                    case Constants.Method.BYBIT_GET_KLINE_1M -> {
                        final var args = message.value();
                        bybitStreamService.getKline1m(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_1M, klines));
                    }

                    case Constants.Method.BYBIT_GET_KLINE_5M -> {
                        final var args = message.value();
                        bybitStreamService.getKline5m(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_5M, klines));
                    }

                    case Constants.Method.BYBIT_GET_KLINE_15M -> {
                        final var args = message.value();
                        bybitStreamService.getKline15m(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_15M, klines));
                    }

                    case Constants.Method.BYBIT_GET_KLINE_60M -> {
                        final var args = message.value();
                        bybitStreamService.getKline60m(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_60M, klines));
                    }

                    case Constants.Method.BYBIT_GET_KLINE_240M -> {
                        final var args = message.value();
                        bybitStreamService.getKline240m(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_240M, klines));
                    }

                    case Constants.Method.BYBIT_GET_KLINE_1D -> {
                        final var args = message.value();
                        bybitStreamService.getKline1d(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(klines ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_KLINE_1D, klines));
                    }

                    case Constants.Method.BYBIT_GET_TICKER -> {
                        final var args = message.value();
                        bybitStreamService.getTicker(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(tickers ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_TICKER, tickers));
                    }

                    case Constants.Method.BYBIT_GET_ORDER_BOOK_1 -> {
                        final var args = message.value();
                        bybitStreamService.getOrderBook1(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_ORDER_BOOK_1, orderBooks));
                    }

                    case Constants.Method.BYBIT_GET_ORDER_BOOK_50 -> {
                        final var args = message.value();
                        bybitStreamService.getOrderBook50(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_ORDER_BOOK_50, orderBooks));
                    }

                    case Constants.Method.BYBIT_GET_ORDER_BOOK_200 -> {
                        final var args = message.value();
                        bybitStreamService.getOrderBook200(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_ORDER_BOOK_200, orderBooks));
                    }

                    case Constants.Method.BYBIT_GET_ORDER_BOOK_1000 -> {
                        final var args = message.value();
                        bybitStreamService.getOrderBook1000(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(orderBooks ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_ORDER_BOOK_1000, orderBooks));
                    }

                    case Constants.Method.BYBIT_GET_PUBLIC_TRADE -> {
                        final var args = message.value();
                        bybitStreamService.getPublicTrade(BybitStreamService.Type.valueOf((String) args.get(0)),
                                        (String) args.get(1), toOdt(args.get(2)), toOdt(args.get(3))).
                                whenResult(trades ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_PUBLIC_TRADE, trades));
                    }

                    case Constants.Method.BYBIT_GET_ALL_LIQUIDATION -> {
                        final var args = message.value();
                        bybitStreamService.getAllLiquidation((String) args.get(0), toOdt(args.get(1)), toOdt(args.get(2))).
                                whenResult(liquidations ->
                                        publish(command.source(), Constants.Method.BYBIT_GET_ALL_LIQUIDATION, liquidations));
                    }

                    default -> LOGGER.debug("Unhandled request method: {}", command.method());
                }
            }

            default -> LOGGER.debug("Unhandled message type: {}", command.type());
        }
    }

    private <T> void publish(final String source, final String method, final T data) {
        final var command = Message.Command.of(Message.Type.RESPONSE, Constants.Source.COLLECTOR, method);
        switch (source) {
            case Constants.Source.CHATBOT -> chatbotPublisher.publish(
                    AmqpConfig.getAmqpCryptoScoutExchange(),
                    AmqpConfig.getAmqpChatbotRoutingKey(),
                    Message.of(command, data)
            );

            default -> LOGGER.warn("Unknown source for response: {}", source);
        }
    }

    private final class InternalStreamConsumer extends AbstractStreamConsumer<byte[]> {

        @Override
        protected void onStarted() {
            resume(DataService.this::consume);
        }

        @Override
        protected void onEndOfStream() {
            acknowledge();
        }

        @Override
        protected void onError(final Exception e) {
            LOGGER.error("Stream error in DataService consumer", e);
        }
    }
}
