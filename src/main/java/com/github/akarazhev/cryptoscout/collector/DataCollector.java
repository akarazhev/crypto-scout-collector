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
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;

public final class DataCollector extends AbstractReactive {
    private final static Logger LOGGER = LoggerFactory.getLogger(DataCollector.class);
    private final BybitCryptoCollector bybitCryptoCollector;
    private final BybitTaCryptoCollector bybitTaCryptoCollector;
    private final BybitParserCollector bybitParserCollector;
    private final CmcParserCollector cmcParserCollector;
    private final AmqpPublisher chatbotPublisher;
    private final AmqpPublisher analystPublisher;

    public static DataCollector create(final NioReactor reactor,
                                       final BybitCryptoCollector bybitCryptoCollector,
                                       final BybitTaCryptoCollector bybitTaCryptoCollector,
                                       final BybitParserCollector bybitParserCollector,
                                       final CmcParserCollector cmcParserCollector,
                                       final AmqpPublisher chatbotPublisher,
                                       final AmqpPublisher analystPublisher) {
        return new DataCollector(reactor, bybitCryptoCollector, bybitTaCryptoCollector, bybitParserCollector,
                cmcParserCollector, chatbotPublisher, analystPublisher);
    }

    private DataCollector(final NioReactor reactor,
                          final BybitCryptoCollector bybitCryptoCollector,
                          final BybitTaCryptoCollector bybitTaCryptoCollector,
                          final BybitParserCollector bybitParserCollector,
                          final CmcParserCollector cmcParserCollector,
                          final AmqpPublisher chatbotPublisher,
                          final AmqpPublisher analystPublisher) {
        super(reactor);
        this.bybitCryptoCollector = bybitCryptoCollector;
        this.bybitTaCryptoCollector = bybitTaCryptoCollector;
        this.bybitParserCollector = bybitParserCollector;
        this.cmcParserCollector = cmcParserCollector;
        this.chatbotPublisher = chatbotPublisher;
        this.analystPublisher = analystPublisher;
    }

    public void handleMessage(final byte[] body) {
        try {
            @SuppressWarnings("unchecked") final var message =
                    (Message<Object[]>) JsonUtils.bytes2Object(body, Message.class);
            process(message);
        } catch (final Exception e) {
            LOGGER.error("Failed to process message", e);
        }
    }

    private void process(final Message<Object[]> message) {
        final var command = message.command();
        switch (command.type()) {
            case Message.Type.REQUEST -> {
                switch (command.method()) {
                    case Method.CMC_PARSER_GET_KLINE_1D -> {
                        final var args = message.value();
                        cmcParserCollector.getKline1d((String) args[0], (OffsetDateTime) args[1], (OffsetDateTime) args[2]).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_KLINE_1D, klines));
                    }

                    case Method.CMC_PARSER_GET_KLINE_1W -> {
                        final var args = message.value();
                        cmcParserCollector.getKline1w((String) args[0], (OffsetDateTime) args[1], (OffsetDateTime) args[2]).
                                whenResult(klines ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_KLINE_1W, klines));
                    }

                    case Method.CMC_PARSER_GET_FGI -> {
                        final var args = message.value();
                        cmcParserCollector.getFgi((OffsetDateTime) args[0], (OffsetDateTime) args[1]).
                                whenResult(fgis ->
                                        publishResponse(command.source(), Method.CMC_PARSER_GET_FGI, fgis));
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

        public static final String CMC_PARSER_GET_KLINE_1D = "cmcParser.getKline1d";
        public static final String CMC_PARSER_GET_KLINE_1W = "cmcParser.getKline1w";
        public static final String CMC_PARSER_GET_FGI = "cmcParser.getFgi";
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
