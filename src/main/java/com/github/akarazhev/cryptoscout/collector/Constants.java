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

final class Constants {
    private Constants() {
        throw new UnsupportedOperationException();
    }

    final static class Method {
        private Method() {
            throw new UnsupportedOperationException();
        }

        // CmcParserCollector methods
        static final String CMC_PARSER_GET_KLINE_1D = "cmcParser.getKline1d";
        static final String CMC_PARSER_GET_KLINE_1W = "cmcParser.getKline1w";
        static final String CMC_PARSER_GET_FGI = "cmcParser.getFgi";

        // BybitCryptoCollector methods
        static final String BYBIT_GET_KLINE_1M = "bybit.getKline1m";
        static final String BYBIT_GET_KLINE_5M = "bybit.getKline5m";
        static final String BYBIT_GET_KLINE_15M = "bybit.getKline15m";
        static final String BYBIT_GET_KLINE_60M = "bybit.getKline60m";
        static final String BYBIT_GET_KLINE_240M = "bybit.getKline240m";
        static final String BYBIT_GET_KLINE_1D = "bybit.getKline1d";
        static final String BYBIT_GET_TICKER = "bybit.getTicker";

        // BybitTaCryptoCollector methods
        static final String BYBIT_TA_GET_ORDER_BOOK_1 = "bybitTa.getOrderBook1";
        static final String BYBIT_TA_GET_ORDER_BOOK_50 = "bybitTa.getOrderBook50";
        static final String BYBIT_TA_GET_ORDER_BOOK_200 = "bybitTa.getOrderBook200";
        static final String BYBIT_TA_GET_ORDER_BOOK_1000 = "bybitTa.getOrderBook1000";
        static final String BYBIT_TA_GET_PUBLIC_TRADE = "bybitTa.getPublicTrade";
        static final String BYBIT_TA_GET_ALL_LIQUIDATION = "bybitTa.getAllLiquidation";

        // BybitParserCollector methods
        static final String BYBIT_PARSER_GET_LPL = "bybitParser.getLpl";
    }

    final static class Source {
        private Source() {
            throw new UnsupportedOperationException();
        }

        static final String COLLECTOR = "collector";
        static final String ANALYST = "analyst";
        static final String CHATBOT = "chatbot";
    }

    final static class Amqp {
        private Amqp() {
            throw new UnsupportedOperationException();
        }

        static final String CONTENT_TYPE_JSON = "application/json";
        static final int DELIVERY_MODE_PERSISTENT = 2;
        static final int PREFETCH_COUNT = 1;
    }
}
