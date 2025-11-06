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

package com.github.akarazhev.cryptoscout.collector.db;

public final class Constants {
    private Constants() {
        throw new UnsupportedOperationException();
    }

    final static class CMC {
        private CMC() {
            throw new UnsupportedOperationException();
        }

        static final String FGI_INSERT = "INSERT INTO crypto_scout.cmc_fgi " +
                "(score, name, timestamp, btc_price, btc_volume) VALUES " +
                "(?, ?, ?, ?, ?)";
        static final int FGI_SCORE = 1;
        static final int FGI_NAME = 2;
        static final int FGI_TIMESTAMP = 3;
        static final int FGI_BTC_PRICE = 4;
        static final int FGI_BTC_VOLUME = 5;
    }

    final static class Bybit {
        private Bybit() {
            throw new UnsupportedOperationException();
        }

        // Bybit spot tables
        static final String BYBIT_SPOT_KLINE_1M_TABLE = "crypto_scout.bybit_spot_kline_1m";
        static final String BYBIT_SPOT_KLINE_5M_TABLE = "crypto_scout.bybit_spot_kline_5m";
        static final String BYBIT_SPOT_KLINE_15M_TABLE = "crypto_scout.bybit_spot_kline_15m";
        static final String BYBIT_SPOT_KLINE_60M_TABLE = "crypto_scout.bybit_spot_kline_60m";
        static final String BYBIT_SPOT_KLINE_240M_TABLE = "crypto_scout.bybit_spot_kline_240m";
        static final String BYBIT_SPOT_KLINE_1D_TABLE = "crypto_scout.bybit_spot_kline_1d";
        static final String BYBIT_SPOT_PUBLIC_TRADE_TABLE = "crypto_scout.bybit_spot_public_trade";
        static final String BYBIT_SPOT_ORDER_BOOK_1_TABLE = "crypto_scout.bybit_spot_order_book_1";
        static final String BYBIT_SPOT_ORDER_BOOK_50_TABLE = "crypto_scout.bybit_spot_order_book_50";
        static final String BYBIT_SPOT_ORDER_BOOK_200_TABLE = "crypto_scout.bybit_spot_order_book_200";
        static final String BYBIT_SPOT_ORDER_BOOK_1000_TABLE = "crypto_scout.bybit_spot_order_book_1000";
        static final String BYBIT_SPOT_TICKERS_TABLE = "crypto_scout.bybit_spot_tickers";
        // Bybit linear tables
        static final String BYBIT_LINEAR_KLINE_1M_TABLE = "crypto_scout.bybit_linear_kline_1m";
        static final String BYBIT_LINEAR_KLINE_5M_TABLE = "crypto_scout.bybit_linear_kline_5m";
        static final String BYBIT_LINEAR_KLINE_15M_TABLE = "crypto_scout.bybit_linear_kline_15m";
        static final String BYBIT_LINEAR_KLINE_60M_TABLE = "crypto_scout.bybit_linear_kline_60m";
        static final String BYBIT_LINEAR_KLINE_240M_TABLE = "crypto_scout.bybit_linear_kline_240m";
        static final String BYBIT_LINEAR_KLINE_1D_TABLE = "crypto_scout.bybit_linear_kline_1d";
        static final String BYBIT_LINEAR_PUBLIC_TRADE_TABLE = "crypto_scout.bybit_linear_public_trade";
        static final String BYBIT_LINEAR_ORDER_BOOK_1_TABLE = "crypto_scout.bybit_linear_order_book_1";
        static final String BYBIT_LINEAR_ORDER_BOOK_50_TABLE = "crypto_scout.bybit_linear_order_book_50";
        static final String BYBIT_LINEAR_ORDER_BOOK_200_TABLE = "crypto_scout.bybit_linear_order_book_200";
        static final String BYBIT_LINEAR_ORDER_BOOK_1000_TABLE = "crypto_scout.bybit_linear_order_book_1000";
        static final String BYBIT_LINEAR_TICKERS_TABLE = "crypto_scout.bybit_linear_tickers";
        static final String BYBIT_LINEAR_ALL_LIQUIDATION_TABLE = "crypto_scout.bybit_linear_all_liqudation";

        // Bybit LPL
        static final String LPL_INSERT = "INSERT INTO crypto_scout.bybit_lpl " +
                "(return_coin, return_coin_icon, description, website, whitepaper, rules, stake_begin_time, " +
                "stake_end_time, trade_begin_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        static final int LPL_RETURN_COIN = 1;
        static final int LPL_RETURN_COIN_ICON = 2;
        static final int LPL_DESC = 3;
        static final int LPL_WEBSITE = 4;
        static final int LPL_WHITE_PAPER = 5;
        static final int LPL_RULES = 6;
        static final int LPL_STAKE_BEGIN_TIME = 7;
        static final int LPL_STAKE_END_TIME = 8;
        static final int LPL_TRADE_BEGIN_TIME = 9;

        // Spot tickers
        static final String SPOT_TICKERS_INSERT = "INSERT INTO " + BYBIT_SPOT_TICKERS_TABLE +
                "(symbol, timestamp, last_price, high_price_24h, low_price_24h, prev_price_24h, " +
                "volume_24h, turnover_24h, price_24h_pcnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        static final int SPOT_TICKERS_SYMBOL = 1;
        static final int SPOT_TICKERS_TIMESTAMP = 2;
        static final int SPOT_TICKERS_LAST_PRICE = 4;
        static final int SPOT_TICKERS_HIGH_PRICE_24H = 5;
        static final int SPOT_TICKERS_LOW_PRICE_24H = 6;
        static final int SPOT_TICKERS_PREV_PRICE_24H = 7;
        static final int SPOT_TICKERS_VOLUME_24H = 8;
        static final int SPOT_TICKERS_TURNOVER_24H = 9;
        static final int SPOT_TICKERS_PRICE_24H_PCNT = 10;

        // Spot klines (confirmed): identical schema across intervals
        static final String SPOT_KLINE_1M_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_1M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_5M_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_5M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_15M_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_15M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_60M_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_60M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_240M_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_240M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_1D_INSERT = "INSERT INTO " + BYBIT_SPOT_KLINE_1D_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final int SPOT_KLINE_SYMBOL = 1;
        static final int SPOT_KLINE_START_TIME = 2;
        static final int SPOT_KLINE_END_TIME = 3;
        static final int SPOT_KLINE_OPEN_PRICE = 4;
        static final int SPOT_KLINE_CLOSE_PRICE = 5;
        static final int SPOT_KLINE_HIGH_PRICE = 6;
        static final int SPOT_KLINE_LOW_PRICE = 7;
        static final int SPOT_KLINE_VOLUME = 8;
        static final int SPOT_KLINE_TURNOVER = 9;

        // Spot public trades
        static final String SPOT_PUBLIC_TRADE_INSERT = "INSERT INTO " + BYBIT_SPOT_PUBLIC_TRADE_TABLE +
                "(symbol, trade_time, trade_id, price, size, taker_side, is_block_trade, is_rpi) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, trade_id, trade_time) DO NOTHING";
        static final int SPOT_PUBLIC_TRADE_SYMBOL = 1;
        static final int SPOT_PUBLIC_TRADE_TRADE_TIME = 2;
        static final int SPOT_PUBLIC_TRADE_TRADE_ID = 3;
        static final int SPOT_PUBLIC_TRADE_PRICE = 4;
        static final int SPOT_PUBLIC_TRADE_SIZE = 5;
        static final int SPOT_PUBLIC_TRADE_TAKER_SIDE = 6;
        static final int SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE = 8;
        static final int SPOT_PUBLIC_TRADE_IS_RPI = 9;

        // Order book side
        static final String ASK = "ask";
        static final String BID = "bid";

        // Spot order books (one row per level)
        static final String SPOT_ORDER_BOOK_1_INSERT = "INSERT INTO " + BYBIT_SPOT_ORDER_BOOK_1_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_50_INSERT = "INSERT INTO " + BYBIT_SPOT_ORDER_BOOK_50_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_200_INSERT = "INSERT INTO " + BYBIT_SPOT_ORDER_BOOK_200_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_1000_INSERT = "INSERT INTO " + BYBIT_SPOT_ORDER_BOOK_1000_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final int SPOT_ORDER_BOOK_SYMBOL = 1;
        static final int SPOT_ORDER_BOOK_ENGINE_TIME = 2;
        static final int SPOT_ORDER_BOOK_SIDE = 3;
        static final int SPOT_ORDER_BOOK_PRICE = 4;
        static final int SPOT_ORDER_BOOK_SIZE = 5;

        // Select count
        static final String SELECT_COUNT = "SELECT COUNT(*) FROM %s";
    }

    final static class Offsets {
        private Offsets() {
            throw new UnsupportedOperationException();
        }

        static final String SELECT = "SELECT \"offset\" FROM crypto_scout.stream_offsets WHERE stream = ?";
        static final int CURRENT_OFFSET = 1;
        static final String UPSERT = "INSERT INTO crypto_scout.stream_offsets(stream, \"offset\") VALUES (?, ?) " +
                "ON CONFLICT (stream) DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()";
        static final int STREAM = 1;
        static final int LAST_OFFSET = 2;
    }
}
