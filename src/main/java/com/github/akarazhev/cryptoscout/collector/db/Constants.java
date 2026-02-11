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

package com.github.akarazhev.cryptoscout.collector.db;

public final class Constants {
    private Constants() {
        throw new UnsupportedOperationException();
    }

    public final static class Range {
        private Range() {
            throw new UnsupportedOperationException();
        }

        // Time range
        static final int SYMBOL = 1;
        static final int FROM_WITH_SYMBOL = 2;
        static final int TO_WITH_SYMBOL = 3;
        static final int FROM = 1;
        static final int TO = 2;
        // Limit for queries
        static final int LIMIT = 2;
    }

    public final static class Cmc {
        private Cmc() {
            throw new UnsupportedOperationException();
        }

        // CMC fgi table
        public static final String CMC_FGI_TABLE = "crypto_scout.cmc_fgi";

        // CMC BTC-USD kline 1d table
        public static final String CMC_KLINE_1D_TABLE = "crypto_scout.cmc_kline_1d";

        // CMC BTC-USD kline 1w table
        public static final String CMC_KLINE_1W_TABLE = "crypto_scout.cmc_kline_1w";

        // CMC fgi
        static final String FGI_INSERT = "INSERT INTO " + CMC_FGI_TABLE +
                "(value, value_classification, update_time) VALUES " +
                "(?, ?, ?) ON CONFLICT (update_time) DO NOTHING";
        static final String FGI_SELECT =
                "SELECT value, value_classification, update_time FROM " + CMC_FGI_TABLE +
                        " WHERE update_time >= ? AND update_time <= ?";
        static final int FGI_VALUE = 1;
        static final int FGI_VALUE_CLASSIFICATION = 2;
        static final int FGI_UPDATE_TIME = 3;

        // CMC kline 1d
        static final String CMC_KLINE_1D_INSERT = "INSERT INTO " + CMC_KLINE_1D_TABLE +
                "(symbol, time_open, time_close, time_high, time_low, open, high, low, close, volume, market_cap, " +
                "circulating_supply, timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, timestamp) DO NOTHING";
        static final String CMC_KLINE_1D_SELECT_BY_SYMBOL =
                "SELECT symbol, time_open, time_close, time_high, time_low, open, high, low, close, volume, " +
                        "market_cap, circulating_supply, timestamp FROM " + CMC_KLINE_1D_TABLE +
                        " WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?";

        // CMC kline 1w
        static final String CMC_KLINE_1W_INSERT = "INSERT INTO " + CMC_KLINE_1W_TABLE +
                "(symbol, time_open, time_close, time_high, time_low, open, high, low, close, volume, market_cap, " +
                "circulating_supply, timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, timestamp) DO NOTHING";
        static final String CMC_KLINE_1W_SELECT_BY_SYMBOL =
                "SELECT symbol, time_open, time_close, time_high, time_low, open, high, low, close, volume, " +
                        "market_cap, circulating_supply, timestamp FROM " + CMC_KLINE_1W_TABLE +
                        " WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?";
        static final String KLINE_1W_SELECT_BY_SYMBOL =
                "SELECT symbol, timestamp, open, high, low, close, volume FROM " + CMC_KLINE_1W_TABLE +
                        " WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?";
        static final int CMC_KLINE_SYMBOL = 1;
        static final int CMC_KLINE_TIME_OPEN = 2;
        static final int CMC_KLINE_TIME_CLOSE = 3;
        static final int CMC_KLINE_TIME_HIGH = 4;
        static final int CMC_KLINE_TIME_LOW = 5;
        static final int CMC_KLINE_OPEN = 6;
        static final int CMC_KLINE_HIGH = 7;
        static final int CMC_KLINE_LOW = 8;
        static final int CMC_KLINE_CLOSE = 9;
        static final int CMC_KLINE_VOLUME = 10;
        static final int CMC_KLINE_MARKET_CAP = 11;
        static final int CMC_KLINE_CIRCULATING_SUPPLY = 12;
        static final int CMC_KLINE_TIMESTAMP = 13;
    }

    public final static class Bybit {
        private Bybit() {
            throw new UnsupportedOperationException();
        }

        // Bybit spot tables
        public static final String SPOT_KLINE_1M_TABLE = "crypto_scout.bybit_spot_kline_1m";
        public static final String SPOT_KLINE_5M_TABLE = "crypto_scout.bybit_spot_kline_5m";
        public static final String SPOT_KLINE_15M_TABLE = "crypto_scout.bybit_spot_kline_15m";
        public static final String SPOT_KLINE_60M_TABLE = "crypto_scout.bybit_spot_kline_60m";
        public static final String SPOT_KLINE_240M_TABLE = "crypto_scout.bybit_spot_kline_240m";
        public static final String SPOT_KLINE_1D_TABLE = "crypto_scout.bybit_spot_kline_1d";
        public static final String SPOT_TICKERS_TABLE = "crypto_scout.bybit_spot_tickers";

        // Bybit ta spot tables
        public static final String SPOT_PUBLIC_TRADE_TABLE = "crypto_scout.bybit_spot_public_trade";
        public static final String SPOT_ORDER_BOOK_1_TABLE = "crypto_scout.bybit_spot_order_book_1";
        public static final String SPOT_ORDER_BOOK_50_TABLE = "crypto_scout.bybit_spot_order_book_50";
        public static final String SPOT_ORDER_BOOK_200_TABLE = "crypto_scout.bybit_spot_order_book_200";
        public static final String SPOT_ORDER_BOOK_1000_TABLE = "crypto_scout.bybit_spot_order_book_1000";

        // Bybit linear tables
        public static final String LINEAR_KLINE_1M_TABLE = "crypto_scout.bybit_linear_kline_1m";
        public static final String LINEAR_KLINE_5M_TABLE = "crypto_scout.bybit_linear_kline_5m";
        public static final String LINEAR_KLINE_15M_TABLE = "crypto_scout.bybit_linear_kline_15m";
        public static final String LINEAR_KLINE_60M_TABLE = "crypto_scout.bybit_linear_kline_60m";
        public static final String LINEAR_KLINE_240M_TABLE = "crypto_scout.bybit_linear_kline_240m";
        public static final String LINEAR_KLINE_1D_TABLE = "crypto_scout.bybit_linear_kline_1d";
        public static final String LINEAR_TICKERS_TABLE = "crypto_scout.bybit_linear_tickers";

        // Bybit linear tables
        public static final String LINEAR_PUBLIC_TRADE_TABLE = "crypto_scout.bybit_linear_public_trade";
        public static final String LINEAR_ORDER_BOOK_1_TABLE = "crypto_scout.bybit_linear_order_book_1";
        public static final String LINEAR_ORDER_BOOK_50_TABLE = "crypto_scout.bybit_linear_order_book_50";
        public static final String LINEAR_ORDER_BOOK_200_TABLE = "crypto_scout.bybit_linear_order_book_200";
        public static final String LINEAR_ORDER_BOOK_1000_TABLE = "crypto_scout.bybit_linear_order_book_1000";
        public static final String LINEAR_ALL_LIQUIDATION_TABLE = "crypto_scout.bybit_linear_all_liquidation";

        // Spot klines (confirmed): identical schema across intervals
        static final String SPOT_KLINE_1M_INSERT = "INSERT INTO " + SPOT_KLINE_1M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_1M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_1M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String SPOT_KLINE_5M_INSERT = "INSERT INTO " + SPOT_KLINE_5M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_5M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_5M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String SPOT_KLINE_15M_INSERT = "INSERT INTO " + SPOT_KLINE_15M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_15M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_15M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String SPOT_KLINE_60M_INSERT = "INSERT INTO " + SPOT_KLINE_60M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_60M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_60M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String SPOT_KLINE_240M_INSERT = "INSERT INTO " + SPOT_KLINE_240M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_240M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_240M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String SPOT_KLINE_1D_INSERT = "INSERT INTO " + SPOT_KLINE_1D_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String SPOT_KLINE_1D_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + SPOT_KLINE_1D_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final int SPOT_KLINE_SYMBOL = 1;
        static final int SPOT_KLINE_START_TIME = 2;
        static final int SPOT_KLINE_END_TIME = 3;
        static final int SPOT_KLINE_OPEN_PRICE = 4;
        static final int SPOT_KLINE_CLOSE_PRICE = 5;
        static final int SPOT_KLINE_HIGH_PRICE = 6;
        static final int SPOT_KLINE_LOW_PRICE = 7;
        static final int SPOT_KLINE_VOLUME = 8;
        static final int SPOT_KLINE_TURNOVER = 9;

        // Linear klines (confirmed): identical schema across intervals
        static final String LINEAR_KLINE_1M_INSERT = "INSERT INTO " + LINEAR_KLINE_1M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_1M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_1M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String LINEAR_KLINE_5M_INSERT = "INSERT INTO " + LINEAR_KLINE_5M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_5M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_5M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String LINEAR_KLINE_15M_INSERT = "INSERT INTO " + LINEAR_KLINE_15M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_15M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_15M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String LINEAR_KLINE_60M_INSERT = "INSERT INTO " + LINEAR_KLINE_60M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_60M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_60M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String LINEAR_KLINE_240M_INSERT = "INSERT INTO " + LINEAR_KLINE_240M_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_240M_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_240M_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final String LINEAR_KLINE_1D_INSERT = "INSERT INTO " + LINEAR_KLINE_1D_TABLE +
                "(symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (symbol, start_time) DO NOTHING";
        static final String LINEAR_KLINE_1D_SELECT_BY_SYMBOL =
                "SELECT symbol, start_time, end_time, open_price, close_price, high_price, low_price, volume, turnover " +
                        "FROM " + LINEAR_KLINE_1D_TABLE + " WHERE symbol = ? AND start_time >= ? AND end_time <= ?";
        static final int LINEAR_KLINE_SYMBOL = 1;
        static final int LINEAR_KLINE_START_TIME = 2;
        static final int LINEAR_KLINE_END_TIME = 3;
        static final int LINEAR_KLINE_OPEN_PRICE = 4;
        static final int LINEAR_KLINE_CLOSE_PRICE = 5;
        static final int LINEAR_KLINE_HIGH_PRICE = 6;
        static final int LINEAR_KLINE_LOW_PRICE = 7;
        static final int LINEAR_KLINE_VOLUME = 8;
        static final int LINEAR_KLINE_TURNOVER = 9;

        // Spot tickers
        static final String SPOT_TICKERS_INSERT = "INSERT INTO " + SPOT_TICKERS_TABLE +
                "(symbol, timestamp, last_price, high_price_24h, low_price_24h, prev_price_24h, " +
                "volume_24h, turnover_24h, price_24h_pcnt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (symbol, timestamp) DO NOTHING";
        static final String SPOT_TICKERS_SELECT_BY_SYMBOL =
                "SELECT symbol, timestamp, last_price, high_price_24h, low_price_24h, prev_price_24h, volume_24h, " +
                        "turnover_24h, price_24h_pcnt FROM " + SPOT_TICKERS_TABLE +
                        " WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?";
        static final int SPOT_TICKERS_SYMBOL = 1;
        static final int SPOT_TICKERS_TIMESTAMP = 2;
        static final int SPOT_TICKERS_LAST_PRICE = 3;
        static final int SPOT_TICKERS_HIGH_PRICE_24H = 4;
        static final int SPOT_TICKERS_LOW_PRICE_24H = 5;
        static final int SPOT_TICKERS_PREV_PRICE_24H = 6;
        static final int SPOT_TICKERS_VOLUME_24H = 7;
        static final int SPOT_TICKERS_TURNOVER_24H = 8;
        static final int SPOT_TICKERS_PRICE_24H_PCNT = 9;

        // Linear tickers
        static final String LINEAR_TICKERS_INSERT = "INSERT INTO " + LINEAR_TICKERS_TABLE +
                "(symbol, timestamp, tick_direction, price_24h_pcnt, last_price, prev_price_24h, " +
                "high_price_24h, low_price_24h, prev_price_1h, mark_price, index_price, open_interest, " +
                "open_interest_value, turnover_24h, volume_24h, funding_interval_hour, funding_cap, " +
                "next_funding_time, funding_rate, bid1_price, bid1_size, ask1_price, ask1_size, " +
                "delivery_time, basis_rate, delivery_fee_rate, predicted_delivery_price, basis, " +
                "basis_rate_year, pre_open_price, pre_qty, cur_pre_listing_phase) VALUES (" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (symbol, timestamp) DO NOTHING";
        static final String LINEAR_TICKERS_SELECT_BY_SYMBOL =
                "SELECT symbol, timestamp, tick_direction, price_24h_pcnt, last_price, prev_price_24h, " +
                        "high_price_24h, low_price_24h, prev_price_1h, mark_price, index_price, open_interest, " +
                        "open_interest_value, turnover_24h, volume_24h, funding_interval_hour, funding_cap, " +
                        "next_funding_time, funding_rate, bid1_price, bid1_size, ask1_price, ask1_size, " +
                        "delivery_time, basis_rate, delivery_fee_rate, predicted_delivery_price, basis, " +
                        "basis_rate_year, pre_open_price, pre_qty, cur_pre_listing_phase FROM " + LINEAR_TICKERS_TABLE +
                        " WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?";
        static final int LINEAR_TICKERS_SYMBOL = 1;
        static final int LINEAR_TICKERS_TIMESTAMP = 2;
        static final int LINEAR_TICKERS_TICK_DIRECTION = 3;
        static final int LINEAR_TICKERS_PRICE_24H_PCNT = 4;
        static final int LINEAR_TICKERS_LAST_PRICE = 5;
        static final int LINEAR_TICKERS_PREV_PRICE_24H = 6;
        static final int LINEAR_TICKERS_HIGH_PRICE_24H = 7;
        static final int LINEAR_TICKERS_LOW_PRICE_24H = 8;
        static final int LINEAR_TICKERS_PREV_PRICE_1H = 9;
        static final int LINEAR_TICKERS_MARK_PRICE = 10;
        static final int LINEAR_TICKERS_INDEX_PRICE = 11;
        static final int LINEAR_TICKERS_OPEN_INTEREST = 12;
        static final int LINEAR_TICKERS_OPEN_INTEREST_VALUE = 13;
        static final int LINEAR_TICKERS_TURNOVER_24H = 14;
        static final int LINEAR_TICKERS_VOLUME_24H = 15;
        static final int LINEAR_TICKERS_FUNDING_INTERVAL_HOUR = 16;
        static final int LINEAR_TICKERS_FUNDING_CAP = 17;
        static final int LINEAR_TICKERS_NEXT_FUNDING_TIME = 18;
        static final int LINEAR_TICKERS_FUNDING_RATE = 19;
        static final int LINEAR_TICKERS_BID1_PRICE = 20;
        static final int LINEAR_TICKERS_BID1_SIZE = 21;
        static final int LINEAR_TICKERS_ASK1_PRICE = 22;
        static final int LINEAR_TICKERS_ASK1_SIZE = 23;
        static final int LINEAR_TICKERS_DELIVERY_TIME = 24;
        static final int LINEAR_TICKERS_BASIS_RATE = 25;
        static final int LINEAR_TICKERS_DELIVERY_FEE_RATE = 26;
        static final int LINEAR_TICKERS_PREDICTED_DELIVERY_PRICE = 27;
        static final int LINEAR_TICKERS_BASIS = 28;
        static final int LINEAR_TICKERS_BASIS_RATE_YEAR = 29;
        static final int LINEAR_TICKERS_PRE_OPEN_PRICE = 30;
        static final int LINEAR_TICKERS_PRE_QTY = 31;
        static final int LINEAR_TICKERS_CUR_PRE_LISTING_PHASE = 32;

        // Spot public trades
        static final String SPOT_PUBLIC_TRADE_INSERT = "INSERT INTO " + SPOT_PUBLIC_TRADE_TABLE +
                "(symbol, trade_time, price, size, taker_side, is_block_trade, is_rpi) VALUES (?, ?, ?, ?, ?, ?, ?)";
        static final String SPOT_PUBLIC_TRADE_SELECT_BY_SYMBOL =
                "SELECT symbol, trade_time, price, size, taker_side, is_block_trade, is_rpi FROM " +
                        SPOT_PUBLIC_TRADE_TABLE + " WHERE symbol = ? AND trade_time >= ? AND trade_time <= ?";
        static final int SPOT_PUBLIC_TRADE_SYMBOL = 1;
        static final int SPOT_PUBLIC_TRADE_TRADE_TIME = 2;
        static final int SPOT_PUBLIC_TRADE_PRICE = 3;
        static final int SPOT_PUBLIC_TRADE_SIZE = 4;
        static final int SPOT_PUBLIC_TRADE_TAKER_SIDE = 5;
        static final int SPOT_PUBLIC_TRADE_IS_BLOCK_TRADE = 6;
        static final int SPOT_PUBLIC_TRADE_IS_RPI = 7;

        // Linear public trades
        static final String LINEAR_PUBLIC_TRADE_INSERT = "INSERT INTO " + LINEAR_PUBLIC_TRADE_TABLE +
                "(symbol, trade_time, price, size, taker_side, is_block_trade, is_rpi) VALUES (?, ?, ?, ?, ?, ?, ?)";
        static final String LINEAR_PUBLIC_TRADE_SELECT_BY_SYMBOL =
                "SELECT symbol, trade_time, price, size, taker_side, is_block_trade, is_rpi FROM " +
                        LINEAR_PUBLIC_TRADE_TABLE + " WHERE symbol = ? AND trade_time >= ? AND trade_time <= ?";
        static final int LINEAR_PUBLIC_TRADE_SYMBOL = 1;
        static final int LINEAR_PUBLIC_TRADE_TRADE_TIME = 2;
        static final int LINEAR_PUBLIC_TRADE_PRICE = 3;
        static final int LINEAR_PUBLIC_TRADE_SIZE = 4;
        static final int LINEAR_PUBLIC_TRADE_TAKER_SIDE = 5;
        static final int LINEAR_PUBLIC_TRADE_IS_BLOCK_TRADE = 6;
        static final int LINEAR_PUBLIC_TRADE_IS_RPI = 7;

        // Order book side
        static final String ASK = "ask";
        static final String BID = "bid";

        // Spot order books (one row per level)
        static final String SPOT_ORDER_BOOK_1_INSERT = "INSERT INTO " + SPOT_ORDER_BOOK_1_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_1_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        SPOT_ORDER_BOOK_1_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String SPOT_ORDER_BOOK_50_INSERT = "INSERT INTO " + SPOT_ORDER_BOOK_50_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_50_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        SPOT_ORDER_BOOK_50_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String SPOT_ORDER_BOOK_200_INSERT = "INSERT INTO " + SPOT_ORDER_BOOK_200_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_200_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        SPOT_ORDER_BOOK_200_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String SPOT_ORDER_BOOK_1000_INSERT = "INSERT INTO " + SPOT_ORDER_BOOK_1000_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String SPOT_ORDER_BOOK_1000_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        SPOT_ORDER_BOOK_1000_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final int SPOT_ORDER_BOOK_SYMBOL = 1;
        static final int SPOT_ORDER_BOOK_ENGINE_TIME = 2;
        static final int SPOT_ORDER_BOOK_SIDE = 3;
        static final int SPOT_ORDER_BOOK_PRICE = 4;
        static final int SPOT_ORDER_BOOK_SIZE = 5;

        // Linear order books (one row per level)
        static final String LINEAR_ORDER_BOOK_1_INSERT = "INSERT INTO " + LINEAR_ORDER_BOOK_1_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String LINEAR_ORDER_BOOK_1_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        LINEAR_ORDER_BOOK_1_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String LINEAR_ORDER_BOOK_50_INSERT = "INSERT INTO " + LINEAR_ORDER_BOOK_50_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String LINEAR_ORDER_BOOK_50_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        LINEAR_ORDER_BOOK_50_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String LINEAR_ORDER_BOOK_200_INSERT = "INSERT INTO " + LINEAR_ORDER_BOOK_200_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String LINEAR_ORDER_BOOK_200_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        LINEAR_ORDER_BOOK_200_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final String LINEAR_ORDER_BOOK_1000_INSERT = "INSERT INTO " + LINEAR_ORDER_BOOK_1000_TABLE +
                "(symbol, engine_time, side, price, size) VALUES (?, ?, ?, ?, ?)";
        static final String LINEAR_ORDER_BOOK_1000_SELECT_BY_SYMBOL =
                "SELECT symbol, engine_time, side, price, size FROM " +
                        LINEAR_ORDER_BOOK_1000_TABLE + " WHERE symbol = ? AND engine_time >= ? AND engine_time <= ?";
        static final int LINEAR_ORDER_BOOK_SYMBOL = 1;
        static final int LINEAR_ORDER_BOOK_ENGINE_TIME = 2;
        static final int LINEAR_ORDER_BOOK_SIDE = 3;
        static final int LINEAR_ORDER_BOOK_PRICE = 4;
        static final int LINEAR_ORDER_BOOK_SIZE = 5;

        // Linear all liquidation
        static final String LINEAR_ALL_LIQUIDATION_INSERT = "INSERT INTO " + LINEAR_ALL_LIQUIDATION_TABLE +
                "(symbol, event_time, position_side, executed_size, bankruptcy_price) VALUES (?, ?, ?, ?, ?)";
        static final String LINEAR_ALL_LIQUIDATION_SELECT_BY_SYMBOL =
                "SELECT symbol, event_time, position_side, executed_size, bankruptcy_price FROM " +
                        LINEAR_ALL_LIQUIDATION_TABLE + " WHERE symbol = ? AND event_time >= ? AND event_time <= ?";
        static final int LINEAR_ALL_LIQUIDATION_SYMBOL = 1;
        static final int LINEAR_ALL_LIQUIDATION_EVENT_TIME = 2;
        static final int LINEAR_ALL_LIQUIDATION_POSITION_SIDE = 3;
        static final int LINEAR_ALL_LIQUIDATION_EXECUTED_SIZE = 4;
        static final int LINEAR_ALL_LIQUIDATION_BANKRUPTCY_PRICE = 5;
    }

    public final static class CmcKline1wIndicators {
        private CmcKline1wIndicators() {
            throw new UnsupportedOperationException();
        }

        // CMC kline 1w indicators table
        public static final String CMC_KLINE_1W_INDICATORS_TABLE = "crypto_scout.cmc_kline_1w_indicators";

        // Column names
        public static final String CLOSE_PRICE = "close_price";
        public static final String EMA_50 = "ema_50";
        public static final String EMA_100 = "ema_100";
        public static final String EMA_200 = "ema_200";
        public static final String SMA_50 = "sma_50";
        public static final String SMA_100 = "sma_100";
        public static final String SMA_200 = "sma_200";

        // CMC kline 1w indicators
        static final String INDICATORS_INSERT = "INSERT INTO " + CMC_KLINE_1W_INDICATORS_TABLE +
                "(symbol, timestamp, close_price, sma_50, sma_100, sma_200, ema_50, ema_100, ema_200) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (symbol, timestamp) DO UPDATE SET " +
                "close_price = EXCLUDED.close_price, " +
                "sma_50 = EXCLUDED.sma_50, " +
                "sma_100 = EXCLUDED.sma_100, " +
                "sma_200 = EXCLUDED.sma_200, " +
                "ema_50 = EXCLUDED.ema_50, " +
                "ema_100 = EXCLUDED.ema_100, " +
                "ema_200 = EXCLUDED.ema_200, " +
                "updated_at = NOW()";
        static final String INDICATORS_SELECT_BY_SYMBOL =
                "SELECT symbol, timestamp, close_price, sma_50, sma_100, sma_200, " +
                        "ema_50, ema_100, ema_200 " +
                        "FROM " + CMC_KLINE_1W_INDICATORS_TABLE +
                        " WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?";
        static final int IND_SYMBOL = 1;
        static final int IND_TIMESTAMP = 2;
        static final int IND_CLOSE_PRICE = 3;
        static final int IND_SMA_50 = 4;
        static final int IND_SMA_100 = 5;
        static final int IND_SMA_200 = 6;
        static final int IND_EMA_50 = 7;
        static final int IND_EMA_100 = 8;
        static final int IND_EMA_200 = 9;
    }

    public final static class Offsets {
        private Offsets() {
            throw new UnsupportedOperationException();
        }

        // Stream offsets table
        public static final String STREAM_OFFSETS_TABLE = "crypto_scout.stream_offsets";

        // Stream offsets
        static final String STREAM_OFFSETS_UPSERT = "INSERT INTO crypto_scout.stream_offsets(stream, \"offset\") VALUES " +
                "(?, ?) ON CONFLICT (stream) DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()";
        static final String STREAM_OFFSETS_SELECT = "SELECT \"offset\" FROM " + STREAM_OFFSETS_TABLE +
                " WHERE stream = ?";
        static final int CURRENT_OFFSET = 1;
        static final int STREAM = 1;
        static final int LAST_OFFSET = 2;
    }
}
