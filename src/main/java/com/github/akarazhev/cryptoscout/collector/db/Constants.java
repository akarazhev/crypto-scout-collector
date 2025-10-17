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
        static final String SPOT_TICKERS_INSERT = "INSERT INTO crypto_scout.bybit_spot_tickers " +
                "(symbol, timestamp, cross_sequence, last_price, high_price_24h, low_price_24h, prev_price_24h, " +
                "volume_24h, turnover_24h, price_24h_pcnt, usd_index_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        static final int SPOT_TICKERS_SYMBOL = 1;
        static final int SPOT_TICKERS_TIMESTAMP = 2;
        static final int SPOT_TICKERS_CROSS_SEQUENCE = 3;
        static final int SPOT_TICKERS_LAST_PRICE = 4;
        static final int SPOT_TICKERS_HIGH_PRICE_24H = 5;
        static final int SPOT_TICKERS_LOW_PRICE_24H = 6;
        static final int SPOT_TICKERS_PREV_PRICE_24H = 7;
        static final int SPOT_TICKERS_VOLUME_24H = 8;
        static final int SPOT_TICKERS_TURNOVER_24H = 9;
        static final int SPOT_TICKERS_PRICE_24H_PCNT = 10;
        static final int SPOT_TICKERS_USD_INDEX_PRICE = 11;
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
