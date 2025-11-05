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

import com.github.akarazhev.cryptoscout.config.JdbcConfig;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Bybit.SELECT_COUNT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.A;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.B;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DbTestUtils {
    public static int getPublicTradeCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var rows = (List<Map<String, Object>>) payload.get(DATA);
        return rows == null ? 0 : rows.size();
    }

    public static int getOrderBookLevelsCount(final Map<String, Object> payload) {
        @SuppressWarnings("unchecked") final var row = (Map<String, Object>) payload.get(DATA);
        if (row == null) {
            return 0;
        }

        @SuppressWarnings("unchecked") final var bids = (List<List<String>>) row.get(B);
        final var b = bids == null ? 0 : bids.size();
        @SuppressWarnings("unchecked") final var asks = (List<List<String>>) row.get(A);
        final var a = asks == null ? 0 : asks.size();
        assertTrue(b > 0 || a > 0, "Orderbook should contain bids or asks");
        return b + a;
    }

    public static void assertTableCount(final String table, final long expected) throws SQLException {
        try (final var c = DriverManager.getConnection(JdbcConfig.getUrl(), JdbcConfig.getUsername(), JdbcConfig.getPassword());
             final var rs = c.createStatement().executeQuery(String.format(SELECT_COUNT, table))) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getLong(1), "Unexpected row count for table: " + table);
        }
    }
}
