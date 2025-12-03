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

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.FROM_WITH_SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.SYMBOL;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.LAST_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Range.TO_WITH_SYMBOL;

final class DBUtils {

    static List<Map<String, Object>> fetchRangeBySymbol(final DataSource dataSource, final String sql, final String symbol,
                                                        final OffsetDateTime from, final OffsetDateTime to,
                                                        final String... columns) throws SQLException {
        final var results = new ArrayList<Map<String, Object>>();
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(sql)) {
            ps.setString(SYMBOL, symbol);
            ps.setObject(FROM_WITH_SYMBOL, from);
            ps.setObject(TO_WITH_SYMBOL, to);
            try (final var rs = ps.executeQuery()) {
                while (rs.next()) {
                    final var row = new HashMap<String, Object>();
                    for (var i = 0; i < columns.length; i++) {
                        row.put(columns[i], rs.getObject(i + 1));
                    }

                    results.add(row);
                }
            }
        }

        return results;
    }

    static void updateOffset(final PreparedStatement ps, final String stream, final long offset) throws SQLException {
        ps.setString(STREAM, stream);
        ps.setLong(LAST_OFFSET, offset);
        ps.executeUpdate();
    }
}
