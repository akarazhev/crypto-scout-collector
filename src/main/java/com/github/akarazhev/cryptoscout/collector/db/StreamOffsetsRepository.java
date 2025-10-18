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

import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.OptionalLong;

import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.CURRENT_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.LAST_OFFSET;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.SELECT;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.STREAM;
import static com.github.akarazhev.cryptoscout.collector.db.Constants.Offsets.UPSERT;

public final class StreamOffsetsRepository extends AbstractReactive implements ReactiveService {
    private final DataSource dataSource;

    private StreamOffsetsRepository(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        super(reactor);
        this.dataSource = collectorDataSource.getDataSource();
    }

    public static StreamOffsetsRepository create(final NioReactor reactor, final CollectorDataSource collectorDataSource) {
        return new StreamOffsetsRepository(reactor, collectorDataSource);
    }

    @Override
    public Promise<?> start() {
        return Promise.complete();
    }

    @Override
    public Promise<?> stop() {
        return Promise.complete();
    }

    public OptionalLong getOffset(final String stream) throws SQLException {
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(SELECT)) {
            ps.setString(STREAM, stream);
            try (final var rs = ps.executeQuery()) {
                if (rs.next()) {
                    final var offset = rs.getLong(CURRENT_OFFSET);
                    if (!rs.wasNull()) {
                        return OptionalLong.of(offset);
                    }
                }
            }
        }

        return OptionalLong.empty();
    }

    public int upsertOffset(final String stream, final long offset) throws SQLException {
        try (final var c = dataSource.getConnection();
             final var ps = c.prepareStatement(UPSERT)) {
            ps.setString(STREAM, stream);
            ps.setLong(LAST_OFFSET, offset);
            return ps.executeUpdate();
        }
    }
}
