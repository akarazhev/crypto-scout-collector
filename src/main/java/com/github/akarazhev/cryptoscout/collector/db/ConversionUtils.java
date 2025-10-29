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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;

final class ConversionUtils {
    private ConversionUtils() {
        throw new UnsupportedOperationException();
    }

    public static OffsetDateTime toOffsetDateTimeFromSeconds(final long epochSeconds) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC);
    }

    public static BigDecimal toBigDecimal(final Object value) {
        return switch (value) {
            case BigDecimal bd -> bd;
            case String s -> s.isEmpty() ? null : new BigDecimal(s);
            case Number n -> new BigDecimal(n.toString());
            case null, default -> null;
        };
    }

    public static Map<String, Object> asRow(final Map<String, Object> src) {
        final var dObj = src.get(DATA);
        if (dObj instanceof Map<?, ?> m) {
            @SuppressWarnings("unchecked") final var d = (Map<String, Object>) m;
            return d;
        }
        return src;
    }

    public static OffsetDateTime toOdt(final Object value) {
        return switch (value) {
            case OffsetDateTime odt -> odt;
            case Number n -> OffsetDateTime.ofInstant(Instant.ofEpochMilli(n.longValue()), ZoneOffset.UTC);
            case null, default -> null;
        };
    }

    public static Boolean valueAsBoolean(final Object v) {
        if (v instanceof Boolean b) {
            return b;
        }

        if (v instanceof String s) {
            return Boolean.parseBoolean(s);
        }

        if (v instanceof Number n) {
            return n.intValue() != 0;
        }

        return null;
    }

    public static String getSymbol(final String topic) {
        if (topic == null || !topic.contains(".")) {
            return null;
        }

        return topic.substring(topic.lastIndexOf("."));
    }
}
