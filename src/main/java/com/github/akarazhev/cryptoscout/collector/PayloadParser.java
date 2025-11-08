package com.github.akarazhev.cryptoscout.collector;

import java.util.List;
import java.util.Map;

import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.A;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.B;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.CONFIRM;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.DATA;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.SNAPSHOT;
import static com.github.akarazhev.jcryptolib.bybit.Constants.Response.TYPE;
import static com.github.akarazhev.jcryptolib.util.ParserUtils.getFirstRow;

public final class PayloadParser {

    public static boolean isKlineConfirmed(final Map<String, Object> payload) {
        final var row = getFirstRow(DATA, payload);
        return row != null && row.containsKey(CONFIRM) && (Boolean) row.get(CONFIRM);
    }

    public static boolean isSnapshot(final Map<String, Object> payload) {
        return SNAPSHOT.equals(payload.get(TYPE));
    }

    public static int getRowsCount(final Map<String, Object> payload) {
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
        return b + a;
    }
}
