package com.github.akarazhev.cryptoscout;

import com.github.akarazhev.jcryptolib.util.JsonUtils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public final class BybitMockData {

    public enum DataType {
        // Klines
        KLINE_1("kline.1"),
        KLINE_5("kline.5"),
        KLINE_15("kline.15"),
        KLINE_60("kline.60"),
        KLINE_240("kline.240"),
        KLINE_D("kline.D"),
        // Ticker
        TICKERS("tickers"),
        // Public trade
        PUBLIC_TRADE("publicTrade"),
        // Order books
        ORDER_BOOK_1("orderbook.1"),
        ORDER_BOOK_50("orderbook.50"),
        ORDER_BOOK_200("orderbook.200"),
        ORDER_BOOK_1000("orderbook.1000");

        private final String type;

        DataType(final String type) {
            this.type = type;
        }

        public String getPath() {
            return "bybit-data/" + type + ".json";
        }
    }

    private BybitMockData() {
        throw new UnsupportedOperationException();
    }

    public static Map<String, Object> get(final DataType dataType) throws Exception {
        final var file = PodmanCompose.class.getClassLoader().getResource(dataType.getPath());
        if (file == null) {
            throw new IllegalStateException("File not found: " + dataType.getPath());
        }

        final var diskFile = Paths.get(file.toURI());
        if (!Files.exists(diskFile)) {
            throw new IllegalStateException("File not found on disk: " + diskFile);
        }

        return JsonUtils.json2Map(new String(Files.readAllBytes(diskFile)));
    }
}
