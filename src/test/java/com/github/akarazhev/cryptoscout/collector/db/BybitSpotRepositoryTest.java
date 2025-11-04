package com.github.akarazhev.cryptoscout.collector.db;

import com.github.akarazhev.cryptoscout.BybitMockData;
import com.github.akarazhev.cryptoscout.PodmanCompose;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class BybitSpotRepositoryTest {

    @BeforeAll
    static void setup() {
        PodmanCompose.up();
    }

    @AfterAll
    static void cleanup() {
        PodmanCompose.down();
    }

    @Test
    public void shouldSaveKline1m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_1);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline5m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_5);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline15m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_15);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline60m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_60);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline240m() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_240);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline1d() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.KLINE_D);
        // TODO: implement it
    }

    @Test
    public void shouldSaveTicker() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.TICKERS);
        // TODO: implement it
    }

    @Test
    public void shouldSavePublicTrade() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.PUBLIC_TRADE);
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook1() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.ORDER_BOOK_1);
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook50() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.ORDER_BOOK_50);
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook200() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.ORDER_BOOK_200);
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook1000() throws Exception {
        final var data = BybitMockData.get(BybitMockData.DataType.ORDER_BOOK_1000);
        // TODO: implement it
    }
}
