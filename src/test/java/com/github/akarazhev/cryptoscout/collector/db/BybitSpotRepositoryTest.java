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
        final var kline1m = BybitMockData.get(BybitMockData.DataType.KLINE_1);
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline5m() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline15m() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline60m() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline240m() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveKline1d() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveTicker() {
        // TODO: implement it
    }

    @Test
    public void shouldSavePublicTrade() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook1() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook50() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook200() {
        // TODO: implement it
    }

    @Test
    public void shouldSaveOrderBook1000() {
        // TODO: implement it
    }
}
