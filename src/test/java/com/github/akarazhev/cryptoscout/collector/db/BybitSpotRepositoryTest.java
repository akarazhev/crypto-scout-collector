package com.github.akarazhev.cryptoscout.collector.db;

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
    public void shouldSaveKline1m() {
        System.out.println("s");
    }
}
