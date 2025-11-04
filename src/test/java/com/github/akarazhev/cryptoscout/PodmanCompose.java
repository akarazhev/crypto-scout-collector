package com.github.akarazhev.cryptoscout;

public final class PodmanCompose {

    static {
        final var file = PodmanCompose.class.getClassLoader().getResource("podman-compose.yml");
        // TODO:
    }

    private PodmanCompose() {
        throw new UnsupportedOperationException();
    }

    public static void up() {
        // podman-compose up -d
    }

    public static void down() {
        // podman-compose down
    }
}
