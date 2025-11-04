# Issue 13: Implement `BybitSpotRepositoryTest` test

In this `crypto-scout-collector` project we are going to implement the
`com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepositoryTest` service by finishing it.

## Roles

Take the following roles:

- Expert java engineer.

## Conditions

- Use the best practices and design patterns.
- Use the current technological stack, that's: `ActiveJ 6.0`, `Java 25`, `maven 3.9.1`, `podman 5.6.2`,
  `podman-compose 1.5.0`, `timescale/timescaledb:latest-pg17`, `JUnit 5.13.4`.
- Rely on the `sample` section. Use implementation of the `PodmanCompose` service to manage containers and the
  `BybitMockData` util to get mock data.
- Implementation must be production ready.
- Do not hallucinate.

## Tasks

- As the `expert java engineer` review the current `BybitSpotRepositoryTest.java` implementation in
  `crypto-scout-collector` project and update it by implementing all methods. These methods should test all
  repository methods.
- As the `expert java engineer` recheck your proposal and make sure that they are correct and haven't missed any
  important points.

## Sample

This is the sample how the `BybitSpotRepositoryTest` implementation can look like:

```java
final class BybitPrivateStreamTest {
    private static ExecutorService executor;
    private static Eventloop reactor;
    private static IDnsClient dnsClient;
    private static HttpClient client;

    @BeforeAll
    static void setup() throws NoSuchAlgorithmException {
        executor = Executors.newVirtualThreadPerTaskExecutor();
        reactor = Eventloop.create();
        dnsClient = DnsClient.builder(reactor, inetAddress("8.8.8.8"))
                .withTimeout(Duration.ofSeconds(5))
                .build();
        client = HttpClient.builder(reactor, dnsClient)
                .withSslEnabled(SSLContext.getDefault(), executor)
                .build();
    }

    @AfterAll
    static void cleanup() {
        reactor.post(() ->
                client.stop()
                        .whenComplete(($, e) -> {
                            dnsClient.close();
                            reactor.breakEventloop();
                        })
        );
        reactor.run();
        executor.shutdown();
    }

    @Test
    public void shouldReceiveOrderDataConsumer() {
        final var config = new DataConfig.Builder()
                .streamType(StreamType.PT)
                .isUseAuth(true)
                .topic(Topic.ORDER)
                .build();
        final var results = getResults(config);
        assertFalse(results.isEmpty(), "Should receive at least one message");
        for (final var value : results) {
            assertEquals(Provider.BYBIT, value.getProvider());
            assertEquals(Source.PT, value.getSource());
            assertEquals(Topic.ORDER.toString(), value.getData().get(Constants.TOPIC_FIELD));
        }
    }

    private List<Payload<Map<String, Object>>> getResults(final DataConfig config) {
        final var bybitStream = BybitStream.create(reactor, client, config);
        final var results = Collections.synchronizedList(new ArrayList<Payload<Map<String, Object>>>());

        reactor.post(() -> bybitStream.start()
                .then(supplier -> supplier.streamTo(StreamConsumers.ofConsumer(results::add))));
        reactor.delay(Duration.ofMillis(5_000), () -> bybitStream.stop().whenResult(() -> reactor.breakEventloop()));
        reactor.run();

        return results;
    }
}
```