_# Issue 12: Implement `PodmanCompose` service

In this `crypto-scout-collector` project we are going to implement the `com.github.akarazhev.cryptoscout.PodmanCompose`
service by finishing it.

## Roles

Take the following roles:

- Expert java engineer.

## Conditions

- Use the best practices and design patterns.
- Use the current technological stack, that's: `ActiveJ 6.0`, `Java 25`, `maven 3.9.1`, `podman 5.6.2`,
  `podman-compose 1.5.0`, `timescale/timescaledb:latest-pg17`.
- Rely on the `src/test/resources/podman-compose.yml` definition.
- Implementation must be production ready.
- Do not hallucinate.

## Tasks

- As the `expert java engineer` review the current `PodmanCompose.java` implementation in `crypto-scout-collector`
  project and update it by implementing the following methods: `up`, `down`. These methods should wait until the 
  containers are up and running or down.
- As the `expert java engineer` recheck your proposal and make sure that they are correct and haven't missed any
  important points.