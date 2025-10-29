# Issue 6: Update Bybit spot repository

In this `crypto-scout-collector` project we are going to update the
`com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository` implementation by adding the
following methods:
- `saveKline15m`
- `saveKline60m`
- `saveKline240m`
- `saveKline1d`
- `savePublicTrade`
- `saveOrderBook200`

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.
- Expert technical writer.

## Conditions

- Use samples in the `Sample` section and `definition` of the data section.
- Use the best practices and design patterns.
- Use the current technology stack: - Use the current technological stack, that's: `ActiveJ 6.0`, `Java 25`,
  `maven 3.9.1`, `podman 5.6.2`, `podman-compose 1.5.0`, `timescale/timescaledb:latest-pg17`.
- Implementation must be production ready and to be optimized to process a lot of the data.
- Do not hallucinate.

## Tasks

- As the `expert java engineer` review the current `BybitSpotRepository.java` implementation in `crypto-scout-collector`
  project and update it by implementing the following methods: `saveKline15m`, `saveKline60m`, `saveKline240m`, 
  `saveKline1d`, `savePublicTrade`, `saveOrderBook200`.
- As the `expert java engineer` rely on the definition of the data section.
- As the `expert java engineer` recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `6-update-bybit-spot-repository.md` file with your resolution.

## Sample

The sample of the method to save data is here:

- `com.github.akarazhev.cryptoscout.collector.db.BybitSpotRepository.saveTicker`.

## Definition of the data

The `Bybit` spot data is defined in the `script/bybit_spot_tables.sql` script.