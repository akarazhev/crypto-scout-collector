# Issue 10: Save `Crypto-Bybit` stream data offset in the database

In this `crypto-scout-collector` project we are going to save `Crypto-Bybit` stream data message offset in the database
after processing instead of saving it on the `rabbitmq` server.

## Roles

Take the following roles:

- Expert java engineer.
- Expert technical writer.

## Conditions

- Use implementations of classes as samples from the `Sample` section.
- Use the best practices and design patterns.
- Use the current technology stack.
- Implementation must be production ready and to be optimized to process a lot of the data.
- Do not hallucinate.

## Tasks

- As the expert java engineer review the current `crypto-scout-collector` project implementation and update it by
  saving `Crypto-Bybit` stream data message offset in the database after processing. The implementation of the stream
  consumer is here: `AmqpConsumer`, processing the data is here: `BybitCryptoCollector`, saving the data is here:
  `BybitCryptoRepository`.
- As the expert java engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.

## Sample

The offset has been implemented for the `CMC` data stream:

- `src/main/java/com/github/akarazhev/cryptoscout/collector/AmqpConsumer.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/db/StreamOffsetsRepository.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/db/MetricsBybitRepository.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/db/MetricsCmcRepository.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/MetricsCmcCollector.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/MetricsBybitCollector.java`
