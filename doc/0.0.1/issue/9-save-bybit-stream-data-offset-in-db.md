# Issue 9: Save metric `Bybit` stream data offset in the database

In this `crypto-scout-collector` project we are going to save metric `Bybit` stream data message offset in the database 
after processing instead of saving it on the `rabbitmq` server.

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.
- Expert technical writer.

## Conditions

- User sample in the `Sample` section.
- Use the best practices and design patterns.
- Use the current technology stack.
- Implementation must be production ready and to be optimized to process a lot of the data.
- Do not hallucinate.

## Tasks

- As the expert java engineer review the current `crypto-scout-collector` project implementation and update it by
  saving metric `Bybit` stream data message offset in the database after processing. The implementation of the stream 
  consumer is here: `AmqpConsumer`, processing the data is here: `BybitParserCollector`, saving the data is here:
  `BybitParserRepository`.
- As the expert java engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the expert database engineer review and update the `init.sql` schema by supporting offset.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `9-save-bybit-stream-data-offset-in-db.md` file with your resolution.
- As the technical writer propose `git` commit message.

## Sample

The offset has been implemented for the `CMC` data stream: 
- `src/main/java/com/github/akarazhev/cryptoscout/collector/AmqpConsumer.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/CmcParserCollector.java`
- `src/main/java/com/github/akarazhev/cryptoscout/collector/db/CmcParserRepository.java`
- `script/init.sql`