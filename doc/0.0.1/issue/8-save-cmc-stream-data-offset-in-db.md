# Issue 8: Save `CMC` stream data offset in the database

In this `crypto-scout-collector` project we are going to save `CMC` stream data message offset in the database after
processing instead of saving it on the `rabbitmq` server.

## Roles

Take the following roles:

- Expert java engineer.
- Expert database engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Use the current technology stack.
- Implementation must be production ready and to be optimized to process a lot of the data.
- Do not hallucinate.

## Tasks

- As the expert java engineer review the current `crypto-scout-collector` project implementation and update it by
  saving `CMC` stream data message offset in the database after processing. The implementation of the stream consumer
  is here: `StreamConsumer`, processing the data is here: `CmcParserCollector`, saving the data is here:
  `CmcParserRepository`.
- As the expert java engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the expert database engineer review and update the `init.sql` schema by supporting offset.
- As the expert database engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `8-save-cmc-stream-data-offset-in-db.md` file with your resolution.
- As the technical writer propose `git` commit message.

## Resolution

### Approach

- **External offset tracking for CMC stream.** Store the last processed RabbitMQ Stream offset in the DB table
  `crypto_scout.stream_offsets` and start the CMC consumer from `offset + 1` on restart.
- **Atomicity.** Insert processed data and update the stored offset within the same DB transaction to guarantee
  at-least-once semantics without drifting offsets.
- **Server-side offset tracking disabled for CMC.** We keep RabbitMQ server-side tracking for Bybit streams but disable
  it for CMC to rely on our DB-backed offsets.

### Code changes

- **Consumer (`src/main/java/com/github/akarazhev/cryptoscout/collector/StreamConsumer.java`)**
    - Use `.noTrackingStrategy()` and `subscriptionListener` for the CMC consumer to choose start offset from DB via
      `CmcParserRepository.getStreamOffset(...)` and `OffsetSpecification.offset(saved+1)` (fallback to `first`).
    - Pass the current message offset to processing: `cmcParserCollector.save(context.offset(), payload)`.
    - Avoid `context.storeOffset()` for CMC; keep it for Bybit streams.

- **Collector (`src/main/java/com/github/akarazhev/cryptoscout/collector/CmcParserCollector.java`)**
    - Accept `save(long offset, Payload<...>)` and buffer `{offset, payload}`.
    - On flush, compute the max processed offset and call
      `CmcParserRepository.insertFgi(fgi, stream, maxOffset)` to write data and update the offset
      atomically.

- **Repository (`src/main/java/com/github/akarazhev/cryptoscout/collector/db/CmcParserRepository.java`)**
    - New methods:
        - `OptionalLong getStreamOffset(String stream)`
        - `int upsertStreamOffset(String stream, long offset)`
        - `int insertFgi(List<Map<String, Object>> fgis, String stream, long offset)` (transactional)
    - New SQL constants in `collector/db/Constants.java` under `Offsets` (SELECT/UPSERT).

- **DI wiring (`src/main/java/com/github/akarazhev/cryptoscout/module/CollectorModule.java`)**
    - Provide `CmcParserRepository` to `StreamConsumer.create(...)`.

### Database schema

- `script/init.sql`
    - Add table `crypto_scout.stream_offsets(stream TEXT PRIMARY KEY, "offset" BIGINT NOT NULL, updated_at TIMESTAMPTZ)`.
    - Set owner to `crypto_scout_db` and rely on existing grants/default privileges.

### Testing

1. Start TimescaleDB with updated `init.sql` (new deployments) or apply the `stream_offsets` DDL manually to existing
   DBs.
2. Run the collector and publish CMC FGI messages to `cmc-parser-stream`.
3. Verify data in `crypto_scout.cmc_fgi` and a row in `crypto_scout.stream_offsets` for the stream name from
   `AmqpConfig.getAmqpMetricsCmcStream()`.
4. Stop the app. Publish additional messages. Restart the app.
5. Confirm consumption resumes from the stored offset (`offset + 1`) with no data loss, and the offset advances after
   flushes.

### Migration note

- Existing environments must add the `stream_offsets` table manually if the DB was initialized before this change
  (or re-initialize the data directory so `init.sql` runs again).