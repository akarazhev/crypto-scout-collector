# Issue 19: Implement @DataCollector.java service

In this `crypto-scout-collector` project we are going to implement the @DataCollector.java service that will listen the 
`rabbitmq` queue of messages and process them. Each message represents as a command to get certain data from the database.
There is a basic implementation of @DataCollector.java in the `crypto-scout-collector` project.

## Roles

Take the following roles:

- Expert java engineer.

## Conditions

- Rely on the examples of `Command.java` and `AmqpTestConsumer.java` below.
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `Java 25`, `ActiveJ 6.0-rc2`, `amqp-client 5.26.0`, `stream-client 1.2.0`.
- Do not hallucinate.

## Tasks

- As the `expert java engineer` review the current @DataCollector.java implementation in `crypto-scout-collector`
  project and update it by finishing implementation to listen the `rabbitmq` queue of messages and process them. 
  Each message represents as a command to get certain data from the database. The certain processing will be 
  implemented in the future. 
- As the `expert java engineer` Double-check your proposal and make sure that they are correct and haven't missed any
  important points.

## Example of Command.java implementation

```java
package com.github.akarazhev.jcryptolib.stream;

import java.util.List;

public record Command(int id, int from, List<Object> values, int size) {
}
```

## Example of AmqpTestConsumer.java implementation

```java
package com.github.akarazhev.cryptoscout.test;

import com.github.akarazhev.jcryptolib.stream.Payload;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.test.Constants.Amqp.CONSUMER_CLIENT_NAME;
import static com.github.akarazhev.cryptoscout.test.Constants.Amqp.PREFETCH_COUNT;

public final class AmqpTestConsumer extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpTestConsumer.class);
    private final Executor executor;
    private final ConnectionFactory connectionFactory;
    private final String queue;
    private final SettablePromise<Payload<Map<String, Object>>> result = new SettablePromise<>();
    private volatile Connection connection;
    private volatile Channel channel;
    private volatile String consumerTag;

    public static AmqpTestConsumer create(final NioReactor reactor, final Executor executor,
                                          final ConnectionFactory connectionFactory, final String queue) {
        return new AmqpTestConsumer(reactor, executor, connectionFactory, queue);
    }

    private AmqpTestConsumer(final NioReactor reactor, final Executor executor,
                             final ConnectionFactory connectionFactory, final String queue) {
        super(reactor);
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.queue = queue;
    }

    @Override
    public Promise<Void> start() {
        return Promise.ofBlocking(executor, () -> {
            try {
                connection = connectionFactory.newConnection(CONSUMER_CLIENT_NAME);
                channel = connection.createChannel();
                channel.basicQos(PREFETCH_COUNT);
                channel.queueDeclarePassive(queue);

                final DeliverCallback deliver = (_, delivery) -> {
                    try {
                        @SuppressWarnings("unchecked") final var payload =
                                (Payload<Map<String, Object>>) JsonUtils.bytes2Object(delivery.getBody(), Payload.class);
                        result.set(payload);
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } catch (final Exception e) {
                        try {
                            channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                        } catch (final IOException ex) {
                            LOGGER.debug("Error cancelling AMQP consumer", ex);
                        }

                        result.setException(e);
                    } finally {
                        if (consumerTag != null && channel.isOpen()) {
                            try {
                                channel.basicCancel(consumerTag);
                            } catch (final Exception ex) {
                                LOGGER.debug("Error cancelling AMQP consumer", ex);
                            }
                        }
                    }
                };

                final CancelCallback cancel = tag -> LOGGER.debug("AMQP consumer cancelled: {}", tag);
                consumerTag = channel.basicConsume(queue, false, deliver, cancel);
            } catch (final Exception ex) {
                LOGGER.error("Failed to start AmqpTestConsumer", ex);
                throw new RuntimeException(ex);
            }
        });
    }

    public Promise<Payload<Map<String, Object>>> getResult() {
        return result;
    }

    @Override
    public Promise<Void> stop() {
        return Promise.ofBlocking(executor, () -> {
            try {
                if (channel != null) {
                    try {
                        if (consumerTag != null) {
                            channel.basicCancel(consumerTag);
                            consumerTag = null;
                        }
                    } catch (final Exception ex) {
                        LOGGER.debug("Error cancelling AMQP consumer on stop", ex);
                    }

                    channel.close();
                    channel = null;
                }
            } catch (final Exception ex) {
                LOGGER.warn("Error closing AMQP channel", ex);
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                        connection = null;
                    }
                } catch (final Exception ex) {
                    LOGGER.warn("Error closing AMQP connection", ex);
                }
            }
        });
    }
}
```