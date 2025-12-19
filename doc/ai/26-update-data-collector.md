# Issue 26: Update @DataCollector.java service

In this `crypto-scout-collector` project we are going to update the @DataCollector.java service that will listen the
`rabbitmq` queue of messages and process them to implement request and response mechanism via `amqp`.
Each message represents as a command to get certain data from the database and send it to the response queue.
There is a core implementation of @DataCollector.java in the `crypto-scout-collector` project.

## Roles

Take the following roles:

- Expert java engineer.

## Conditions

- Rely on the examples of `Command.java` and `AmqpTestPublisher.java` below.
- Double-check your proposal and make sure that they are correct and haven't missed any important points.
- Implementation must be production ready.
- Use the best practices and design patterns.

## Constraints

- Use the current technological stack, that's: `Java 25`, `ActiveJ 6.0-rc2`, `amqp-client 5.26.0`,
  `stream-client 1.2.0`.
- Do not hallucinate.

## Tasks

- As the `expert java engineer` review the current @DataCollector.java implementation in `crypto-scout-collector`
  project and update it by integrating the `amqp` client to publish messages to `rabbitmq` queues with results.
- Each message represents as a command to get certain data from the database and send data back via `amqp`, so the
  `amqp` must support to send data back via: `chatbotPublisher`, `analystPublisher`. Declare publishers in the
  `DataCollector` class. Rely on the example of `AmqpTestPublisher.java` and `Message.java`.
- As the `expert java engineer` Double-check your proposal and make sure that they are correct and haven't missed any
  important points.

## Example of Command.java implementation

```java
package com.github.akarazhev.jcryptolib.stream;

public record Message<T>(Command command, T value) {

    public interface Command {
        enum Type {
            REQUEST,
            RESPONSE
        }

        Type getType();

        String getSource();

        String getMethod();
    }

    public static <T> Message<T> of(final Command command, final T value) {
        return new Message<>(command, value);
    }
}
```

## Example of AmqpTestPublisher.java implementation

```java
package com.github.akarazhev.cryptoscout.test;

import com.github.akarazhev.jcryptolib.stream.Message;
import com.github.akarazhev.jcryptolib.util.JsonUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.activej.async.service.ReactiveService;
import io.activej.promise.Promise;
import io.activej.reactor.AbstractReactive;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

import static com.github.akarazhev.cryptoscout.test.Constants.Amqp.CONTENT_TYPE_JSON;
import static com.github.akarazhev.cryptoscout.test.Constants.Amqp.DELIVERY_MODE_PERSISTENT;
import static com.github.akarazhev.cryptoscout.test.Constants.Amqp.PUBLISHER_CLIENT_NAME;

public final class AmqpTestPublisher extends AbstractReactive implements ReactiveService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AmqpTestPublisher.class);
    private final Executor executor;
    private final ConnectionFactory connectionFactory;
    private final String queue;
    private volatile Connection connection;
    private volatile Channel channel;

    public static AmqpTestPublisher create(final NioReactor reactor, final Executor executor,
                                           final ConnectionFactory connectionFactory, final String queue) {
        return new AmqpTestPublisher(reactor, executor, connectionFactory, queue);
    }

    private AmqpTestPublisher(final NioReactor reactor, final Executor executor,
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
                connection = connectionFactory.newConnection(PUBLISHER_CLIENT_NAME);
                channel = connection.createChannel();
                channel.confirmSelect();
                // Ensure the queue exists (will throw if it doesn't)
                channel.queueDeclarePassive(queue);
            } catch (final Exception ex) {
                LOGGER.error("Failed to start AmqpTestPublisher", ex);
                throw new RuntimeException(ex);
            }
        });
    }

    @Override
    public Promise<Void> stop() {
        return Promise.ofBlocking(executor, () -> {
            try {
                if (channel != null) {
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

    public Promise<Void> publish(final String exchange, final String routingKey, final Message<?> message) {
        return Promise.ofBlocking(executor, () -> {
            if (channel == null || !channel.isOpen()) {
                throw new IllegalStateException("AMQP channel is not open. Call start() before publish().");
            }

            try {
                final var props = new AMQP.BasicProperties.Builder()
                        .contentType(CONTENT_TYPE_JSON)
                        .deliveryMode(DELIVERY_MODE_PERSISTENT)
                        .build();
                channel.basicPublish(exchange, routingKey, props, JsonUtils.object2Bytes(message));
                channel.waitForConfirmsOrDie();
            } catch (final Exception ex) {
                LOGGER.error("Failed to publish payload to AMQP queue {}: {}", queue, ex.getMessage(), ex);
                throw new RuntimeException(ex);
            }
        });
    }
}
```