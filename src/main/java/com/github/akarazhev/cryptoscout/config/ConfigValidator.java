/*
 * MIT License
 *
 * Copyright (c) 2025 Andrey Karazhev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.github.akarazhev.cryptoscout.config;

import com.github.akarazhev.jcryptolib.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_ANALYST_QUEUE;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_ANALYST_ROUTING_KEY;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_BYBIT_STREAM;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_BYBIT_TA_STREAM;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_CHATBOT_QUEUE;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_CHATBOT_ROUTING_KEY;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_COLLECTOR_QUEUE;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_COLLECTOR_ROUTING_KEY;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_CRYPTO_SCOUT_EXCHANGE;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_CRYPTO_SCOUT_STREAM;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_RABBITMQ_HOST;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_RABBITMQ_PORT;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_RABBITMQ_USERNAME;
import static com.github.akarazhev.cryptoscout.config.Constants.AmqpConfig.AMQP_STREAM_PORT;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_URL;
import static com.github.akarazhev.cryptoscout.config.Constants.JdbcConfig.JDBC_USERNAME;
import static com.github.akarazhev.cryptoscout.config.Constants.ServerConfig.SERVER_PORT;

public final class ConfigValidator {
    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigValidator.class);

    private static final String[] REQUIRED_STRING_PROPERTIES = {
            AMQP_RABBITMQ_HOST,
            AMQP_RABBITMQ_USERNAME,
            AMQP_BYBIT_STREAM,
            AMQP_BYBIT_TA_STREAM,
            AMQP_CRYPTO_SCOUT_STREAM,
            AMQP_CRYPTO_SCOUT_EXCHANGE,
            AMQP_COLLECTOR_ROUTING_KEY,
            AMQP_COLLECTOR_QUEUE,
            AMQP_CHATBOT_QUEUE,
            AMQP_CHATBOT_ROUTING_KEY,
            AMQP_ANALYST_QUEUE,
            AMQP_ANALYST_ROUTING_KEY,
            JDBC_URL,
            JDBC_USERNAME
    };

    private static final String[] REQUIRED_INT_PROPERTIES = {
            AMQP_RABBITMQ_PORT,
            AMQP_STREAM_PORT,
            SERVER_PORT
    };

    private ConfigValidator() {
        throw new UnsupportedOperationException();
    }

    public static void validate() throws Exception {
        LOGGER.info("Validating configuration properties...");
        final var errors = new ArrayList<String>();

        for (final var property : REQUIRED_STRING_PROPERTIES) {
            try {
                final var value = AppConfig.getAsString(property);
                if (value == null || value.isBlank()) {
                    errors.add("Required property '" + property + "' is missing or empty");
                }
            } catch (final Exception ex) {
                errors.add("Required property '" + property + "' is missing: " + ex.getMessage());
            }
        }

        for (final var property : REQUIRED_INT_PROPERTIES) {
            try {
                final var value = AppConfig.getAsInt(property);
                if (value <= 0) {
                    errors.add("Required property '" + property + "' must be a positive integer");
                }
            } catch (final Exception ex) {
                errors.add("Required property '" + property + "' is missing or invalid: " + ex.getMessage());
            }
        }

        if (!errors.isEmpty()) {
            LOGGER.error("Configuration validation failed with {} error(s):", errors.size());
            for (final var error : errors) {
                LOGGER.error("  - {}", error);
            }

            throw new Exception("Configuration validation failed. See logs for details.");
        }

        LOGGER.info("Configuration validation passed successfully");
    }
}
