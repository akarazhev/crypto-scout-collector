# MIT License
#
# Copyright (c) 2025 Andrey Karazhev
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

FROM eclipse-temurin:25-jre-alpine@sha256:bf9c91071c4f90afebb31d735f111735975d6fe2b668a82339f8204202203621
LABEL org.opencontainers.image.title="crypto-scout-collector" \
      org.opencontainers.image.description="Production-ready Java microservice that ingests crypto market metrics from RabbitMQ Streams and persists structured events into TimescaleDB." \
      org.opencontainers.image.version="0.0.1" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="Andrey Karazhev" \
      org.opencontainers.image.source="https://github.com/akarazhev/crypto-scout-collector"
ENV JAVA_TOOL_OPTIONS="-XX:+ExitOnOutOfMemoryError"
WORKDIR /opt/crypto-scout
RUN addgroup -S app -g 10001 && adduser -S -G app -u 10001 app
COPY --chown=10001:app target/crypto-scout-collector-0.0.1.jar crypto-scout-collector.jar
RUN apk add --no-cache curl
USER 10001:10001
EXPOSE 8081
STOPSIGNAL SIGTERM
ENTRYPOINT ["java", "-jar", "crypto-scout-collector.jar"]