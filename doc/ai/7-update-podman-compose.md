# Issue 7: Update the `podman-compose.yml` file for the `crypto-scout-collector` project

In this `crypto-scout-collector` project we are going to update a `podman-compose.yml` file to run the `collector`
service in a container. Create a secret file `secret/collector.env.example` with key settings and update a documentation
file `secret/README.md` as a guide how to create a `collector.env` file.

## Roles

Take the following roles:

- Expert dev-opts engineer.
- Expert technical writer.

## Conditions

- Use the best practices and design patterns.
- Use the minimal production image with `java 25`.
- Do not hallucinate.

## Tasks

- As the expert dev-opts engineer update `podman-compose.yml` in the root directory for the `crypto-scout-collector`
  project. Define everything that is needed in the file to run the `collector` service in a container and to be ready
  for production.
- As the expert dev-opts engineer create `secret/collector.env.example` file for the `crypto-scout-collector` project.
  Define everything that is needed in the file to run the service in a container and to be ready for production.
- As the expert dev-opts engineer recheck your proposal and make sure that they are correct and haven't missed any
  important points.
- As the technical writer update the `README.md` and `collector-production-setup.md` files with your results.
- As the technical writer update the `7-update-podman-compose.md` file with your resolution.

## Resolution

- Updated `podman-compose.yml`:
    - Added `crypto-scout-collector` service with production settings: `cpus`/`memory`, `read_only` FS, `tmpfs:/tmp`,
      `no-new-privileges`, dropped caps, non-root `user`, `init`, `pids_limit`, `ulimits nofile`, healthcheck on
      `GET /health`, and external network `crypto-scout-bridge`.
    - `depends_on` now includes both the DB and backup services.
- Created `secret/collector.env.example` with individual environment variables (`SERVER_PORT`, `AMQP_*`, `JDBC_*`) and
  `JAVA_TOOL_OPTIONS=-XX:+ExitOnOutOfMemoryError` only.
- Hardened `Dockerfile`:
    - Pinned base image digest `eclipse-temurin:25-jre-alpine@sha256:...` with OCI labels.
    - Non-root user/group `10001:10001`, `WORKDIR /opt/crypto-scout`, `EXPOSE 8081`, installed `curl`,
      `STOPSIGNAL SIGTERM`.
- Updated docs:
    - `secret/README.md` documents `collector.env` usage and `POSTGRES_HOST=crypto-scout-collector-db`.
    - `README.md` details compose usage, secrets, and health check.
    - `doc/0.0.1/collector-production-setup.md` updated for the application container and secrets.

### How to run

1) Build, network, and secrets:

```bash
mvn -q -DskipTests package
./script/network.sh
cp ./secret/timescaledb.env.example ./secret/timescaledb.env
cp ./secret/postgres-backup.env.example ./secret/postgres-backup.env
cp ./secret/collector.env.example ./secret/collector.env
chmod 600 ./secret/*.env
```

2) Start:

```bash
podman-compose -f podman-compose.yml build crypto-scout-collector
podman-compose -f podman-compose.yml up -d
```

3) Health:

```bash
curl -s http://localhost:8081/health  # -> ok
```