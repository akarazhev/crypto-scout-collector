# Context: Logs checks of the running `timescaledb` and `postgres-backup` containers as the production ready service

In this `crypto-scout-collector-db` project we have configured the `timescaledb` and `postgres-backup` servers in containers to 
collect crypto data and backups. So you will need to check logs and make sure that the service is running correctly.

## Roles

Take the following roles:

- Expert dev-opts engineer.

## Conditions

- Use the best practices and design patterns to fix any issues found in the logs.
- Do not hallucinate.
- Use the latest technology stack: `timescale/timescaledb:latest-pg17` and `prodrigestivill/postgres-backup-local:latest`.

## Tasks

- As the expert dev-opts engineer check uploaded `logs` and make sure that the service is running correctly, 
  correct any issues found.
- Recheck your proposal and make sure that they are correct and haven't missed any important points.
- Write a report with your proposal and implementation into `doc/timescaledb-production-setup.md`.