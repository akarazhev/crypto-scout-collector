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