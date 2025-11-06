# Testing Guide

The repository ships with unit and integration tests covering the shared utilities and transport-specific adapters.

## Prerequisites

Running the RabbitMQ and Valkey adapter tests requires:

- Docker (or a compatible OCI runtime) accessible from the local environment. When using Podman, export `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock` so Testcontainers can reach the rootless socket.
- Network access to pull `rabbitmq:3.13-management` and `valkey/valkey:7.2` images.
- Sufficient permissions to create and remove containers.

If Docker is not available (for example inside a restricted CI sandbox), you can still run the core unit tests:

```bash
GOCACHE=$(pwd)/.cache go test ./...
```

To execute the RabbitMQ suite:

```bash
cd ./rabbitmq
GOCACHE=$(pwd)/../.cache go test ./...
```

For Valkey:

```bash
cd ./valkey
GOCACHE=$(pwd)/../.cache go test ./...
```

> **Note:** The adapter tests will fail fast if Docker is unavailable. This is expected behaviour and should be treated as an environment issue rather than a code regression.

## Troubleshooting

- Ensure `$DOCKER_HOST` and `$XDG_RUNTIME_DIR` are set correctly when running inside environments that use rootless Docker.
- If image pulls fail, pre-fetch the required images (`docker pull rabbitmq:3.13-management` and `docker pull valkey/valkey:7.2`) before running the tests.
- Remove stale containers created by earlier test runs with `docker ps -a` and `docker rm`.
For NATS (JetStream & core):

```bash
cd ./nats
export DOCKER_HOST=unix:///run/user/1000/podman/podman.sock
GOCACHE=$(pwd)/../.cache go test ./...
```
