# go-mq Overview

The `github.com/hadi77ir/go-mq` module defines a broker-agnostic abstraction for message queues together with shared configuration primitives and a light-weight connection pool. Two transport-specific adapters are provided in dedicated submodules:

- `github.com/hadi77ir/go-mq/rabbitmq` builds on top of `amqp091-go` and maps the shared broker interface to RabbitMQ exchanges, queues, and queue management.
- `github.com/hadi77ir/go-mq/valkey` uses the high-performance `valkey-go` client to provide Valkey Streams, Pub/Sub, and list-backed push/pull queues.
- `github.com/hadi77ir/go-mq/nats` offers NATS core and JetStream implementations, including explicit ack workflows.

All adapters share the same concurrency contract: a `Broker` can be reused safely across goroutines. Publish/consume operations borrow independent connections or channels from the pool and never mutate shared configuration, so no additional synchronization is required when sharing a broker instance across workers.

## Core Concepts

| Type | Purpose |
| --- | --- |
| `mq.Config` | Common connection settings shared by all adapters (hosts, credentials, TLS materials). |
| `mq.Broker` | Unified broker contract providing publish, consume, queue lifecycle and shutdown operations. |
| `mq.Message` / `mq.Delivery` | Portable payload and delivery metadata including acknowledgement hooks. |
| `mq.ConnectionPool` | Generic pool intended for long-lived broker connections. Implementations reuse it for connection re-use and throttling. |

Each adapter exposes an implementation-specific `Config` struct that embeds `mq.Config` and adds transport specific knobs (prefetch, exchanges, stream trimming, etc.). Consumer options capture routing, queue, batching and dead-letter behaviour in a transport-agnostic way.

## Repository Layout

```
go-mq/
├── config.go               # Shared configuration
├── interfaces.go           # Broker, message and consumer definitions
├── pool.go                 # Generic connection pool
├── pool_test.go            # Connection pool unit tests
├── rabbitmq/               # RabbitMQ adapter (independent Go module)
│   ├── broker.go
│   ├── consumer.go
│   ├── config.go
│   ├── errors.go
│   ├── broker_test.go      # Testcontainers-based integration test
│   └── go.mod
├── valkey/                 # Valkey Streams & Pub/Sub adapter (independent Go module)
│   ├── broker.go
│   ├── stream_consumer.go
│   ├── pubsub_consumer.go
│   ├── message.go
│   ├── stream.go
│   ├── util.go
│   ├── broker_test.go      # Testcontainers integration coverage
│   └── go.mod
├── nats/                   # NATS core & JetStream adapter (independent Go module)
│   ├── broker.go
│   ├── config.go
│   ├── core_consumer.go
│   ├── jetstream_consumer.go
│   ├── consumer_util.go
│   ├── broker_test.go      # Podman-backed Testcontainers suite
│   └── go.mod
└── docs/
    └── overview.md
```

Both adapter modules declare their own `go.mod` files and use a `replace` directive that points back to the root module, ensuring the core interfaces are consumed without introducing circular dependencies.
