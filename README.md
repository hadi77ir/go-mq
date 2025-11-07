# go-mq

A unified, broker-agnostic message queue library for Go that provides a consistent API across multiple message queue systems.

## Overview

`go-mq` defines a common interface for message queue operations, allowing you to write code that works seamlessly with RabbitMQ, NATS, or Valkey. The library abstracts away transport-specific details while maintaining access to broker-specific features through typed options.

## Features

- **Unified API**: Single interface (`mq.Broker`) for all supported message brokers
- **Multiple Backends**: Support for RabbitMQ (Push/Pull, Pub/Sub, Streams), NATS (Core & JetStream), and Valkey (Streams, Pub/Sub, Lists)
- **Type-Safe Options**: Functional options pattern with distinct struct types for compile-time safety
- **Connection Pooling**: Built-in connection pool for efficient resource management
- **Explicit Acknowledgments**: Full control over message acknowledgment and negative acknowledgment
- **Dead Letter Queues**: Built-in support for dead letter topics/queues
- **TLS Support**: Secure connections with configurable TLS settings

## Supported Brokers

- **RabbitMQ** (`github.com/hadi77ir/go-mq/rabbitmq`) - AMQP-based messaging
- **NATS** (`github.com/hadi77ir/go-mq/nats`) - Core NATS and JetStream
- **Valkey** (`github.com/hadi77ir/go-mq/valkey`) - Redis-compatible streams, pub/sub, and lists

## Quick Start

### Installation

```bash
go get github.com/hadi77ir/go-mq
go get github.com/hadi77ir/go-mq/rabbitmq  # or nats, or valkey
```

### Example: Publishing Messages

```go
package main

import (
    "context"
    "time"
    
    "github.com/hadi77ir/go-mq"
    "github.com/hadi77ir/go-mq/rabbitmq"
)

func main() {
    ctx := context.Background()
    
    cfg := rabbitmq.Config{
        Connection: mq.Config{
            Addresses: []string{"localhost:5672"},
            Username:  "guest",
            Password:  "guest",
        },
        Exchange:        "events",
        ExchangeType:    "topic",
        DeclareExchange: true,
    }
    
    broker, err := rabbitmq.NewBroker(ctx, cfg)
    if err != nil {
        panic(err)
    }
    defer broker.Close(ctx)
    
    msg := mq.Message{
        Body:        []byte("Hello, World!"),
        ContentType: "text/plain",
        Headers:     map[string]string{"source": "example"},
    }
    
    err = broker.Publish(ctx, "events.created", msg,
        rabbitmq.WithPersistent(true),
        mq.WithHeaders(map[string]string{"priority": "high"}),
    )
    if err != nil {
        panic(err)
    }
}
```

### Example: Consuming Messages

```go
consumer, err := broker.Consume(ctx, "events.created", "worker-queue", "worker-1",
    mq.WithAutoAck(false),
    mq.WithPrefetch(10),
    mq.WithDeadLetterTopic("events.dlq"),
)
if err != nil {
    panic(err)
}
defer consumer.Close()

for {
    delivery, err := consumer.Receive(ctx)
    if err != nil {
        break
    }
    
    // Process message
    processMessage(delivery.Message)
    
    // Acknowledge
    if err := delivery.Ack(ctx); err != nil {
        // Handle error
    }
}
```

## Options Pattern

The library uses a type-safe functional options pattern where each option is a distinct struct type:

### Common Options

- `mq.WithAutoAck(bool)` - Enable automatic message acknowledgment
- `mq.WithPrefetch(int)` - Set prefetch count for consumers
- `mq.WithDeadLetterTopic(string)` - Configure dead letter topic/queue
- `mq.WithHeaders(map[string]string)` - Add custom headers to messages
- `mq.WithReplyTo(string)` - Set reply-to address
- `mq.WithCorrelationID(string)` - Set correlation ID for request/response

### RabbitMQ-Specific Options

- `rabbitmq.WithMandatory(bool)` - Return message if unroutable
- `rabbitmq.WithPersistent(bool)` - Persist message to disk
- `rabbitmq.WithExpiration(time.Duration)` - Set message expiration
- `rabbitmq.WithDelay(time.Duration)` - Delay message delivery (requires plugin)

### NATS-Specific Options

- `mqnats.WithStartFromOldest(bool)` - Start consuming from oldest messages (JetStream)

## Architecture

The library is organized into:

- **Core Package** (`mq`): Defines the unified `Broker` interface, `Message`, `Delivery`, and common options
- **Adapter Packages**: Implementation-specific packages (`rabbitmq`, `nats`, `valkey`) that implement the `Broker` interface
- **Connection Pool**: Reusable connection pool for efficient resource management

Each adapter is an independent Go module with its own `go.mod`, using replace directives to reference the core module.

## Documentation

Detailed documentation for each adapter:

- [Overview](docs/overview.md) - Architecture and design decisions
- [RabbitMQ Adapter](docs/rabbitmq.md) - RabbitMQ-specific features
- [NATS Adapter](docs/nats.md) - NATS Core and JetStream usage
- [Valkey Adapter](docs/valkey.md) - Valkey Streams, Pub/Sub, and Lists
- [Testing](docs/testing.md) - Testing strategies and examples
- [Consistency](docs/consistency.md) - Cross-broker consistency guarantees

## Requirements

- Go 1.21 or later
- RabbitMQ 3.13+ (for RabbitMQ adapter)
- NATS Server 2.10+ (for NATS adapter)
- Valkey 7.2+ or Redis 7.0+ (for Valkey adapter)

## License

Apache 2.0

---

**Note**: This README was generated by AI. For the most up-to-date information, please refer to the source code and inline documentation.
