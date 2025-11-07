# NATS Adapter

The NATS adapter lives under `github.com/hadi77ir/go-mq/nats` and supports both classic subjects/queue subscribers and JetStream-based workflows with explicit acknowledgements.

## Configuration

```go
type Config struct {
    Connection     mq.Config  // shared addresses, credentials, TLS material
    MaxConnections int        // connection pool size (default 4)
    IdleTimeout    time.Duration
    PublishMode    PublishMode
    StreamName     string     // required for JetStream mode
    SubjectPrefix  string     // optional prefix applied to all subjects
    PullBatch      int        // JetStream pull batch size (default 1)
    PullTimeout    time.Duration
    AckWait        time.Duration
    TLSConfig      *tls.Config // optional explicit TLS config; overrides Connection.UseTLS fields
}
```

If `TLSConfig` is supplied it is used as-is, otherwise the adapter derives a `tls.Config` from the fields on `Connection` (`UseTLS`, certificate paths, etc.).

`PublishMode` selects between:

- `PublishModeCore` – plain NATS publish with queue subscribers. Ack/nack become no-ops.
- `PublishModeJetStream` – messages land in a JetStream stream (declared automatically) and are consumed via durable pull subscribers with explicit `Ack`/`Nack` semantics.

## Thread Safety

`mqnats.Broker` shares the same concurrency guarantees as the other adapters. Operations borrow a `nats.Conn` wrapper from the connection pool, execute their work, and return it, so multiple goroutines can reuse a single broker safely. JetStream setup routines rely on the same pool and guard stream declaration work internally.

## Publishing

`Broker.Publish` composes the final subject using `SubjectPrefix` and the supplied `target`. Common message metadata such as headers, content type, timestamps, correlation IDs, and reply addresses are attached using NATS headers.

```go
broker.Publish(ctx, "events.created", mq.Message{
    Body:        payload,
    ContentType: "application/json",
}, nil)
```

In JetStream mode the adapter calls `JetStreamContext.PublishMsg`; in core mode it uses `Conn.PublishMsg`.

## Consuming

### JetStream

- `ConsumeOptions.Topic` maps to the JetStream subject (after prefixing).
- `ConsumeOptions.Queue` becomes the durable consumer name. If omitted, a deterministic name is generated.
- `Prefetch` controls `MaxAckPending` and the pull batch size.
- `StartFromOldest` toggles between `DeliverAll` and `DeliverNew` policies.
- `DeadLetterTopic` (if provided) is published to on `Delivery.Nack(ctx, false)`.

`Delivery.Ack` calls `msg.Ack()`. `Delivery.Nack(ctx, true)` issues `msg.Nak()` to requeue. `Delivery.Nack(ctx, false)` acknowledges the original message and republishs it to the configured dead-letter subject.

### Core NATS

In `PublishModeCore`, `Consume` creates either a standard subscription or a queue subscription (when `Queue` is set). Messages flow through a buffered channel and acknowledgements are no-ops because basic NATS does not support them.

## Queue Management

`CreateQueue` and `DeleteQueue` wrap JetStream `AddConsumer`/`DeleteConsumer`. They return `mq.ErrNotSupported` in core mode where queues are not applicable.

## Testing

`nats/broker_test.go` provisions a `nats:2.10-alpine` container (with `-js` enabled) using Testcontainers. When running the suite with Podman, export:

```bash
export DOCKER_HOST=unix:///run/user/1000/podman/podman.sock
```

This mirrors the harness expectations used in automated testing.
