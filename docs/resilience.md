# Resilience, Retries, and Reconnection

`go-mq` automatically retries broker operations with an exponential backoff policy (`mq.RetryPolicy`). RabbitMQ adapters proactively discard broken channels/connections so each retry dials a fresh connection and replays the required declarations. RabbitMQ super-stream producers are recreated transparently when the underlying TCP link drops. This keeps publish and consume setup calls resilient by default while allowing applications to bound retries through configuration.

## Default Behaviour

- Every `Publish`, `Consume`, `CreateQueue`, `DeleteQueue`, and `Flush` call uses the adapter's `Retry` policy. The default policy retries indefinitely (bounded by the request context) with exponential backoff starting at 200 ms and capped at 5 s.
- RabbitMQ (AMQP modes) validates the connection on every retry. Whenever an operation encounters `amqp.ErrClosed`, network errors, or channel/connection shutdowns, the pool entry is discarded and a new TCP connection is created for the next attempt.
- RabbitMQ Streams recreate `SuperStreamProducer` instances the moment a send fails, so the next retry goes through a fresh producer.
- Operations that fail with `mq.ErrConfiguration` or `mq.ErrNotSupported` return immediately; these are considered permanent errors.

Because retries are tied to the call's context, you can keep the default infinite attempts for best effort delivery and rely on context deadlines/timeouts to bound latency.

## Customising the Retry Policy

Each adapter exposes a `Retry mq.RetryPolicy` field on its `Config`. Adjust it to cap attempts or tune delays:

```go
cfg := rabbitmq.Config{
    Retry: mq.RetryPolicy{
        MaxAttempts:    6,
        InitialBackoff: 250 * time.Millisecond,
        MaxBackoff:     3 * time.Second,
        Multiplier:     2,
    },
    // ...
}
```

- `MaxAttempts <= 0` means infinite retries.
- `InitialBackoff`, `MaxBackoff`, and `Multiplier` control the exponential curve.

The helper `mq.Retry(ctx, policy, func(ctx context.Context) error)` is exported so applications can reuse the same policy for higher-level workflows (for example, wrapping business logic that calls multiple brokers).

## Consumers

`Broker.Consume` now retries the subscription setup (queue declarations, QoS, bindings, stream consumer creation) using the configured policy. When the call returns a consumer, it owns a healthy connection/channel. If the connection later drops, `Receive` still returns `mq.ErrConsumeFailed`; callers should close the consumer and create a new one, which will once again benefit from the retry policy during setup.

## Adapter Notes

- **RabbitMQ (AMQP)** – Connections that fail mid-operation are removed from the pool, ensuring the next retry dials a fresh socket before redeclaring queues/bindings.
- **RabbitMQ Streams** – Any producer failure results in the cached producer being discarded and lazily recreated on the next retry.
- **NATS / Valkey** – These adapters rely on their respective client libraries for reconnection. Planned work will extend the shared retry policy to their operations for full parity.

Use the `Retry` policy together with context deadlines to achieve the resilience profile you need without sprinkling manual retry loops throughout your codebase.
