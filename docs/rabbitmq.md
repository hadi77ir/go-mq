# RabbitMQ Adapter

The RabbitMQ adapter is published as `github.com/hadi77ir/go-mq/rabbitmq` and targets RabbitMQ 3.13+ using the `amqp091-go` client. It implements the shared `mq.Broker` interface on top of AMQP exchanges, queues, and consumer tags.

## Configuration

```go
type Config struct {
    Connection        mq.Config
    MaxConnections    int           // size of the underlying connection pool
    DialTimeout       time.Duration // timeout while dialing new connections
    Exchange          string        // exchange used for publish/consume bindings
    ExchangeType      string        // fanout, direct, topic, etc. (defaults to direct)
    DeclareExchange   bool          // auto declare the exchange on start
    Prefetch          int           // QoS prefetch when consuming
    QueueDurable      bool          // declare durable queues
    IdleTimeout       time.Duration // idle connection eviction window
    PublishMode       PublishMode   // Push/Pull, Pub/Sub, or Streams (defaults to persistent push-pull)
    Stream            StreamConfig  // retention controls when PublishModeStreams is selected
    Retry             mq.RetryPolicy // exponential backoff for publish/consume retries
}

type StreamConfig struct {
    Addresses            []string      // override stream endpoints (defaults to Connection.Addresses)
    Partitions           int           // number of partitions per super stream
    MaxLengthBytes       int64         // x-max-length-bytes
    MaxSegmentSizeBytes  int64         // x-stream-max-segment-size-bytes
    MaxAge               time.Duration // x-max-age
    MaxProducersPerClient int          // stream client pooling for producers
    MaxConsumersPerClient int          // stream client pooling for consumers
}
```

> **Note**: RabbitMQ Streams requires the `rabbitmq_stream` plugin. The integration tests automatically enable the plugin by running `rabbitmq-plugins enable --offline rabbitmq_stream` and restarting the container before exercising the adapter. Ensure the plugin is installed and enabled in any non-test environment before selecting `PublishModeStreams`.

The shared `mq.Config` embedded in `Connection` supplies addresses, credentials, and optional TLS information. The adapter dials each address in order until a connection succeeds. TLS can be provided either through `TLSConfig` or the legacy `UseTLS`/certificate path fields.

## Thread Safety & Resilience

`rabbitmq.Broker` instances are safe for concurrent publishing or queue management. Each call acquires a connection from the shared pool, opens a dedicated AMQP channel, and releases the resources once complete. Streams mode also multiplexes a pool of producers via `sync.Map`, so you can share a single broker across goroutines without extra locking.

All operations use the shared `Retry` policy (default: unlimited attempts with exponential backoff starting at 200 ms) so transient failures trigger automatic reconnection and re-declaration attempts. Tune `Config.Retry` if you need to cap attempts or adjust the backoff curve.

## Publishing

`Publish` routes messages to the configured exchange using the provided routing key. Headers from `mq.Message` and `mq.PublishOptions` are merged into AMQP headers and the body is sent verbatim. Persistence is controlled through `PublishOptions.Persistent`.

```go
broker.Publish(ctx, "orders.created", mq.Message{
    Body:        payload,
    ContentType: "application/json",
}, &mq.PublishOptions{Persistent: true})
```

Publish modes control how queues are declared and how persistence is handled:

- `PublishModePushPull` (default) – competing consumers with transient queues.
- `PublishModePersistentPushPull` – push/pull with durable queues and persistent messages.
- `PublishModePubSub` / `PublishModePersistentPubSub` – fan-out semantics where each consumer receives a copy.
- `PublishModeStreams` – RabbitMQ stream queues backed by the streams feature, providing log-style retention similar to Valkey streams. `StreamConfig` exposes retention knobs such as `MaxLengthBytes`, `MaxSegmentSizeBytes`, and `MaxAge`.

```go
cfg := rabbitmq.Config{
    PublishMode: rabbitmq.PublishModeStreams,
    Stream: rabbitmq.StreamConfig{
        Addresses:            []string{"rabbitmq-stream://guest:guest@localhost:5552/"},
        Partitions:           5,
        MaxLengthBytes:      256 << 20, // 256 MiB retention
        MaxSegmentSizeBytes: 8 << 20,   // 8 MiB segments
        MaxAge:              24 * time.Hour,
        MaxProducersPerClient: 5,
        MaxConsumersPerClient: 5,
    },
}
```

## Consuming

`Consume` maps `ConsumeOptions` to queue declarations and QoS configuration:

- `Topic` is treated as the binding key.
- `Queue` (or `Topic` if `Queue` is empty) becomes the queue name.
- `Prefetch` overrides the default QoS setting.
- `DeadLetterTopic` adds `x-dead-letter-exchange` queue arguments.

The consumer manages acknowledgements through the supplied delivery callbacks. `Requeue` in `Delivery.Nack` uses AMQP negative acknowledgement semantics.

```go
consumer, _ := broker.Consume(ctx, mq.ConsumeOptions{
    Topic:   "orders.created",
    Queue:   "accounting",
    Prefetch: 10,
})
```

When using `PublishModeStreams`, `CreateQueue` declares `x-queue-type=stream` queues and `Consume` sets `x-stream-offset=next`, meaning consumers receive new messages only. The queue behaves like the Valkey streams adapter—multiple consumers on the same queue share work, acknowledged messages stay in the log up to the configured retention limits, and manual acknowledgements advance the shared offset.

## Queue Management

The shared `CreateQueue` and `DeleteQueue` map to queue declaration and deletion. When an exchange is configured, `CreateQueue` also creates the necessary binding.

## Testing

`rabbitmq/broker_test.go` runs an integration test with Testcontainers, provisioning a RabbitMQ container, publishing and consuming messages, and exercising acknowledgement paths. Running these tests requires Docker; see `docs/testing.md` for guidance.
