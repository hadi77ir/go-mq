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
}
```

The shared `mq.Config` embedded in `Connection` supplies addresses, credentials, and optional TLS information. The adapter dials each address in order until a connection succeeds. TLS can be provided either through `TLSConfig` or the legacy `UseTLS`/certificate path fields.

## Publishing

`Publish` routes messages to the configured exchange using the provided routing key. Headers from `mq.Message` and `mq.PublishOptions` are merged into AMQP headers and the body is sent verbatim. Persistence is controlled through `PublishOptions.Persistent`.

```go
broker.Publish(ctx, "orders.created", mq.Message{
    Body:        payload,
    ContentType: "application/json",
}, &mq.PublishOptions{Persistent: true})
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

## Queue Management

The shared `CreateQueue` and `DeleteQueue` map to queue declaration and deletion. When an exchange is configured, `CreateQueue` also creates the necessary binding.

## Testing

`rabbitmq/broker_test.go` runs an integration test with Testcontainers, provisioning a RabbitMQ container, publishing and consuming messages, and exercising acknowledgement paths. Running these tests requires Docker; see `docs/testing.md` for guidance.
