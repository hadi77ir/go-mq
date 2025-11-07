# Resilience, Retries, and Reconnection

`go-mq` deliberately fails fast when the underlying transport becomes unavailable. Each broker call borrows a connection from the shared pool, performs the requested operation, and releases the connection. The pool itself does **not** attempt transparent reconnections or automatic retries—this keeps the core focused on deterministic behaviour and leaves resiliency trade-offs to the application.

This document describes what happens when a connection is lost and how to add retries and restarts around the provided brokers.

## Connection Lifecycle

- **Publish / queue APIs**: Each call obtains a connection (or stream client), opens the necessary channel, performs the operation, and immediately releases the resources. If the connection has been severed, the call returns one of the standard errors (`mq.ErrNoConnection`, `mq.ErrPublishFailed`, `mq.ErrConfiguration`, etc.).
- **Consumers**: A consumer owns a single connection (or stream client) for its entire lifetime. Once the underlying connection drops, `Receive` eventually returns `mq.ErrConsumeFailed` (wrapping the transport error) and the consumer cannot recover. The caller must close it and build a new consumer.
- **Connection pool**: The pool only creates new connections when asked for one and less than `MaxSize` are currently open. It does not monitor health; a dead connection that is returned to the pool is reused until it is explicitly closed or evicted by `IdleTimeout`. When repeated `mq.ErrNoConnection` errors are observed, close the broker so the pool drains and rebuild it.

Because of this design, retries and restarts should happen in user code.

## Retrying Publish Calls

Wrap publish operations in a loop that retries on transient errors (`mq.ErrNoConnection`, `mq.ErrPublishFailed`, context cancellations, etc.). Use a bounded backoff to avoid overwhelming the broker.

```go
func publishWithRetry(ctx context.Context, broker mq.Broker, topic string, msg mq.Message) error {
	backoff := time.Millisecond * 200
	for attempts := 0; attempts < 5; attempts++ {
		if err := broker.Publish(ctx, topic, msg); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		return nil
	}
	return fmt.Errorf("publish retries exhausted")
}
```

When retries keep failing with `mq.ErrNoConnection`, close the broker and recreate it to force the pool to re-dial:

```go
broker.Close(ctx)              // ignore error
broker, _ = rabbitmq.NewBroker(ctx, cfg)
```

## Restarting Consumers After Disconnects

Consumers must be recreated when `Receive` fails. A common pattern is to run each logical consumer inside a loop that recreates both the broker consumer and the goroutine reading from it.

```go
func consumeForever(ctx context.Context, broker mq.Broker, topic, queue string) {
	for ctx.Err() == nil {
		consumer, err := broker.Consume(ctx, topic, queue, "")
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for ctx.Err() == nil {
			msg, err := consumer.Receive(ctx)
			if err != nil {
				consumer.Close() // nolint:errcheck
				break
			}
			process(msg)
			_ = msg.Ack(ctx)
		}
	}
}
```

If the broker itself errors repeatedly during consumer creation (`mq.ErrNoConnection`), recreate the broker as described above.

## Adapter-Specific Notes

### RabbitMQ (AMQP modes)

- `amqp091-go` connections do not reconnect automatically once the TCP socket is closed.
- When a call returns `mq.ErrNoConnection` or `mq.ErrPublishFailed`, retry the publish. If the error persists, close the broker so the pool drops the dead connections and build a new broker.
- Consumers exit when their channel or connection closes. Wrap them in a restart loop as shown earlier.

### RabbitMQ Streams

- `rabbitmq-stream-go-client` internally reconnects individual partitions, but `go-mq` still reports `mq.ErrPublishFailed` when the client cannot recover. Retry publishes; if the environment has been torn down, close and recreate the broker (which recreates the stream environment and producers).

### NATS

- `nats.Conn` has built-in reconnect logic. During reconnect attempts publish calls return errors from `conn.PublishMsg`/`JetStream.PublishMsg`, so retries are still required.
- JetStream pull consumers (`Fetch`) return errors when the subscription is drained during reconnect. Close the consumer and recreate it; JetStream will resume from the durable cursor.
- You can customise reconnect wait/limits by wrapping `nats.Options` before constructing the broker.

### Valkey

- `valkey-go` reconnects sockets internally, but long outages will surface as errors from `Client.Do`. When `Receive` or `Publish` returns a `mq.Err*` error, retry; if every attempt fails, rebuild the broker to reinitialise the client.
- Consumers exit on `XREADGROUP` failures and must be recreated.

## Summary

The current implementations report transport failures back to the caller rather than hiding them. Implement retries with exponential backoff around publish operations, restart consumers when `Receive` fails, and recreate brokers when the connection pool appears to be full of dead connections. This keeps failure handling explicit and lets each application decide how aggressive it should be when recovering from outages.
