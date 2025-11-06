# Consistency Notes

This document summarizes configuration fields and runtime options that are currently ignored by the individual broker implementations. Use it to track potential behaviour gaps and plan future enhancements.

## Shared Options

### `mq.PublishOptions`

| Field        | RabbitMQ               | Valkey                                    | NATS                 |
|--------------|------------------------|-------------------------------------------|----------------------|
| `Delay`      | Respected (x-delay header, requires plugin) | Respected (stored in payload)             | Ignored              |
| `Mandatory`  | Respected              | Respected (stored in payload)            | Ignored              |
| `Persistent` | Respected              | Respected (stored in payload)            | Ignored              |
| `Expiration` | Respected              | Respected (stored in payload)             | Ignored              |
| `Headers`    | Respected              | Respected (merged into payload)           | Respected            |
| `ReplyTo`    | Respected              | Respected (stored in payload)            | Respected            |
| `CorrelationID` | Respected           | Respected (stored in payload)            | Respected            |

### `mq.ConsumeOptions`

| Field              | RabbitMQ                                 | Valkey                                                                 | NATS                                                                                   |
|--------------------|-------------------------------------------|-------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| `Topic`            | Used for queue binding                    | Stream key / channel name                                              | Subject (core & JetStream)                                                             |
| `Queue`            | Queue name / durable consumer             | Streams: consumer group; List/PubSub mode: not supported (ErrNotSupported) | Core: queue subscribers; JetStream: durable name; Core mode: ErrNotSupported |
| `ConsumerName`     | Consumer tag                              | Streams: explicit consumer name; others ignored                        | JetStream only (durable fallback); ignored in core mode                                |
| `AutoAck`          | Passed to `Consume`                       | Respected (all modes auto-ack on receive)                              | JetStream: respected; core mode ignored (no ack concept)                               |
| `Prefetch`         | Applied via `Qos`                         | Streams: overrides batch size; List/PubSub: ignored                     | JetStream: maps to `MaxAckPending`/fetch batch; core mode ignores                      |
| `StartFromOldest`  | Respected (queue order is always oldest first) | Streams: respected (uses "0-0" instead of ">"); List/PubSub: ignored | JetStream: maps to `DeliverAllPolicy`; core mode ignores                               |
| `DeadLetterTopic`  | Declares dead-letter exchange mapping     | Streams/list mode: used for requeue; Pub/Sub ignores (no DL support)   | JetStream: used as replay subject; core mode ignores                                   |

## Adapter-Specific Notes

### RabbitMQ (`rabbitmq`)

* `mq.PublishOptions.Delay` is supported via the `x-delay` header, which requires the RabbitMQ delayed message exchange plugin to be enabled.
* `mq.ConsumeOptions.StartFromOldest` is respected (queue order is always oldest first in AMQP).
* `mq.ConsumeOptions.DeadLetterTopic` is translated to `x-dead-letter-exchange` but never validated; if the exchange does not exist the broker will reject messages later.

### Valkey (`valkey`)

* `mq.PublishOptions` fields are now respected and stored in the payload (Delay, Headers, ReplyTo, CorrelationID, Persistent, Mandatory, Expiration).
* `mq.ConsumeOptions.AutoAck` is respected in all modes (streams, list, pub/sub).
* `mq.ConsumeOptions.StartFromOldest` is respected for streams (uses "0-0" instead of ">"); ignored for list/pubsub modes.
* `mq.ConsumeOptions.ConsumerName` is used only for streams; list and pub/sub consumers drop it.
* `Config.StreamMaxLen` and `Config.ApproximateTrimming` apply only in stream mode; they are ignored when `PublishMode` is `PublishModeList` or `PublishModePubSub`.
* `Config.AutoCreateStream` applies to streams and lists; pubsub channels are implicit and don't require creation.

### NATS (`nats`)

* `mq.PublishOptions.Delay`, `Persistent`, and `Mandatory` have no meaning in core or JetStream modes and are ignored.
* `mq.PublishOptions.Headers` and `CorrelationID` are applied as NATS headers; JetStream does not persist the structured `Delay`/`Expiration`.
* `mq.ConsumeOptions.AutoAck` is respected in JetStream mode; ignored in core mode (no ack concept).
* `mq.ConsumeOptions.DeadLetterTopic` is ignored in core mode; JetStream maps it to a secondary subject.
* `Config.StreamName`, `PullBatch`, `PullTimeout`, and `AckWait` are only respected in JetStream mode; they are ignored when `PublishModeCore` is selected.
* `Config.SubjectPrefix` impacts both publish and consume paths; however, an empty `Topic` still produces an empty subject and is rejected manually.
