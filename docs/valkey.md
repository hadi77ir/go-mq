# Valkey Adapter

The Valkey adapter lives in `github.com/hadi77ir/go-mq/valkey` and is powered by the `valkey-go` client (the successor of `rueidis`). It exposes Valkey Streams with acknowledgement support and optional Pub/Sub semantics while sharing the common `mq.Broker` contract.

## Configuration Highlights

```go
type Config struct {
    Connection          mq.Config
    MaxConnections      int
    DialTimeout         time.Duration
    Database            int              // `SELECT <db>` for standalone deployments
    DisableCache        bool             // opt-out of client-side caching
    StreamMaxLen        int64            // trim streams (0 disables)
    ApproximateTrimming bool
    AutoCreateStream    bool
    AutoCreateGroup     bool
    BatchSize           int64            // default XREADGROUP COUNT
    BlockTimeout        time.Duration    // default XREADGROUP BLOCK
    PublishMode         PublishMode      // Streams (default) or Pub/Sub
}
```

- `Connection.TLSConfig` can be supplied to reuse an existing `*tls.Config`; otherwise the legacy `UseTLS`/certificate path fields remain available.
- Streams mode uses `XADD` for publishing and `XREADGROUP`/`XACK` for consumption. Messages are stored as base64-encoded payloads with metadata headers.
- Dead-letter routing is handled via `ConsumeOptions.DeadLetterTopic`, which pushes failed messages to a dedicated stream on negative acknowledgement.
- When `PublishModePubSub` is selected, messages are emitted via `PUBLISH`. Pub/Sub deliveries present acknowledgement functions that are safe no-ops.
- Setting `PublishModeList` enables a classic push/pull queue built on `RPUSH`/`BRPOP`. Messages use CBOR-encoded envelopes (matching Pub/Sub) for efficient binary serialization. `Delivery.Nack(ctx, true)` requeues via `RPUSH`, while `Delivery.Nack(ctx, false)` forwards to `DeadLetterTopic` when provided.

## Thread Safety

`valkeymq.Broker` obtains a dedicated Valkey client connection from the shared pool for each publish/consume/queue call and returns it afterward. As a result you can share a single broker instance between multiple goroutines without introducing your own locks.

## Acknowledgements & Requeue

Deliveries obtained from `Consume` expose `Ack` and `Nack` closures. `Nack(ctx, true)` removes the original pending entry and pushes an identical copy back onto the source stream. `Nack(ctx, false)` acks the original record and, when a dead-letter stream is configured, forwards the message there.

## Queues

`CreateQueue` / `DeleteQueue` map to `XGROUP CREATE`/`XGROUP DESTROY` for stream mode. Enabling `AutoCreateGroup` allows `Consume` to automatically attempt group creation (using `MKSTREAM`) before entering the read loop. `CreateQueue` and `DeleteQueue` return `mq.ErrNotSupported` for List and PubSub modes.

## Pub/Sub Mode

While Pub/Sub lacks acknowledgement semantics, the adapter still exposes a unified consumer. Messages are encoded as CBOR (Concise Binary Object Representation) envelopes that include headers and metadata, allowing consumers to reconstruct `mq.Message`. CBOR is used instead of JSON to minimize byte usage and efficiently handle binary message bodies.

## Testing

`valkey/broker_test.go` spins up a Valkey container via Testcontainers, validates stream acknowledgements, dead-letter forwarding, and Pub/Sub message flow. Docker must be available to execute these tests. Refer to `docs/testing.md` for environment setup tips.
