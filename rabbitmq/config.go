package rabbitmq

import (
	"time"

	"github.com/hadi77ir/go-mq"
)

// PublishMode controls how messages are published/consumed.
type PublishMode int

const (
	// PublishModePubSub uses fanout exchange with temporary queues (non-persistent pub-sub).
	PublishModePubSub PublishMode = iota + 1
	// PublishModePersistentPubSub uses fanout exchange with durable queues (persistent pub-sub).
	PublishModePersistentPubSub
	// PublishModePushPull uses queues for work distribution (non-persistent push-pull).
	PublishModePushPull
	// PublishModePersistentPushPull uses durable queues for work distribution (persistent push-pull).
	PublishModePersistentPushPull
	// PublishModeStreams uses RabbitMQ stream queues to provide log-style semantics similar to Valkey streams.
	PublishModeStreams
)

// StreamConfig captures RabbitMQ stream queue specific settings.
type StreamConfig struct {
	// Addresses overrides Connection.Addresses for stream connections (host:port or URI).
	Addresses []string
	// Partitions controls how many partitions are declared for each super stream.
	Partitions int
	// MaxLengthBytes caps the total size retained by the stream (x-max-length-bytes).
	MaxLengthBytes int64
	// MaxSegmentSizeBytes controls the size of each stream segment (x-stream-max-segment-size-bytes).
	MaxSegmentSizeBytes int64
	// MaxAge bounds how long messages are retained in the stream (x-max-age).
	MaxAge time.Duration
	// MaxProducersPerClient configures stream client pooling for producers.
	MaxProducersPerClient int
	// MaxConsumersPerClient configures stream client pooling for consumers.
	MaxConsumersPerClient int
}

// Config captures RabbitMQ specific settings alongside the shared mq.Config.
type Config struct {
	Connection mq.Config

	// MaxConnections controls the size of the underlying connection pool.
	MaxConnections int

	// DialTimeout limits how long the broker waits when establishing new connections.
	DialTimeout time.Duration

	// Exchange defines the exchange used by default when publishing.
	Exchange string

	// ExchangeType is used when DeclareExchange is true.
	ExchangeType string

	// DeclareExchange indicates whether NewBroker should declare the exchange.
	DeclareExchange bool

	// Prefetch controls the default QoS prefetch for consumers (if ConsumeOptions.Prefetch is unset).
	Prefetch int

	// QueueDurable indicates if queues created via CreateQueue should be durable.
	// This is overridden by PublishMode for Consume operations.
	QueueDurable bool

	// IdleTimeout is applied to pooled connections.
	IdleTimeout time.Duration

	// PublishMode selects the messaging model (pub-sub vs push-pull, persistent vs non-persistent).
	// Defaults to PublishModePersistentPushPull if not set.
	PublishMode PublishMode

	// Stream configures RabbitMQ stream queues when PublishModeStreams is selected.
	Stream StreamConfig

	// Retry defines the exponential backoff policy for publish/consume operations.
	Retry mq.RetryPolicy
}

func (c Config) normalized() Config {
	if c.MaxConnections <= 0 {
		c.MaxConnections = 5
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	// Default to persistent push-pull if not set (zero value)
	if c.PublishMode == 0 {
		c.PublishMode = PublishModePersistentPushPull
	}
	if c.ExchangeType == "" {
		// Default to fanout for pub-sub modes, direct for push-pull modes
		if c.PublishMode == PublishModePubSub || c.PublishMode == PublishModePersistentPubSub {
			c.ExchangeType = "fanout"
		} else {
			c.ExchangeType = "direct"
		}
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = time.Minute
	}
	if c.Stream.Partitions <= 0 {
		c.Stream.Partitions = 1
	}
	if c.Stream.MaxProducersPerClient <= 0 {
		c.Stream.MaxProducersPerClient = 1
	}
	if c.Stream.MaxConsumersPerClient <= 0 {
		c.Stream.MaxConsumersPerClient = 1
	}
	c.Retry = c.Retry.Normalized()
	return c
}

func (c Config) validate() error {
	if len(c.Connection.Addresses) == 0 {
		return ErrNoAddresses
	}
	return nil
}
