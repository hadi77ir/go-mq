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
)

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
	return c
}

func (c Config) validate() error {
	if len(c.Connection.Addresses) == 0 {
		return ErrNoAddresses
	}
	return nil
}
