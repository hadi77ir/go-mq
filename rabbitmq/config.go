package rabbitmq

import (
	"time"

	"github.com/hadi77ir/go-mq"
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
	QueueDurable bool

	// IdleTimeout is applied to pooled connections.
	IdleTimeout time.Duration
}

func (c Config) normalized() Config {
	if c.MaxConnections <= 0 {
		c.MaxConnections = 5
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ExchangeType == "" {
		c.ExchangeType = "direct"
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
