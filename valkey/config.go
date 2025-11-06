package valkeymq

import (
	"errors"
	"time"

	"github.com/hadi77ir/go-mq"
)

// PublishMode indicates which Valkey primitive is used when publishing messages.
type PublishMode int

const (
	// PublishModeStreams publishes messages using XADD (Valkey Streams).
	PublishModeStreams PublishMode = iota
	// PublishModePubSub publishes messages using PUBLISH.
	PublishModePubSub
	// PublishModeList publishes messages to a list using RPUSH/BRPOP (push/pull).
	PublishModeList
)

// Config captures Valkey specific settings.
type Config struct {
	Connection mq.Config

	// MaxConnections defines the pool size.
	MaxConnections int

	// DialTimeout controls the connection dial timeout.
	DialTimeout time.Duration

	// Database selects the database index for standalone deployments.
	Database int

	// DisableCache toggles the Valkey client cache.
	DisableCache bool

	// StreamMaxLen trims the stream to the provided length when publishing (0 disables trimming).
	StreamMaxLen int64

	// ApproximateTrimming uses approximate trimming when StreamMaxLen is set.
	ApproximateTrimming bool

	// AutoCreateStream creates the stream when publishing if it does not exist.
	AutoCreateStream bool

	// AutoCreateGroup creates the consumer group if it is missing during Consume.
	AutoCreateGroup bool

	// BatchSize controls the default batch when pulling from streams.
	BatchSize int64

	// BlockTimeout controls the blocking duration used when consuming from streams.
	BlockTimeout time.Duration

	// PublishMode selects whether Publish uses Streams or Pub/Sub.
	PublishMode PublishMode
}

func (c Config) normalized() Config {
	if c.MaxConnections <= 0 {
		c.MaxConnections = 5
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
	if c.BlockTimeout == 0 {
		c.BlockTimeout = 5 * time.Second
	}
	return c
}

func (c Config) validate() error {
	if len(c.Connection.Addresses) == 0 {
		return ErrNoAddresses
	}
	return nil
}

var ErrNoAddresses = errors.New("valkey: no addresses configured")
