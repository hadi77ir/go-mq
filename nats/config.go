package mqnats

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/hadi77ir/go-mq"
)

// PublishMode controls how messages are published/consumed.
type PublishMode int

const (
	// PublishModeCore uses standard NATS subjects and queue subscribers (no acknowledgements).
	PublishModeCore PublishMode = iota
	// PublishModeJetStream uses JetStream streams and consumers with explicit acknowledgements.
	PublishModeJetStream
)

// Config defines NATS specific configuration.
type Config struct {
	Connection mq.Config

	// MaxConnections defines the size of the connection pool.
	MaxConnections int

	// IdleTimeout controls connection eviction in the pool.
	IdleTimeout time.Duration

	// PublishMode selects the messaging model.
	PublishMode PublishMode

	// StreamName is the JetStream stream used when PublishModeJetStream is selected.
	StreamName string

	// SubjectPrefix is prepended to subjects when publishing.
	SubjectPrefix string

	// PullBatch controls the number of messages fetched per request in JetStream pull mode.
	PullBatch int

	// PullTimeout limits the blocking duration when fetching JetStream messages.
	PullTimeout time.Duration

	// AckWait configures the JetStream consumer AckWait duration.
	AckWait time.Duration

	// TLSConfig allows callers to provide a fully configured tls.Config.
	TLSConfig *tls.Config
}

func (c Config) normalized() Config {
	if c.MaxConnections <= 0 {
		c.MaxConnections = 4
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 2 * time.Minute
	}
	if c.PullBatch <= 0 {
		c.PullBatch = 1
	}
	if c.PullTimeout == 0 {
		c.PullTimeout = 5 * time.Second
	}
	if c.AckWait == 0 {
		c.AckWait = 30 * time.Second
	}
	return c
}

func (c Config) validate() error {
	if len(c.Connection.Addresses) == 0 {
		return ErrNoAddresses
	}
	if c.PublishMode == PublishModeJetStream && c.StreamName == "" {
		return errors.New("nats: StreamName is required for JetStream mode")
	}
	return nil
}

var ErrNoAddresses = errors.New("nats: no addresses configured")
