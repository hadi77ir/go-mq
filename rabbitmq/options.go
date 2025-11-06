package rabbitmq

import (
	"time"

	"github.com/hadi77ir/go-mq"
)

// MandatoryOption sets the mandatory flag for publishing (RabbitMQ-specific).
// When true, the message will be returned if it cannot be routed to any queue.
type MandatoryOption struct {
	Value bool
}

func (MandatoryOption) IsOption() {}

// WithMandatory returns a MandatoryOption.
func WithMandatory(mandatory bool) mq.Option {
	return MandatoryOption{Value: mandatory}
}

// PersistentOption sets the persistent flag for publishing (RabbitMQ-specific).
// When true, the message will be persisted to disk.
type PersistentOption struct {
	Value bool
}

func (PersistentOption) IsOption() {}

// WithPersistent returns a PersistentOption.
func WithPersistent(persistent bool) mq.Option {
	return PersistentOption{Value: persistent}
}

// ExpirationOption sets the message expiration duration for publishing (RabbitMQ-specific).
// Messages that are not consumed within this duration will be discarded.
type ExpirationOption struct {
	Value time.Duration
}

func (ExpirationOption) IsOption() {}

// WithExpiration returns an ExpirationOption.
func WithExpiration(expiration time.Duration) mq.Option {
	return ExpirationOption{Value: expiration}
}

// DelayOption sets the message delay duration for publishing (RabbitMQ-specific).
// Requires the RabbitMQ delayed message exchange plugin.
// The message will be delivered after the specified delay.
type DelayOption struct {
	Value time.Duration
}

func (DelayOption) IsOption() {}

// WithDelay returns a DelayOption.
func WithDelay(delay time.Duration) mq.Option {
	return DelayOption{Value: delay}
}
