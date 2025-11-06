package rabbitmq

import "errors"

var (
	// ErrNoAddresses is returned when no RabbitMQ brokers are configured.
	ErrNoAddresses = errors.New("rabbitmq: no addresses configured")
)
