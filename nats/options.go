package mqnats

import (
	"github.com/hadi77ir/go-mq"
)

// StartFromOldestOption sets whether to start consuming from the oldest message (NATS JetStream-specific).
// When true, the consumer will receive all messages from the beginning of the stream.
// When false, the consumer will only receive new messages published after subscription.
type StartFromOldestOption struct {
	Value bool
}

func (StartFromOldestOption) IsOption() {}

// WithStartFromOldest returns a StartFromOldestOption.
func WithStartFromOldest(startFromOldest bool) mq.Option {
	return StartFromOldestOption{Value: startFromOldest}
}
