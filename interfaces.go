package mq

import (
	"context"
	"time"
)

// Message represents a queue message payload and metadata that can be shared across implementations.
type Message struct {
	Key         string
	Body        []byte
	Headers     map[string]string
	ContentType string
	Timestamp   time.Time
}

// DeliveryMetadata captures attributes related to the delivery event.
type DeliveryMetadata struct {
	ID           string
	Topic        string
	Partition    string
	Queue        string
	ConsumerName string
	Redelivered  bool
}

// Delivery wraps a Message with acknowledgement controls and metadata.
type Delivery struct {
	Message  Message
	Metadata DeliveryMetadata

	ackFunc  func(context.Context) error
	nackFunc func(context.Context, bool) error
}

// NewDelivery constructs a Delivery with the provided acknowledgement callbacks.
func NewDelivery(msg Message, meta DeliveryMetadata, ack func(context.Context) error, nack func(context.Context, bool) error) *Delivery {
	return &Delivery{
		Message:  msg,
		Metadata: meta,
		ackFunc:  ack,
		nackFunc: nack,
	}
}

// Ack acknowledges successful processing of the delivery.
func (d *Delivery) Ack(ctx context.Context) error {
	if d.ackFunc == nil {
		return nil
	}
	return d.ackFunc(ctx)
}

// Nack signals that processing failed. Requeue indicates if the message should be re-delivered.
func (d *Delivery) Nack(ctx context.Context, requeue bool) error {
	if d.nackFunc == nil {
		return nil
	}
	return d.nackFunc(ctx, requeue)
}

// Option is a marker interface for functional options.
type Option interface {
	IsOption()
}

// AutoAckOption sets the auto-acknowledge option for consuming.
type AutoAckOption struct {
	Value bool
}

func (AutoAckOption) IsOption() {}

// WithAutoAck returns an AutoAckOption.
func WithAutoAck(autoAck bool) Option {
	return AutoAckOption{Value: autoAck}
}

// PrefetchOption sets the prefetch count for consuming.
type PrefetchOption struct {
	Value int
}

func (PrefetchOption) IsOption() {}

// WithPrefetch returns a PrefetchOption.
func WithPrefetch(prefetch int) Option {
	return PrefetchOption{Value: prefetch}
}

// DeadLetterTopicOption sets the dead letter topic for consuming.
type DeadLetterTopicOption struct {
	Value string
}

func (DeadLetterTopicOption) IsOption() {}

// WithDeadLetterTopic returns a DeadLetterTopicOption.
func WithDeadLetterTopic(deadLetterTopic string) Option {
	return DeadLetterTopicOption{Value: deadLetterTopic}
}

// HeadersOption sets additional headers for publishing.
type HeadersOption struct {
	Value map[string]string
}

func (HeadersOption) IsOption() {}

// WithHeaders returns a HeadersOption.
func WithHeaders(headers map[string]string) Option {
	return HeadersOption{Value: headers}
}

// ReplyToOption sets the reply-to address for publishing.
type ReplyToOption struct {
	Value string
}

func (ReplyToOption) IsOption() {}

// WithReplyTo returns a ReplyToOption.
func WithReplyTo(replyTo string) Option {
	return ReplyToOption{Value: replyTo}
}

// CorrelationIDOption sets the correlation ID for publishing.
type CorrelationIDOption struct {
	Value string
}

func (CorrelationIDOption) IsOption() {}

// WithCorrelationID returns a CorrelationIDOption.
func WithCorrelationID(correlationID string) Option {
	return CorrelationIDOption{Value: correlationID}
}

// Consumer receives deliveries sequentially.
type Consumer interface {
	Receive(ctx context.Context) (*Delivery, error)
	Close() error
}

// Publisher publishes messages to a broker target such as an exchange, routing key, topic or stream.
type Publisher interface {
	Publish(ctx context.Context, target string, msg Message, opts ...Option) error
}

// Broker describes a client that supports a shared set of operations across message queue systems.
// All Broker implementations provided by go-mq are safe for concurrent use by multiple goroutines.
type Broker interface {
	Publisher

	Consume(ctx context.Context, topic string, queue string, consumerName string, opts ...Option) (Consumer, error)
	CreateQueue(ctx context.Context, topic string, queue string) error
	DeleteQueue(ctx context.Context, topic string, queue string) error
	Flush(ctx context.Context) error // Flush ensures all pending operations are completed
	Close(ctx context.Context) error
}
