package mq

import "errors"

var (
	// ErrConfiguration is returned when configuration validation fails.
	ErrConfiguration = errors.New("mq: configuration error")
	// ErrNoConnection indicates that a connection could not be established or has been lost.
	ErrNoConnection = errors.New("mq: no connection available")
	// ErrConnectionClosed signals that the underlying connection was closed unexpectedly.
	ErrConnectionClosed = errors.New("mq: connection closed")
	// ErrPublishFailed indicates a failure while publishing a message.
	ErrPublishFailed = errors.New("mq: publish failed")
	// ErrConsumeFailed indicates a failure while consuming messages.
	ErrConsumeFailed = errors.New("mq: consume failed")
	// ErrAcknowledgeFailed indicates a failure when acknowledging deliveries.
	ErrAcknowledgeFailed = errors.New("mq: acknowledge failed")
	// ErrNotSupported indicates that the requested operation is not supported in the current mode or configuration.
	ErrNotSupported = errors.New("mq: operation not supported")
)

// annotate returns sentinel when err is nil, otherwise joins sentinel and err for preserved context.
func annotate(sentinel, err error) error {
	if err == nil {
		return sentinel
	}
	return errors.Join(sentinel, err)
}
