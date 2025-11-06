package rabbitmq

import (
	"context"
	"errors"
	"sync"

	"github.com/hadi77ir/go-mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type consumer struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	pool       *mq.ConnectionPool[*amqp.Connection]
	queueName  string
	tag        string

	closeOnce sync.Once
	closed    chan struct{}
}

func newConsumer(conn *amqp.Connection, ch *amqp.Channel, deliveries <-chan amqp.Delivery, pool *mq.ConnectionPool[*amqp.Connection], queueName, tag string) mq.Consumer {
	return &consumer{
		conn:       conn,
		channel:    ch,
		deliveries: deliveries,
		pool:       pool,
		queueName:  queueName,
		tag:        tag,
		closed:     make(chan struct{}),
	}
}

func (c *consumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
	case <-c.closed:
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("rabbitmq: consumer closed"))
	case delivery, ok := <-c.deliveries:
		if !ok {
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("rabbitmq: deliveries channel closed"))
		}
		msgHeaders := make(map[string]string, len(delivery.Headers))
		for k, v := range delivery.Headers {
			if str, ok := v.(string); ok {
				msgHeaders[k] = str
			}
		}

		msg := mq.Message{
			Key:         delivery.MessageId,
			Body:        delivery.Body,
			Headers:     msgHeaders,
			ContentType: delivery.ContentType,
			Timestamp:   delivery.Timestamp,
		}

		meta := mq.DeliveryMetadata{
			ID:           delivery.MessageId,
			Topic:        delivery.RoutingKey,
			Queue:        c.queueName,
			ConsumerName: c.tag,
			Redelivered:  delivery.Redelivered,
		}

		return mq.NewDelivery(
			msg,
			meta,
			func(_ context.Context) error {
				if err := delivery.Ack(false); err != nil {
					return errors.Join(mq.ErrAcknowledgeFailed, err)
				}
				return nil
			},
			func(_ context.Context, requeue bool) error {
				if err := delivery.Nack(false, requeue); err != nil {
					return errors.Join(mq.ErrAcknowledgeFailed, err)
				}
				return nil
			},
		), nil
	}
}

func (c *consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.tag != "" {
			if chErr := c.channel.Cancel(c.tag, false); chErr != nil {
				err = chErr
			}
		}
		if chErr := c.channel.Close(); chErr != nil && err == nil {
			err = chErr
		}
		if relErr := c.pool.Release(c.conn); relErr != nil && err == nil {
			err = relErr
		}
	})
	if err != nil {
		return errors.Join(mq.ErrConnectionClosed, err)
	}
	return nil
}
