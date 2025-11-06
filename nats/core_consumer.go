package mqnats

import (
	"context"
	"errors"
	"sync"

	"github.com/hadi77ir/go-mq"
	"github.com/nats-io/nats.go"
)

type coreConsumer struct {
	conn      *connWrapper
	pool      *mq.ConnectionPool[*connWrapper]
	sub       *nats.Subscription
	messages  chan *mq.Delivery
	errs      chan error
	closeOnce sync.Once
	closed    chan struct{}
}

func newCoreConsumer(conn *connWrapper, pool *mq.ConnectionPool[*connWrapper], subject string, opts consumeOptsInternal) mq.Consumer {
	msgCh := make(chan *mq.Delivery, 64)
	errCh := make(chan error, 1)
	closed := make(chan struct{})

	c := &coreConsumer{
		conn:     conn,
		pool:     pool,
		messages: msgCh,
		errs:     errCh,
		closed:   closed,
	}

	handler := func(msg *nats.Msg) {
		delivery := buildCoreDelivery(subject, opts.Queue, msg)
		select {
		case msgCh <- delivery:
		default:
			// backpressure: drop oldest to avoid blocking; rare since buffer sized.
			select {
			case <-msgCh:
			default:
			}
			msgCh <- delivery
		}
	}

	var sub *nats.Subscription
	var err error
	nc := conn.Conn
	if opts.Queue != "" {
		sub, err = nc.QueueSubscribe(subject, opts.Queue, handler)
	} else {
		sub, err = nc.Subscribe(subject, handler)
	}
	if err != nil {
		errCh <- errors.Join(mq.ErrConsumeFailed, err)
		close(msgCh)
	} else {
		c.sub = sub
		if flushErr := nc.Flush(); flushErr != nil {
			if c.sub != nil {
				_ = c.sub.Drain()
				c.sub = nil
			}
			errCh <- errors.Join(mq.ErrConsumeFailed, flushErr)
			close(msgCh)
		}
	}

	return c
}

func buildCoreDelivery(subject, group string, msg *nats.Msg) *mq.Delivery {
	headers := make(map[string]string, len(msg.Header))
	for k, vals := range msg.Header {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}

	deliveryMsg := mq.Message{
		Key:         msg.Subject,
		Body:        msg.Data,
		Headers:     headers,
		ContentType: msg.Header.Get("Content-Type"),
	}

	meta := mq.DeliveryMetadata{
		Topic: subject,
		Queue: group,
	}

	return mq.NewDelivery(deliveryMsg, meta,
		func(context.Context) error { return nil },
		func(context.Context, bool) error { return nil },
	)
}

func (c *coreConsumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
	case err := <-c.errs:
		if err != nil {
			return nil, err
		}
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("nats: subscription closed"))
	case msg, ok := <-c.messages:
		if !ok {
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("nats: subscription closed"))
		}
		return msg, nil
	case <-c.closed:
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("nats: consumer closed"))
	}
}

func (c *coreConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.sub != nil {
			if unsubErr := c.sub.Drain(); unsubErr != nil && err == nil {
				err = unsubErr
			}
		}
		close(c.messages)
		if releaseErr := c.pool.Release(c.conn); releaseErr != nil && err == nil {
			err = releaseErr
		}
	})
	if err != nil {
		return errors.Join(mq.ErrConnectionClosed, err)
	}
	return nil
}
