package valkeymq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

type listConsumer struct {
	client     *clientWrapper
	pool       *mq.ConnectionPool[*clientWrapper]
	cfg        Config
	key        string
	deadLetter string
	block      time.Duration
	autoAck    bool

	closeOnce sync.Once
	closed    chan struct{}
}

func newListConsumer(client *clientWrapper, pool *mq.ConnectionPool[*clientWrapper], cfg Config, opts consumeOptsInternal) mq.Consumer {
	timeout := cfg.BlockTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	return &listConsumer{
		client:     client,
		pool:       pool,
		cfg:        cfg,
		key:        opts.Topic,
		deadLetter: opts.DeadLetterTopic,
		block:      timeout,
		autoAck:    opts.AutoAck,
		closed:     make(chan struct{}),
	}
}

func (c *listConsumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
		case <-c.closed:
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("valkey: consumer closed"))
		default:
		}

		delivery, err := c.pull(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, errors.Join(mq.ErrConsumeFailed, err)
			}
			if errors.Is(err, valkey.Nil) || valkey.IsValkeyNil(err) {
				continue
			}
			return nil, errors.Join(mq.ErrConsumeFailed, err)
		}
		if delivery == nil {
			continue
		}
		if c.autoAck {
			if err := delivery.Ack(ctx); err != nil {
				return nil, errors.Join(mq.ErrAcknowledgeFailed, err)
			}
		}
		return delivery, nil
	}
}

func (c *listConsumer) pull(ctx context.Context) (*mq.Delivery, error) {
	var timeout float64
	if c.block <= 0 {
		timeout = 0
	} else {
		timeout = c.block.Seconds()
	}

	completed := c.client.Client.B().Brpop().Key(c.key).Timeout(timeout).Build()
	result, err := c.client.Client.Do(ctx, completed).AsStrSlice()
	if err != nil {
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	if len(result) < 2 {
		return nil, nil
	}

	// Convert string from BRPOP to byte slice for CBOR decoding
	message := decodePubSubPayload([]byte(result[1]))
	meta := mq.DeliveryMetadata{
		Topic: c.key,
	}

	ack := func(context.Context) error { return nil }

	nack := func(ctx context.Context, requeue bool) error {
		payload := encodePubSubPayload(message, publishOptsInternal{})
		// Convert byte slice to string for Valkey RPUSH command
		payloadStr := string(payload)
		if requeue {
			return c.client.Client.Do(ctx, c.client.Client.B().Rpush().Key(c.key).Element(payloadStr).Build()).Error()
		}

		if c.deadLetter != "" {
			return c.client.Client.Do(ctx, c.client.Client.B().Rpush().Key(c.deadLetter).Element(payloadStr).Build()).Error()
		}
		return nil
	}

	return mq.NewDelivery(
		message,
		meta,
		func(ctx context.Context) error {
			if err := ack(ctx); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			return nil
		},
		func(ctx context.Context, requeue bool) error {
			if err := nack(ctx, requeue); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			return nil
		},
	), nil
}

func (c *listConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		if releaseErr := c.pool.Release(c.client); releaseErr != nil {
			err = releaseErr
		}
	})
	if err != nil {
		return errors.Join(mq.ErrConnectionClosed, err)
	}
	return nil
}
