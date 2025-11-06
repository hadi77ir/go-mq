package valkeymq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

type streamConsumer struct {
	client          *clientWrapper
	pool            *mq.ConnectionPool[*clientWrapper]
	cfg             Config
	stream          string
	group           string
	consumer        string
	batchSize       int64
	block           time.Duration
	deadLetter      string
	autoAck         bool
	startFromOldest bool

	closeOnce sync.Once
	closed    chan struct{}
}

func newStreamConsumer(client *clientWrapper, pool *mq.ConnectionPool[*clientWrapper], cfg Config, stream, group, consumer string, opts consumeOptsInternal) mq.Consumer {
	batch := cfg.BatchSize
	if opts.Prefetch > 0 {
		batch = int64(opts.Prefetch)
	}

	block := cfg.BlockTimeout
	if block <= 0 {
		block = 5 * time.Second
	}

	return &streamConsumer{
		client:          client,
		pool:            pool,
		cfg:             cfg,
		stream:          stream,
		group:           group,
		consumer:        consumer,
		batchSize:       batch,
		block:           block,
		deadLetter:      opts.DeadLetterTopic,
		autoAck:         opts.AutoAck,
		startFromOldest: opts.StartFromOldest,
		closed:          make(chan struct{}),
	}
}

func (c *streamConsumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
		case <-c.closed:
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("valkey: consumer closed"))
		default:
		}

		delivery, err := c.fetch(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, errors.Join(mq.ErrConsumeFailed, err)
			}
			if errors.Is(err, valkey.Nil) {
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

func (c *streamConsumer) fetch(ctx context.Context) (*mq.Delivery, error) {
	blockMs := c.block.Milliseconds()
	if blockMs <= 0 {
		blockMs = 5000
	}

	// Use "0-0" to start from oldest when StartFromOldest is true, otherwise use ">" for new messages
	startID := ">"
	if c.startFromOldest {
		startID = "0-0"
	}

	var completed valkey.Completed
	switch {
	case c.batchSize > 0 && blockMs > 0:
		completed = c.client.Client.B().Xreadgroup().Group(c.group, c.consumer).Count(c.batchSize).Block(blockMs).Streams().Key(c.stream).Id(startID).Build()
	case c.batchSize > 0:
		completed = c.client.Client.B().Xreadgroup().Group(c.group, c.consumer).Count(c.batchSize).Streams().Key(c.stream).Id(startID).Build()
	case blockMs > 0:
		completed = c.client.Client.B().Xreadgroup().Group(c.group, c.consumer).Block(blockMs).Streams().Key(c.stream).Id(startID).Build()
	default:
		completed = c.client.Client.B().Xreadgroup().Group(c.group, c.consumer).Streams().Key(c.stream).Id(startID).Build()
	}

	res, err := c.client.Client.Do(ctx, completed).AsXRead()
	if err != nil {
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	entries := res[c.stream]
	if len(entries) == 0 {
		return nil, nil
	}

	entry := entries[0]
	message := decodeMessage(entry)
	meta := mq.DeliveryMetadata{
		ID:           entry.ID,
		Topic:        c.stream,
		Queue:        c.group,
		ConsumerName: c.consumer,
	}

	ack := func(ctx context.Context) error {
		return c.client.Client.Do(ctx, c.client.Client.B().Xack().Key(c.stream).Group(c.group).Id(entry.ID).Build()).Error()
	}

	requeue := func(ctx context.Context, target string) error {
		return appendStreamEntry(ctx, c.cfg, c.client.Client, target, message, publishOptsInternal{})
	}

	nack := func(ctx context.Context, requeueFlag bool) error {
		if requeueFlag {
			if err := ack(ctx); err != nil {
				return err
			}
			return requeue(ctx, c.stream)
		}

		if err := ack(ctx); err != nil {
			return err
		}
		if c.deadLetter != "" {
			return requeue(ctx, c.deadLetter)
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
		func(ctx context.Context, requeueFlag bool) error {
			if err := nack(ctx, requeueFlag); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			return nil
		},
	), nil
}

func (c *streamConsumer) Close() error {
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
