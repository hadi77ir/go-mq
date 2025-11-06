package valkeymq

import (
	"context"
	"errors"
	"sync"

	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

type pubSubConsumer struct {
	client  *clientWrapper
	pool    *mq.ConnectionPool[*clientWrapper]
	channel string
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	autoAck bool

	messages chan *mq.Delivery
	errs     chan error

	closeOnce sync.Once
	closed    chan struct{}
}

func newPubSubConsumer(client *clientWrapper, pool *mq.ConnectionPool[*clientWrapper], channel string, opts consumeOptsInternal) mq.Consumer {
	msgCh := make(chan *mq.Delivery, 64)
	errCh := make(chan error, 1)
	internalCtx, cancel := context.WithCancel(context.Background())

	c := &pubSubConsumer{
		client:   client,
		pool:     pool,
		channel:  channel,
		cancel:   cancel,
		messages: msgCh,
		errs:     errCh,
		closed:   make(chan struct{}),
		autoAck:  opts.AutoAck,
	}

	c.wg.Add(1)
	go c.run(internalCtx)

	return c
}

func (c *pubSubConsumer) run(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.errs)
	err := c.client.Client.Receive(ctx, c.client.Client.B().Subscribe().Channel(c.channel).Build(), func(msg valkey.PubSubMessage) {
		// Convert string from PubSub to byte slice for CBOR decoding
		delivery := mq.NewDelivery(
			decodePubSubPayload([]byte(msg.Message)),
			mq.DeliveryMetadata{
				Topic:     msg.Channel,
				Partition: "",
			},
			func(context.Context) error { return nil },
			func(context.Context, bool) error { return nil },
		)

		// For pub/sub, AutoAck means we acknowledge immediately (no-op since pub/sub has no ack concept)
		if c.autoAck {
			_ = delivery.Ack(context.Background())
		}
		select {
		case c.messages <- delivery:
		case <-ctx.Done():
		}
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		select {
		case c.errs <- errors.Join(mq.ErrConsumeFailed, err):
		default:
		}
	}
	close(c.messages)
}

func (c *pubSubConsumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
	case <-c.closed:
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("valkey: consumer closed"))
	case err := <-c.errs:
		if err != nil {
			return nil, err
		}
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("valkey: subscription terminated"))
	case delivery, ok := <-c.messages:
		if !ok {
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("valkey: subscription terminated"))
		}
		return delivery, nil
	}
}

func (c *pubSubConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		c.cancel()
		c.wg.Wait()
		if releaseErr := c.pool.Release(c.client); releaseErr != nil {
			err = releaseErr
		}
	})
	if err != nil {
		return errors.Join(mq.ErrConnectionClosed, err)
	}
	return nil
}
