package mqnats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hadi77ir/go-mq"
	"github.com/nats-io/nats.go"
)

type jetStreamConsumer struct {
	conn       *connWrapper
	pool       *mq.ConnectionPool[*connWrapper]
	sub        *nats.Subscription
	js         nats.JetStreamContext
	cfg        Config
	opts       consumeOptsInternal
	durable    string
	subject    string
	deadLetter string

	buffer    []*nats.Msg
	closeOnce sync.Once
	closed    chan struct{}
	mu        sync.Mutex
}

func newJetStreamConsumer(conn *connWrapper, pool *mq.ConnectionPool[*connWrapper], sub *nats.Subscription, _ *nats.ConsumerInfo, cfg Config, opts consumeOptsInternal) mq.Consumer {
	js, _ := conn.Conn.JetStream()
	durable := opts.Queue
	if durable == "" {
		durable = opts.ConsumerName
	}
	if durable == "" {
		durable = "go-mq-" + time.Now().Format("20060102T150405.000000000")
	}
	return &jetStreamConsumer{
		conn:       conn,
		pool:       pool,
		sub:        sub,
		js:         js,
		cfg:        cfg,
		opts:       opts,
		durable:    durable,
		subject:    composeSubject(cfg.SubjectPrefix, opts.Topic),
		deadLetter: composeSubject(cfg.SubjectPrefix, opts.DeadLetterTopic),
		buffer:     make([]*nats.Msg, 0),
		closed:     make(chan struct{}),
	}
}

func (c *jetStreamConsumer) Receive(ctx context.Context) (*mq.Delivery, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
		case <-c.closed:
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("nats: consumer closed"))
		default:
		}

		msg := c.nextBuffered()
		if msg == nil {
			if err := c.fetch(ctx); err != nil {
				return nil, err
			}
			continue
		}

		delivery := c.convertMessage(msg)
		if delivery != nil {
			if c.opts.AutoAck {
				if err := delivery.Ack(ctx); err != nil {
					return nil, errors.Join(mq.ErrAcknowledgeFailed, err)
				}
			}
			return delivery, nil
		}
	}
}

func (c *jetStreamConsumer) nextBuffered() *nats.Msg {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffer) == 0 {
		return nil
	}
	msg := c.buffer[0]
	c.buffer = c.buffer[1:]
	return msg
}

func (c *jetStreamConsumer) fetch(ctx context.Context) error {
	batch := c.cfg.PullBatch
	if c.opts.Prefetch > 0 {
		batch = c.opts.Prefetch
	}
	if batch <= 0 {
		batch = 1
	}

	fetchCtx := ctx
	var cancel context.CancelFunc
	if c.cfg.PullTimeout > 0 {
		fetchCtx, cancel = context.WithTimeout(ctx, c.cfg.PullTimeout)
		defer cancel()
	}

	msgs, err := c.sub.Fetch(batch, nats.Context(fetchCtx))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return errors.Join(mq.ErrConsumeFailed, err)
		}
		if errors.Is(err, nats.ErrTimeout) {
			return errors.Join(mq.ErrConsumeFailed, context.DeadlineExceeded)
		}
		return errors.Join(mq.ErrConsumeFailed, err)
	}

	if len(msgs) == 0 {
		return errors.Join(mq.ErrConsumeFailed, context.DeadlineExceeded)
	}

	c.mu.Lock()
	c.buffer = append(c.buffer, msgs...)
	c.mu.Unlock()
	return nil
}

func (c *jetStreamConsumer) convertMessage(msg *nats.Msg) *mq.Delivery {
	headers := make(map[string]string, len(msg.Header))
	for k, vals := range msg.Header {
		if len(vals) > 0 {
			headers[k] = vals[0]
		}
	}

	meta := mq.DeliveryMetadata{
		Topic:        c.opts.Topic,
		Queue:        c.opts.Queue,
		ConsumerName: c.opts.ConsumerName,
	}
	if md, err := msg.Metadata(); err == nil {
		meta.ID = fmtSequence(md.Sequence.Stream, md.Sequence.Consumer)
	}

	message := mq.Message{
		Key:         msg.Subject,
		Body:        msg.Data,
		Headers:     headers,
		ContentType: msg.Header.Get("Content-Type"),
	}

	return mq.NewDelivery(
		message,
		meta,
		func(ctx context.Context) error {
			if err := msg.Ack(); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			return nil
		},
		func(ctx context.Context, requeue bool) error {
			if requeue {
				if err := msg.Nak(); err != nil {
					return errors.Join(mq.ErrAcknowledgeFailed, err)
				}
				return nil
			}
			if err := msg.Ack(); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			if c.deadLetter == "" {
				return nil
			}
			headerCopy := nats.Header{}
			for k, vals := range msg.Header {
				for _, v := range vals {
					headerCopy.Add(k, v)
				}
			}
			payload := &nats.Msg{
				Subject: c.deadLetter,
				Data:    msg.Data,
				Header:  headerCopy,
			}
			if _, err := c.js.PublishMsg(payload); err != nil {
				return errors.Join(mq.ErrAcknowledgeFailed, err)
			}
			return nil
		},
	)
}

func (c *jetStreamConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		if c.sub != nil {
			if drainErr := c.sub.Drain(); drainErr != nil && err == nil {
				err = drainErr
			}
		}
		if releaseErr := c.pool.Release(c.conn); releaseErr != nil && err == nil {
			err = releaseErr
		}
	})
	if err != nil {
		return errors.Join(mq.ErrConnectionClosed, err)
	}
	return nil
}

func fmtSequence(streamSeq, consumerSeq uint64) string {
	return fmt.Sprintf("%d:%d", streamSeq, consumerSeq)
}
