package mqnats

import (
	"context"
	"errors"

	"github.com/hadi77ir/go-mq"
	"github.com/nats-io/nats.go"
)

func ensureConsumer(_ context.Context, js nats.JetStreamContext, cfg Config, subject, durable string, opts consumeOptsInternal) (*nats.ConsumerInfo, error) {
	info, err := js.ConsumerInfo(cfg.StreamName, durable)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, nats.ErrConsumerNotFound) {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	deliverPolicy := nats.DeliverNewPolicy
	if opts.StartFromOldest {
		deliverPolicy = nats.DeliverAllPolicy
	}

	consumerCfg := &nats.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       cfg.AckWait,
		DeliverPolicy: deliverPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
		FilterSubject: subject,
	}

	if opts.Prefetch > 0 {
		consumerCfg.MaxAckPending = opts.Prefetch
	}

	_, err = js.AddConsumer(cfg.StreamName, consumerCfg)
	if err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}
	info, err = js.ConsumerInfo(cfg.StreamName, durable)
	if err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}
	return info, nil
}
