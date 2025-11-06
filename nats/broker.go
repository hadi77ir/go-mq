package mqnats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hadi77ir/go-mq"
	"github.com/nats-io/nats.go"
)

// consumeOptsInternal is an internal struct for passing consume options within the nats package.
type consumeOptsInternal struct {
	Topic           string
	Queue           string
	ConsumerName    string
	AutoAck         bool
	Prefetch        int
	StartFromOldest bool
	DeadLetterTopic string
}

// Broker implements mq.Broker for NATS.
type Broker struct {
	cfg  Config
	pool *mq.ConnectionPool[*connWrapper]
}

type connWrapper struct {
	*nats.Conn
}

func (c *connWrapper) Close() error {
	if c.Conn != nil {
		c.Conn.Close()
	}
	return nil
}

// NewBroker creates a pooled broker backed by NATS connections.
func NewBroker(ctx context.Context, cfg Config) (*Broker, error) {
	cfg = cfg.normalized()
	if err := cfg.validate(); err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	factory := func(ctx context.Context) (*connWrapper, error) {
		opts, err := buildOptions(cfg)
		if err != nil {
			return nil, err
		}
		opts.Timeout = cfg.ConnectionTimeout()
		conn, err := opts.Connect()
		if err != nil {
			return nil, errors.Join(mq.ErrNoConnection, err)
		}
		return &connWrapper{Conn: conn}, nil
	}

	pool, err := mq.NewConnectionPool(factory, mq.PoolOptions{
		MaxSize:     cfg.MaxConnections,
		IdleTimeout: cfg.IdleTimeout,
	})
	if err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	b := &Broker{
		cfg:  cfg,
		pool: pool,
	}

	if cfg.PublishMode == PublishModeJetStream {
		if err := b.ensureStream(ctx); err != nil {
			_ = pool.Close()
			return nil, err
		}
	}

	return b, nil
}

func (c Config) ConnectionTimeout() time.Duration {
	if c.Connection.UseTLS || c.TLSConfig != nil {
		return 10 * time.Second
	}
	return 5 * time.Second
}

func (b *Broker) ensureStream(ctx context.Context) error {
	wrapper, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(wrapper) // nolint:errcheck

	conn := wrapper.Conn

	js, err := conn.JetStream()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}

	subject := composeSubject(b.cfg.SubjectPrefix, ">")
	stream, err := js.StreamInfo(b.cfg.StreamName)
	if err == nil && stream != nil {
		return nil
	}
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return errors.Join(mq.ErrConfiguration, err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     b.cfg.StreamName,
		Subjects: []string{subject},
		Storage:  nats.FileStorage,
		Discard:  nats.DiscardOld,
		MaxAge:   0,
		Replicas: 1,
	})
	if err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// Publish sends a message to the desired subject or stream.
func (b *Broker) Publish(ctx context.Context, target string, msg mq.Message, opts ...mq.Option) error {
	subject := composeSubject(b.cfg.SubjectPrefix, target)
	if subject == "" {
		return errors.Join(mq.ErrConfiguration, errors.New("nats: subject is required"))
	}
	wrapper, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(wrapper) // nolint:errcheck

	conn := wrapper.Conn

	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    msg.Body,
		Header:  nats.Header{},
	}

	if msg.ContentType != "" {
		natsMsg.Header.Set("Content-Type", msg.ContentType)
	}
	if !msg.Timestamp.IsZero() {
		natsMsg.Header.Set("X-Timestamp", msg.Timestamp.UTC().Format(time.RFC3339Nano))
	}
	for k, v := range msg.Headers {
		natsMsg.Header.Set(k, v)
	}

	var headers map[string]string
	var replyTo string
	var correlationID string

	for _, opt := range opts {
		switch o := opt.(type) {
		case mq.HeadersOption:
			if headers == nil {
				headers = make(map[string]string)
			}
			for k, v := range o.Value {
				headers[k] = v
			}
		case mq.ReplyToOption:
			replyTo = o.Value
		case mq.CorrelationIDOption:
			correlationID = o.Value
		}
	}

	for k, v := range headers {
		natsMsg.Header.Set(k, v)
	}
	if correlationID != "" {
		natsMsg.Header.Set("X-Correlation-ID", correlationID)
	}
	if replyTo != "" {
		natsMsg.Reply = replyTo
	}

	switch b.cfg.PublishMode {
	case PublishModeCore:
		if err := conn.PublishMsg(natsMsg); err != nil {
			return errors.Join(mq.ErrPublishFailed, err)
		}
	case PublishModeJetStream:
		js, err := conn.JetStream()
		if err != nil {
			return errors.Join(mq.ErrNoConnection, err)
		}
		if _, err = js.PublishMsg(natsMsg); err != nil {
			return errors.Join(mq.ErrPublishFailed, err)
		}
	default:
		return errors.Join(mq.ErrConfiguration, fmt.Errorf("nats: unsupported publish mode %d", b.cfg.PublishMode))
	}
	return nil
}

// Consume subscribes to messages using the configured mode.
func (b *Broker) Consume(ctx context.Context, topic string, queue string, consumerName string, opts ...mq.Option) (mq.Consumer, error) {
	wrapper, err := b.pool.Get(ctx)
	if err != nil {
		return nil, errors.Join(mq.ErrNoConnection, err)
	}

	var autoAck bool
	var prefetch int
	var startFromOldest bool
	var deadLetterTopic string

	for _, opt := range opts {
		switch o := opt.(type) {
		case mq.AutoAckOption:
			autoAck = o.Value
		case mq.PrefetchOption:
			prefetch = o.Value
		case mq.DeadLetterTopicOption:
			deadLetterTopic = o.Value
		case StartFromOldestOption:
			startFromOldest = o.Value
		}
	}

	if b.cfg.PublishMode == PublishModeCore {
		subject := composeSubject(b.cfg.SubjectPrefix, topic)
		if subject == "" {
			b.pool.Release(wrapper) // nolint:errcheck
			return nil, errors.Join(mq.ErrConfiguration, errors.New("nats: subject required for core mode"))
		}
		// Create a simple struct for core consumer
		consumeOpts := consumeOptsInternal{
			Topic:           topic,
			Queue:           queue,
			ConsumerName:    consumerName,
			AutoAck:         autoAck,
			Prefetch:        prefetch,
			DeadLetterTopic: deadLetterTopic,
		}
		return newCoreConsumer(wrapper, b.pool, subject, consumeOpts), nil
	}

	conn := wrapper.Conn
	js, err := conn.JetStream()
	if err != nil {
		b.pool.Release(wrapper) // nolint:errcheck
		return nil, errors.Join(mq.ErrNoConnection, err)
	}

	subject := composeSubject(b.cfg.SubjectPrefix, topic)
	if subject == "" {
		b.pool.Release(wrapper) // nolint:errcheck
		return nil, errors.Join(mq.ErrConfiguration, errors.New("nats: subject required for JetStream mode"))
	}

	durable := queue
	if durable == "" {
		durable = consumerName
	}
	if durable == "" {
		durable = "go-mq-" + fmt.Sprint(time.Now().UnixNano())
	}

	consumeName := consumerName
	if consumeName == "" {
		consumeName = durable
	}

	// Create internal struct for ensureConsumer
	consumeOpts := consumeOptsInternal{
		Topic:           topic,
		Queue:           durable,
		ConsumerName:    consumeName,
		AutoAck:         autoAck,
		Prefetch:        prefetch,
		StartFromOldest: startFromOldest,
		DeadLetterTopic: deadLetterTopic,
	}

	consumer, err := ensureConsumer(ctx, js, b.cfg, subject, durable, consumeOpts)
	if err != nil {
		b.pool.Release(wrapper) // nolint:errcheck
		return nil, err
	}

	subscription, err := js.PullSubscribe(subject, durable, nats.Bind(b.cfg.StreamName, durable))
	if err != nil {
		b.pool.Release(wrapper) // nolint:errcheck
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	return newJetStreamConsumer(wrapper, b.pool, subscription, consumer, b.cfg, consumeOpts), nil
}

// CreateQueue ensures the JetStream durable consumer exists.
func (b *Broker) CreateQueue(ctx context.Context, topic, queue string) error {
	if b.cfg.PublishMode != PublishModeJetStream {
		return errors.Join(mq.ErrNotSupported, errors.New("nats: queues require JetStream mode"))
	}

	wrapper, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(wrapper) // nolint:errcheck

	js, err := wrapper.Conn.JetStream()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}

	subject := composeSubject(b.cfg.SubjectPrefix, topic)
	_, err = ensureConsumer(ctx, js, b.cfg, subject, queue, consumeOptsInternal{
		Topic:        topic,
		Queue:        queue,
		ConsumerName: queue,
	})
	return err
}

// DeleteQueue removes a JetStream durable consumer.
func (b *Broker) DeleteQueue(ctx context.Context, topic, queue string) error {
	if b.cfg.PublishMode != PublishModeJetStream {
		return errors.Join(mq.ErrNotSupported, errors.New("nats: queues require JetStream mode"))
	}

	wrapper, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(wrapper) // nolint:errcheck

	js, err := wrapper.Conn.JetStream()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}

	if err := js.DeleteConsumer(b.cfg.StreamName, queue); err != nil {
		if errors.Is(err, nats.ErrConsumerNotFound) {
			return nil
		}
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// Close releases the pool.
func (b *Broker) Close(ctx context.Context) error {
	if err := b.pool.Close(); err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	return nil
}

func composeSubject(prefix, subject string) string {
	subject = strings.TrimSpace(subject)
	if prefix == "" {
		return subject
	}
	if subject == "" {
		return prefix
	}
	if strings.HasSuffix(prefix, ".") {
		return prefix + subject
	}
	return prefix + "." + subject
}

func buildOptions(cfg Config) (*nats.Options, error) {
	urls := make([]string, 0, len(cfg.Connection.Addresses))
	for _, addr := range cfg.Connection.Addresses {
		if strings.HasPrefix(addr, "nats://") || strings.HasPrefix(addr, "tls://") {
			urls = append(urls, addr)
		} else {
			scheme := "nats"
			if cfg.Connection.UseTLS || cfg.TLSConfig != nil {
				scheme = "tls"
			}
			urls = append(urls, fmt.Sprintf("%s://%s", scheme, addr))
		}
	}

	options := &nats.Options{
		Servers:  urls,
		User:     cfg.Connection.Username,
		Password: cfg.Connection.Password,
	}

	if cfg.TLSConfig != nil {
		options.Secure = true
		options.TLSConfig = cfg.TLSConfig.Clone()
	} else {
		tlsConfig, err := mq.BuildTLSConfig(cfg.Connection)
		if err != nil {
			return nil, errors.Join(mq.ErrConfiguration, err)
		}
		if tlsConfig != nil {
			options.Secure = true
			options.TLSConfig = tlsConfig
		}
	}

	return options, nil
}
