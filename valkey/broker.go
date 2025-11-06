package valkeymq

import (
	"context"
	"errors"
	"net"

	"github.com/hadi77ir/go-mq"
	valkey "github.com/valkey-io/valkey-go"
)

// publishOptsInternal is an internal struct for passing publish options within the valkey package.
type publishOptsInternal struct {
	Headers       map[string]string
	ReplyTo       string
	CorrelationID string
}

// consumeOptsInternal is an internal struct for passing consume options within the valkey package.
type consumeOptsInternal struct {
	Topic           string
	Queue           string
	ConsumerName    string
	AutoAck         bool
	Prefetch        int
	StartFromOldest bool
	DeadLetterTopic string
}

type clientWrapper struct {
	valkey.Client
}

func (c *clientWrapper) Close() error {
	c.Client.Close()
	return nil
}

// Broker implements mq.Broker for Valkey Streams and Pub/Sub.
type Broker struct {
	cfg  Config
	pool *mq.ConnectionPool[*clientWrapper]
}

// NewBroker constructs a Valkey-backed broker.
func NewBroker(ctx context.Context, cfg Config) (*Broker, error) {
	cfg = cfg.normalized()
	if err := cfg.validate(); err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	factory := func(ctx context.Context) (*clientWrapper, error) {
		client, err := buildClient(cfg)
		if err != nil {
			return nil, err
		}
		return &clientWrapper{Client: client}, nil
	}

	pool, err := mq.NewConnectionPool(factory, mq.PoolOptions{
		MaxSize:     cfg.MaxConnections,
		IdleTimeout: cfg.BlockTimeout * 6,
	})
	if err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	return &Broker{cfg: cfg, pool: pool}, nil
}

// Publish sends the message either via XADD or PUBLISH depending on configuration.
func (b *Broker) Publish(ctx context.Context, target string, msg mq.Message, opts ...mq.Option) error {
	if target == "" {
		return errors.Join(mq.ErrConfiguration, errors.New("valkey: target stream or channel is required"))
	}

	client, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(client) // nolint:errcheck

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

	publishOpts := publishOptsInternal{
		Headers:       headers,
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
	}

	switch b.cfg.PublishMode {
	case PublishModePubSub:
		if err := publishPubSub(ctx, client.Client, target, msg, publishOpts); err != nil {
			return errors.Join(mq.ErrPublishFailed, err)
		}
	case PublishModeList:
		if err := publishList(ctx, client.Client, target, msg, publishOpts); err != nil {
			return errors.Join(mq.ErrPublishFailed, err)
		}
	default:
		if err := b.publishStream(ctx, client.Client, target, msg, publishOpts); err != nil {
			return errors.Join(mq.ErrPublishFailed, err)
		}
	}
	return nil
}

func (b *Broker) publishStream(ctx context.Context, client valkey.Client, stream string, msg mq.Message, opts publishOptsInternal) error {
	return appendStreamEntry(ctx, b.cfg, client, stream, msg, opts)
}

// Consume returns a stream or pub/sub consumer depending on the options provided.
func (b *Broker) Consume(ctx context.Context, topic string, queue string, consumerName string, opts ...mq.Option) (mq.Consumer, error) {
	client, err := b.pool.Get(ctx)
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
		}
	}

	consumeOpts := consumeOptsInternal{
		Topic:           topic,
		Queue:           queue,
		ConsumerName:    consumerName,
		AutoAck:         autoAck,
		Prefetch:        prefetch,
		StartFromOldest: startFromOldest,
		DeadLetterTopic: deadLetterTopic,
	}

	if b.cfg.PublishMode == PublishModeList {
		if consumeOpts.Topic == "" {
			b.pool.Release(client) // nolint:errcheck
			return nil, errors.Join(mq.ErrConfiguration, errors.New("valkey: list queue name required"))
		}
		if consumeOpts.Queue != "" {
			b.pool.Release(client) // nolint:errcheck
			return nil, errors.Join(mq.ErrNotSupported, errors.New("valkey: queues not supported for list mode"))
		}
		if b.cfg.AutoCreateStream {
			if err := ensureListExists(ctx, client.Client, consumeOpts.Topic); err != nil {
				b.pool.Release(client) // nolint:errcheck
				return nil, errors.Join(mq.ErrConfiguration, err)
			}
			if consumeOpts.DeadLetterTopic != "" {
				if err := ensureListExists(ctx, client.Client, consumeOpts.DeadLetterTopic); err != nil {
					b.pool.Release(client) // nolint:errcheck
					return nil, errors.Join(mq.ErrConfiguration, err)
				}
			}
		}
		return newListConsumer(client, b.pool, b.cfg, consumeOpts), nil
	}

	if consumeOpts.Queue != "" {
		if consumeOpts.Topic == "" {
			b.pool.Release(client) // nolint:errcheck
			return nil, errors.Join(mq.ErrConfiguration, errors.New("valkey: stream name required for stream consumer"))
		}

		consumeName := consumeOpts.ConsumerName
		if consumeName == "" {
			consumeName = generateConsumerName()
		}

		if b.cfg.AutoCreateGroup {
			if err := ensureGroup(ctx, client.Client, consumeOpts.Topic, consumeOpts.Queue); err != nil {
				b.pool.Release(client) // nolint:errcheck
				return nil, errors.Join(mq.ErrConfiguration, err)
			}
		}

		return newStreamConsumer(client, b.pool, b.cfg, consumeOpts.Topic, consumeOpts.Queue, consumeName, consumeOpts), nil
	}

	if consumeOpts.Topic == "" {
		b.pool.Release(client) // nolint:errcheck
		return nil, errors.Join(mq.ErrConfiguration, errors.New("valkey: stream or channel required"))
	}

	// AutoCreateStream is respected for consistency, though pubsub channels are implicit
	if b.cfg.AutoCreateStream {
		// Pubsub channels don't need explicit creation, but we verify connectivity
		// by attempting a simple operation (no-op for pubsub)
	}

	return newPubSubConsumer(client, b.pool, consumeOpts.Topic, consumeOpts), nil
}

// CreateQueue creates the Valkey stream queue (consumer group), optionally creating the stream first.
func (b *Broker) CreateQueue(ctx context.Context, topic, queue string) error {
	if b.cfg.PublishMode == PublishModeList || b.cfg.PublishMode == PublishModePubSub {
		return errors.Join(mq.ErrNotSupported, errors.New("valkey: queues not supported in list/pubsub mode"))
	}
	client, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(client) // nolint:errcheck

	var completed valkey.Completed
	if b.cfg.AutoCreateStream {
		completed = client.Client.B().XgroupCreate().Key(topic).Group(queue).Id("0").Mkstream().Build()
	} else {
		completed = client.Client.B().XgroupCreate().Key(topic).Group(queue).Id("0").Build()
	}
	if err := ignoreBusyGroup(client.Client.Do(ctx, completed).Error()); err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// DeleteQueue destroys the Valkey queue (consumer group).
func (b *Broker) DeleteQueue(ctx context.Context, topic, queue string) error {
	if b.cfg.PublishMode == PublishModeList || b.cfg.PublishMode == PublishModePubSub {
		return errors.Join(mq.ErrNotSupported, errors.New("valkey: queues not supported in list/pubsub mode"))
	}
	client, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(client) // nolint:errcheck

	if err := client.Client.Do(ctx, client.Client.B().XgroupDestroy().Key(topic).Group(queue).Build()).Error(); err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// Flush ensures all pending operations are completed.
// For Valkey, this is a no-op as operations are synchronous, but we verify connection health.
func (b *Broker) Flush(ctx context.Context) error {
	client, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(client) // nolint:errcheck

	// Verify connection by doing a simple PING
	if err := client.Client.Do(ctx, client.Client.B().Ping().Build()).Error(); err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	return nil
}

// Close releases pooled clients.
func (b *Broker) Close(ctx context.Context) error {
	if err := b.pool.Close(); err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	return nil
}

func ignoreBusyGroup(err error) error {
	if err == nil {
		return nil
	}
	if valkey.IsValkeyBusyGroup(err) {
		return nil
	}
	return err
}

func ensureGroup(ctx context.Context, client valkey.Client, stream, group string) error {
	err := client.Do(ctx, client.B().XgroupCreate().Key(stream).Group(group).Id("0").Mkstream().Build()).Error()
	return ignoreBusyGroup(err)
}

func ensureListExists(ctx context.Context, client valkey.Client, key string) error {
	if key == "" {
		return nil
	}
	exists, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	if err := client.Do(ctx, client.B().Rpush().Key(key).Element("__go-mq-init__").Build()).Error(); err != nil {
		return err
	}
	if err := client.Do(ctx, client.B().Lpop().Key(key).Build()).Error(); err != nil {
		return err
	}
	return nil
}

func buildClient(cfg Config) (valkey.Client, error) {
	option := valkey.ClientOption{
		InitAddress:  cfg.Connection.Addresses,
		Username:     cfg.Connection.Username,
		Password:     cfg.Connection.Password,
		SelectDB:     cfg.Database,
		DisableCache: cfg.DisableCache,
		Dialer: net.Dialer{
			Timeout: cfg.DialTimeout,
		},
	}

	tlsConfig, err := mq.BuildTLSConfig(cfg.Connection)
	if err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}
	if tlsConfig != nil {
		option.TLSConfig = tlsConfig
	}

	client, err := valkey.NewClient(option)
	if err != nil {
		return nil, errors.Join(mq.ErrNoConnection, err)
	}
	return client, nil
}

func publishPubSub(ctx context.Context, client valkey.Client, channel string, msg mq.Message, opts publishOptsInternal) error {
	payload := encodePubSubPayload(msg, opts)
	// Convert byte slice to string for Valkey PUBLISH command
	return client.Do(ctx, client.B().Publish().Channel(channel).Message(string(payload)).Build()).Error()
}

func publishList(ctx context.Context, client valkey.Client, key string, msg mq.Message, opts publishOptsInternal) error {
	payload := encodePubSubPayload(msg, opts)
	// Convert byte slice to string for Valkey RPUSH command
	return client.Do(ctx, client.B().Rpush().Key(key).Element(string(payload)).Build()).Error()
}
