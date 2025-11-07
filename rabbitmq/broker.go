package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/hadi77ir/go-mq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// Broker is a RabbitMQ implementation of the mq.Broker interface.
type Broker struct {
	cfg  Config
	pool *mq.ConnectionPool[*amqp.Connection]

	streamEnv       *stream.Environment
	streamProducers sync.Map // map[string]*stream.SuperStreamProducer
	streamDeclared  sync.Map // map[string]struct{}
}

// NewBroker creates a Broker backed by a connection pool.
func NewBroker(ctx context.Context, cfg Config) (*Broker, error) {
	cfg = cfg.normalized()
	if err := cfg.validate(); err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	isStreamMode := cfg.PublishMode == PublishModeStreams

	var pool *mq.ConnectionPool[*amqp.Connection]
	var err error
	if !isStreamMode {
		factory := func(ctx context.Context) (*amqp.Connection, error) {
			amqpCfg, err := buildAMQPConfig(cfg)
			if err != nil {
				return nil, errors.Join(mq.ErrConfiguration, err)
			}

			var lastErr error
			for _, address := range cfg.Connection.Addresses {
				select {
				case <-ctx.Done():
					return nil, errors.Join(mq.ErrNoConnection, ctx.Err())
				default:
				}

				uri, err := buildAMQPURI(cfg.Connection, address)
				if err != nil {
					lastErr = errors.Join(mq.ErrConfiguration, err)
					continue
				}
				conn, err := amqp.DialConfig(uri, *amqpCfg)
				if err != nil {
					lastErr = errors.Join(mq.ErrNoConnection, err)
					continue
				}
				return conn, nil
			}
			if lastErr == nil {
				lastErr = errors.Join(mq.ErrNoConnection, fmt.Errorf("rabbitmq: unable to connect"))
			}
			return nil, lastErr
		}

		pool, err = mq.NewConnectionPool(factory, mq.PoolOptions{
			MaxSize:     cfg.MaxConnections,
			IdleTimeout: cfg.IdleTimeout,
		})
		if err != nil {
			return nil, errors.Join(mq.ErrConfiguration, err)
		}
	}

	var streamEnv *stream.Environment
	if isStreamMode {
		streamEnv, err = newStreamEnvironment(cfg)
		if err != nil {
			return nil, errors.Join(mq.ErrConfiguration, err)
		}
	}

	b := &Broker{
		cfg:            cfg,
		pool:           pool,
		streamEnv:      streamEnv,
		streamDeclared: sync.Map{},
	}

	if !isStreamMode && cfg.DeclareExchange && cfg.Exchange != "" {
		if err := b.ensureExchange(ctx); err != nil {
			_ = pool.Close()
			return nil, err
		}
	}

	return b, nil
}

func (b *Broker) ensureExchange(ctx context.Context) error {
	if b.pool == nil {
		return nil
	}
	conn, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(conn) // nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer ch.Close() // nolint:errcheck

	if err := ch.ExchangeDeclare(
		b.cfg.Exchange,
		b.cfg.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// Publish sends a message to the configured exchange using the provided routing key.
func (b *Broker) Publish(ctx context.Context, routingKey string, msg mq.Message, opts ...mq.Option) error {
	if b.cfg.PublishMode == PublishModeStreams {
		return b.publishStream(ctx, routingKey, msg, opts...)
	}

	conn, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(conn) // nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer ch.Close() // nolint:errcheck

	var mandatory bool
	var persistent bool
	var expiration time.Duration
	var delay time.Duration
	var headers map[string]string
	var replyTo string
	var correlationID string

	for _, opt := range opts {
		switch o := opt.(type) {
		case MandatoryOption:
			mandatory = o.Value
		case PersistentOption:
			persistent = o.Value
		case ExpirationOption:
			expiration = o.Value
		case DelayOption:
			delay = o.Value
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

	headerCount := len(msg.Headers)
	if headers != nil {
		headerCount += len(headers)
	}
	publishing := amqp.Publishing{
		Body:        msg.Body,
		Timestamp:   msg.Timestamp,
		ContentType: msg.ContentType,
		AppId:       "go-mq",
		Headers:     make(amqp.Table, headerCount),
	}

	if msg.Key != "" {
		if publishing.Headers == nil {
			publishing.Headers = amqp.Table{}
		}
		publishing.Headers["key"] = msg.Key
	}

	for k, v := range msg.Headers {
		publishing.Headers[k] = v
	}
	for k, v := range headers {
		publishing.Headers[k] = v
	}

	// Set persistence based on PublishMode or explicit PersistentOption
	shouldBePersistent := persistent
	if !shouldBePersistent {
		// Check if mode requires persistence
		shouldBePersistent = b.cfg.PublishMode == PublishModePersistentPubSub ||
			b.cfg.PublishMode == PublishModePersistentPushPull
	}
	if shouldBePersistent {
		publishing.DeliveryMode = amqp.Persistent
	}
	if expiration > 0 {
		publishing.Expiration = fmt.Sprintf("%d", expiration.Milliseconds())
	}
	if replyTo != "" {
		publishing.ReplyTo = replyTo
	}
	if correlationID != "" {
		publishing.CorrelationId = correlationID
	}
	if delay > 0 {
		// Delay support requires the delayed message exchange plugin
		// The x-delay header is used by the plugin to delay message delivery
		if publishing.Headers == nil {
			publishing.Headers = amqp.Table{}
		}
		publishing.Headers["x-delay"] = delay.Milliseconds()
	}

	if err := ch.PublishWithContext(
		ctx,
		b.cfg.Exchange,
		routingKey,
		mandatory,
		false,
		publishing,
	); err != nil {
		return errors.Join(mq.ErrPublishFailed, err)
	}
	return nil
}

// Consume creates a consumer for the desired queue/binding.
func (b *Broker) Consume(ctx context.Context, topic string, queue string, consumerName string, opts ...mq.Option) (mq.Consumer, error) {
	if b.cfg.PublishMode == PublishModeStreams {
		return b.consumeStream(ctx, topic, queue, consumerName, opts...)
	}

	conn, err := b.pool.Get(ctx)
	if err != nil {
		return nil, errors.Join(mq.ErrNoConnection, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = b.pool.Release(conn)
		return nil, errors.Join(mq.ErrNoConnection, err)
	}

	var autoAck bool
	var prefetch int
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

	if prefetch <= 0 {
		prefetch = b.cfg.Prefetch
	}
	if prefetch > 0 {
		if err := ch.Qos(prefetch, 0, false); err != nil {
			ch.Close()           // nolint:errcheck
			b.pool.Release(conn) // nolint:errcheck
			return nil, errors.Join(mq.ErrConsumeFailed, err)
		}
	}

	// Determine queue properties based on PublishMode
	isPubSub := b.cfg.PublishMode == PublishModePubSub || b.cfg.PublishMode == PublishModePersistentPubSub
	isPersistent := b.cfg.PublishMode == PublishModePersistentPubSub || b.cfg.PublishMode == PublishModePersistentPushPull

	queueName := queue
	if queueName == "" {
		if isPubSub {
			// For pub-sub, generate a unique queue name per consumer
			queueName = fmt.Sprintf("pubsub-%s-%d", topic, time.Now().UnixNano())
		} else {
			queueName = topic
		}
	}
	if queueName == "" {
		ch.Close()           // nolint:errcheck
		b.pool.Release(conn) // nolint:errcheck
		return nil, errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: queue or topic must be provided"))
	}

	queueArgs := amqp.Table{}
	if deadLetterTopic != "" {
		queueArgs["x-dead-letter-exchange"] = deadLetterTopic
	}

	// For pub-sub modes: auto-delete queues (temporary for non-persistent, durable for persistent)
	// For push-pull modes: regular queues with durability based on mode
	queueDurable := isPersistent
	queueAutoDelete := isPubSub && !isPersistent
	queueExclusive := isPubSub && !isPersistent

	_, err = ch.QueueDeclare(
		queueName,
		queueDurable,
		queueAutoDelete,
		queueExclusive,
		false,
		queueArgs,
	)
	if err != nil {
		ch.Close()           // nolint:errcheck
		b.pool.Release(conn) // nolint:errcheck
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	// For pub-sub modes with fanout exchange, routing key is ignored
	// For push-pull modes, bind with the topic as routing key
	if b.cfg.Exchange != "" {
		routingKey := topic
		if isPubSub && b.cfg.ExchangeType == "fanout" {
			routingKey = "" // Fanout exchanges ignore routing keys
		}
		if err := ch.QueueBind(queueName, routingKey, b.cfg.Exchange, false, nil); err != nil {
			ch.Close()           // nolint:errcheck
			b.pool.Release(conn) // nolint:errcheck
			return nil, errors.Join(mq.ErrConsumeFailed, err)
		}
	}

	consumerTag := consumerName
	if consumerTag == "" {
		consumerTag = generateConsumerTag()
	}

	deliveries, err := ch.Consume(
		queueName,
		consumerTag,
		autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()           // nolint:errcheck
		b.pool.Release(conn) // nolint:errcheck
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	return newConsumer(conn, ch, deliveries, b.pool, queueName, consumerTag), nil
}

// CreateQueue declares a queue and optionally binds it to the configured exchange.
func (b *Broker) CreateQueue(ctx context.Context, topic, queue string) error {
	if b.cfg.PublishMode == PublishModeStreams {
		return b.createStream(topic)
	}
	conn, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(conn) // nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer ch.Close() // nolint:errcheck

	if b.cfg.PublishMode == PublishModePubSub {
		return errors.Join(mq.ErrNotSupported, fmt.Errorf("rabbitmq: CreateQueue not supported in non-persistent pub-sub mode"))
	}

	// Determine queue properties based on PublishMode
	isPersistent := b.cfg.PublishMode == PublishModePersistentPubSub || b.cfg.PublishMode == PublishModePersistentPushPull
	queueDurable := isPersistent
	if b.cfg.QueueDurable {
		// Explicit QueueDurable config overrides PublishMode
		queueDurable = true
	}

	_, err = ch.QueueDeclare(
		queue,
		queueDurable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}

	if b.cfg.Exchange != "" && topic != "" {
		routingKey := topic
		isPubSub := b.cfg.PublishMode == PublishModePubSub || b.cfg.PublishMode == PublishModePersistentPubSub
		if isPubSub && b.cfg.ExchangeType == "fanout" {
			routingKey = "" // Fanout exchanges ignore routing keys
		}
		if err := ch.QueueBind(queue, routingKey, b.cfg.Exchange, false, nil); err != nil {
			return errors.Join(mq.ErrConfiguration, err)
		}
	}

	return nil
}

// DeleteQueue removes a queue and its bindings.
func (b *Broker) DeleteQueue(ctx context.Context, _, queue string) error {
	if b.cfg.PublishMode == PublishModeStreams {
		return nil
	}
	conn, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(conn) // nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer ch.Close() // nolint:errcheck

	if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}
	return nil
}

// Flush ensures all pending operations are completed by flushing the connection pool.
func (b *Broker) Flush(ctx context.Context) error {
	if b.cfg.PublishMode == PublishModeStreams {
		return nil
	}
	conn, err := b.pool.Get(ctx)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer b.pool.Release(conn) // nolint:errcheck

	ch, err := conn.Channel()
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	defer ch.Close() // nolint:errcheck

	// AMQP channels don't have a direct flush, but we can ensure the channel is ready
	// by checking if we can declare a temporary queue and delete it
	tempQueue := fmt.Sprintf("__flush__%d", time.Now().UnixNano())
	_, err = ch.QueueDeclare(tempQueue, false, true, true, false, nil)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	_, err = ch.QueueDelete(tempQueue, false, false, false)
	if err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	return nil
}

// Close releases all resources.
func (b *Broker) Close(ctx context.Context) error {
	var retErr error
	if b.pool != nil {
		if err := b.pool.Close(); err != nil {
			retErr = errors.Join(mq.ErrNoConnection, err)
		}
	}
	b.streamProducers.Range(func(key, value any) bool {
		if producer, ok := value.(*stream.SuperStreamProducer); ok {
			_ = producer.Close()
		}
		return true
	})
	if b.streamEnv != nil {
		if err := b.streamEnv.Close(); err != nil && retErr == nil {
			retErr = errors.Join(mq.ErrNoConnection, err)
		}
	}
	return retErr
}

func generateConsumerTag() string {
	return fmt.Sprintf("go-mq-%d", time.Now().UnixNano())
}
func buildAMQPURI(cfg mq.Config, address string) (string, error) {
	scheme := "amqp"
	if cfg.UseTLS || cfg.TLSConfig != nil {
		scheme = "amqps"
	}

	var userInfo *url.Userinfo
	if cfg.Username != "" {
		userInfo = url.UserPassword(cfg.Username, cfg.Password)
	}

	vhost := cfg.VirtualHost
	if vhost == "" {
		vhost = "/"
	}
	if !strings.HasPrefix(vhost, "/") {
		vhost = "/" + vhost
	}

	u := url.URL{
		Scheme: scheme,
		Host:   address,
		Path:   vhost,
	}

	if userInfo != nil {
		u.User = userInfo
	}

	return u.String(), nil
}

func buildAMQPConfig(cfg Config) (*amqp.Config, error) {
	amqpCfg := amqp.Config{
		Vhost:      cfg.Connection.VirtualHost,
		Properties: amqp.Table{"product": "go-mq"},
		Dial: func(network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: cfg.DialTimeout}
			return d.Dial(network, addr)
		},
	}

	tlsCfg, err := mq.BuildTLSConfig(cfg.Connection)
	if err != nil {
		return nil, err
	}
	amqpCfg.TLSClientConfig = tlsCfg

	return &amqpCfg, nil
}
