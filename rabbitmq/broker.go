package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/hadi77ir/go-mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Broker is a RabbitMQ implementation of the mq.Broker interface.
type Broker struct {
	cfg  Config
	pool *mq.ConnectionPool[*amqp.Connection]
}

// NewBroker creates a Broker backed by a connection pool.
func NewBroker(ctx context.Context, cfg Config) (*Broker, error) {
	cfg = cfg.normalized()
	if err := cfg.validate(); err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

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

	if cfg.DeclareExchange && cfg.Exchange != "" {
		if err := b.ensureExchange(ctx); err != nil {
			_ = pool.Close()
			return nil, err
		}
	}

	return b, nil
}

func (b *Broker) ensureExchange(ctx context.Context) error {
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

	if persistent {
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

	queueName := queue
	if queueName == "" {
		queueName = topic
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

	_, err = ch.QueueDeclare(
		queueName,
		b.cfg.QueueDurable,
		false,
		false,
		false,
		queueArgs,
	)
	if err != nil {
		ch.Close()           // nolint:errcheck
		b.pool.Release(conn) // nolint:errcheck
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}

	if b.cfg.Exchange != "" && topic != "" {
		if err := ch.QueueBind(queueName, topic, b.cfg.Exchange, false, nil); err != nil {
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

	_, err = ch.QueueDeclare(
		queue,
		b.cfg.QueueDurable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.Join(mq.ErrConfiguration, err)
	}

	if b.cfg.Exchange != "" && topic != "" {
		if err := ch.QueueBind(queue, topic, b.cfg.Exchange, false, nil); err != nil {
			return errors.Join(mq.ErrConfiguration, err)
		}
	}

	return nil
}

// DeleteQueue removes a queue and its bindings.
func (b *Broker) DeleteQueue(ctx context.Context, _, queue string) error {
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

// Close releases all resources.
func (b *Broker) Close(ctx context.Context) error {
	if err := b.pool.Close(); err != nil {
		return errors.Join(mq.ErrNoConnection, err)
	}
	return nil
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
