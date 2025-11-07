package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hadi77ir/go-mq"
	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	streammessage "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const streamKeyHeader = "__mq_key"

var streamMessageCounter uint64

func (b *Broker) publishStream(ctx context.Context, streamName string, msg mq.Message, opts ...mq.Option) error {
	if streamName == "" {
		return errors.Join(mq.ErrPublishFailed, fmt.Errorf("rabbitmq: stream topic required"))
	}
	if b.streamEnv == nil {
		return errors.Join(mq.ErrPublishFailed, fmt.Errorf("rabbitmq: streaming environment not configured"))
	}

	if err := b.createStream(streamName); err != nil {
		return errors.Join(mq.ErrPublishFailed, err)
	}

	streamMsg, err := buildStreamMessage(streamName, msg, opts...)
	if err != nil {
		return errors.Join(mq.ErrPublishFailed, err)
	}

	producer, err := b.getOrCreateStreamProducer(streamName)
	if err != nil {
		return errors.Join(mq.ErrPublishFailed, err)
	}

	if err := producer.Send(streamMsg); err != nil {
		b.streamProducers.Delete(streamName)
		return errors.Join(mq.ErrPublishFailed, err)
	}
	return nil
}

func buildStreamMessage(streamName string, msg mq.Message, opts ...mq.Option) (streammessage.StreamMessage, error) {
	headers := make(map[string]string)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	var replyTo string
	var correlationID string

	for _, opt := range opts {
		switch o := opt.(type) {
		case mq.HeadersOption:
			for k, v := range o.Value {
				headers[k] = v
			}
		case mq.ReplyToOption:
			replyTo = o.Value
		case mq.CorrelationIDOption:
			correlationID = o.Value
		}
	}

	streamMsg := streamamqp.NewMessage(msg.Body)
	if len(headers) > 0 || msg.Key != "" {
		streamMsg.ApplicationProperties = make(map[string]any, len(headers)+1)
		for k, v := range headers {
			streamMsg.ApplicationProperties[k] = v
		}
		if msg.Key != "" {
			streamMsg.ApplicationProperties[streamKeyHeader] = msg.Key
		}
	}

	props := &streamamqp.MessageProperties{
		ContentType:   msg.ContentType,
		ReplyTo:       replyTo,
		CorrelationID: correlationID,
		CreationTime:  msg.Timestamp,
	}
	partitionKey := msg.Key
	if partitionKey == "" {
		partitionKey = fmt.Sprintf("%s-%d", streamName, atomic.AddUint64(&streamMessageCounter, 1))
	}
	props.MessageID = partitionKey
	streamMsg.Properties = props

	return streamMsg, nil
}

func (b *Broker) getOrCreateStreamProducer(streamName string) (*stream.SuperStreamProducer, error) {
	if producer, ok := b.streamProducers.Load(streamName); ok {
		return producer.(*stream.SuperStreamProducer), nil
	}

	if b.streamEnv == nil {
		return nil, fmt.Errorf("rabbitmq: streaming environment not configured")
	}

	routing := stream.NewHashRoutingStrategy(func(m streammessage.StreamMessage) string {
		if props := m.GetMessageProperties(); props != nil {
			if id, ok := props.MessageID.(string); ok && id != "" {
				return id
			}
		}
		return streamName
	})

	options := stream.NewSuperStreamProducerOptions(routing).
		SetClientProvidedName("go-mq-stream-producer")

	producer, err := b.streamEnv.NewSuperStreamProducer(streamName, options)
	if err != nil {
		return nil, err
	}

	actual, loaded := b.streamProducers.LoadOrStore(streamName, producer)
	if loaded {
		_ = producer.Close()
		return actual.(*stream.SuperStreamProducer), nil
	}
	return producer, nil
}

func (b *Broker) consumeStream(ctx context.Context, topic string, queue string, consumerName string, opts ...mq.Option) (mq.Consumer, error) {
	if topic == "" {
		return nil, errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: stream topic required"))
	}
	if queue == "" {
		return nil, errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: queue required for streams mode"))
	}
	if b.streamEnv == nil {
		return nil, errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: streaming environment not configured"))
	}
	if err := b.createStream(topic); err != nil {
		return nil, errors.Join(mq.ErrConfiguration, err)
	}

	var autoAck bool
	var deadLetter string
	var prefetch int

	for _, opt := range opts {
		switch o := opt.(type) {
		case mq.AutoAckOption:
			autoAck = o.Value
		case mq.DeadLetterTopicOption:
			deadLetter = o.Value
		case mq.PrefetchOption:
			prefetch = o.Value
		}
	}

	if prefetch <= 0 {
		prefetch = b.cfg.Prefetch
	}
	if prefetch <= 0 {
		prefetch = 32
	}
	if prefetch > math.MaxInt16 {
		prefetch = math.MaxInt16
	}

	bufferSize := prefetch * 2
	if bufferSize < 64 {
		bufferSize = 64
	}

	consumer := &streamConsumerWrapper{
		broker:          b,
		topic:           topic,
		queue:           queue,
		consumerName:    consumerName,
		deadLetterTopic: deadLetter,
		autoAck:         autoAck,
		deliveries:      make(chan *mq.Delivery, bufferSize),
		closed:          make(chan struct{}),
	}

	handler := consumer.messageHandler()

	options := stream.NewSuperStreamConsumerOptions().
		SetConsumerName(queue).
		SetManualCommit().
		SetOffset(stream.OffsetSpecification{}.Next())

	if consumerName != "" {
		options = options.SetClientProvidedName(consumerName)
	}

	sac := stream.NewSingleActiveConsumer(func(partition string, isActive bool) stream.OffsetSpecification {
		if !isActive {
			return stream.OffsetSpecification{}.Next()
		}
		offset, err := b.streamEnv.QueryOffset(queue, partition)
		if err != nil {
			return stream.OffsetSpecification{}.Next()
		}
		return stream.OffsetSpecification{}.Offset(offset + 1)
	})
	options = options.SetSingleActiveConsumer(sac)

	superConsumer, err := b.streamEnv.NewSuperStreamConsumer(topic, handler, options)
	if err != nil {
		close(consumer.closed)
		return nil, errors.Join(mq.ErrConsumeFailed, err)
	}
	consumer.superConsumer = superConsumer

	return consumer, nil
}

type streamConsumerWrapper struct {
	broker          *Broker
	topic           string
	queue           string
	consumerName    string
	deadLetterTopic string
	autoAck         bool

	superConsumer *stream.SuperStreamConsumer
	deliveries    chan *mq.Delivery
	closed        chan struct{}
	closeOnce     sync.Once
}

func (c *streamConsumerWrapper) messageHandler() stream.MessagesHandler {
	return func(consCtx stream.ConsumerContext, raw *streamamqp.Message) {
		delivery := c.buildDelivery(consCtx, raw)
		if delivery == nil {
			return
		}

		if c.autoAck {
			_ = delivery.Ack(context.Background())
		}

		select {
		case c.deliveries <- delivery:
		case <-c.closed:
		}
	}
}

func (c *streamConsumerWrapper) buildDelivery(consCtx stream.ConsumerContext, raw *streamamqp.Message) *mq.Delivery {
	msg := convertStreamMessage(raw)

	meta := mq.DeliveryMetadata{
		ID:           fmt.Sprintf("%d", consCtx.Consumer.GetOffset()),
		Topic:        c.topic,
		Queue:        c.queue,
		ConsumerName: c.consumerName,
	}

	ack := func(ctx context.Context) error {
		if err := consCtx.Consumer.StoreOffset(); err != nil {
			return errors.Join(mq.ErrAcknowledgeFailed, err)
		}
		return nil
	}

	nack := func(ctx context.Context, requeue bool) error {
		if err := consCtx.Consumer.StoreOffset(); err != nil {
			return errors.Join(mq.ErrAcknowledgeFailed, err)
		}

		target := c.deadLetterTopic
		if requeue || target == "" {
			target = c.topic
		}
		return c.broker.publishStream(ctx, target, msg)
	}

	return mq.NewDelivery(msg, meta, ack, nack)
}

func (c *streamConsumerWrapper) Receive(ctx context.Context) (*mq.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Join(mq.ErrConsumeFailed, ctx.Err())
	case <-c.closed:
		return nil, errors.Join(mq.ErrConsumeFailed, errors.New("rabbitmq: stream consumer closed"))
	case delivery, ok := <-c.deliveries:
		if !ok {
			return nil, errors.Join(mq.ErrConsumeFailed, errors.New("rabbitmq: stream consumer closed"))
		}
		return delivery, nil
	}
}

func (c *streamConsumerWrapper) Close() error {
	var retErr error
	c.closeOnce.Do(func() {
		close(c.closed)
		close(c.deliveries)
		if c.superConsumer != nil {
			retErr = c.superConsumer.Close()
		}
	})
	return retErr
}

func (b *Broker) createStream(topic string) error {
	if topic == "" {
		return errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: stream topic required"))
	}
	if b.streamEnv == nil {
		return errors.Join(mq.ErrConfiguration, fmt.Errorf("rabbitmq: streaming environment not configured"))
	}

	if _, loaded := b.streamDeclared.Load(topic); loaded {
		return nil
	}

	partitions := b.cfg.Stream.Partitions
	if partitions <= 0 {
		partitions = 1
	}

	options := stream.NewPartitionsOptions(partitions)
	if b.cfg.Stream.MaxLengthBytes > 0 {
		options.SetMaxLengthBytes(stream.ByteCapacity{}.B(b.cfg.Stream.MaxLengthBytes))
	}
	if b.cfg.Stream.MaxSegmentSizeBytes > 0 {
		options.SetMaxSegmentSizeBytes(stream.ByteCapacity{}.B(b.cfg.Stream.MaxSegmentSizeBytes))
	}
	if b.cfg.Stream.MaxAge > 0 {
		options.SetMaxAge(b.cfg.Stream.MaxAge)
	}

	if err := b.streamEnv.DeclareSuperStream(topic, options); err != nil && !errors.Is(err, stream.StreamAlreadyExists) {
		return errors.Join(mq.ErrConfiguration, err)
	}
	b.streamDeclared.Store(topic, struct{}{})
	return nil
}

func convertStreamMessage(raw *streamamqp.Message) mq.Message {
	var body []byte
	if len(raw.Data) > 0 && raw.Data[0] != nil {
		body = append([]byte(nil), raw.Data[0]...)
	}

	headers := make(map[string]string, len(raw.ApplicationProperties))
	var key string
	for k, v := range raw.ApplicationProperties {
		if k == streamKeyHeader {
			key = fmt.Sprint(v)
			continue
		}
		headers[k] = fmt.Sprint(v)
	}

	var contentType string
	var timestamp time.Time
	if raw.Properties != nil {
		contentType = raw.Properties.ContentType
		timestamp = raw.Properties.CreationTime
	}

	return mq.Message{
		Key:         key,
		Body:        body,
		Headers:     headers,
		ContentType: contentType,
		Timestamp:   timestamp,
	}
}

func newStreamEnvironment(cfg Config) (*stream.Environment, error) {
	options := stream.NewEnvironmentOptions().
		SetMaxProducersPerClient(cfg.Stream.MaxProducersPerClient).
		SetMaxConsumersPerClient(cfg.Stream.MaxConsumersPerClient)

	addresses := cfg.Stream.Addresses
	if len(addresses) == 0 {
		addresses = cfg.Connection.Addresses
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("rabbitmq: no stream addresses configured")
	}

	uris := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		uri, err := buildStreamURI(cfg.Connection, addr)
		if err != nil {
			return nil, err
		}
		uris = append(uris, uri)
	}
	options.SetUris(uris)

	if host, port, err := parseStreamAddress(addresses[0]); err == nil {
		options.SetAddressResolver(stream.AddressResolver{Host: host, Port: port})
	}

	tlsCfg, err := mq.BuildTLSConfig(cfg.Connection)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		options.SetTLSConfig(tlsCfg)
		options.IsTLS(true)
	}

	return stream.NewEnvironment(options)
}

func buildStreamURI(cfg mq.Config, address string) (string, error) {
	if strings.Contains(address, "://") {
		return address, nil
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			host = address
			port = stream.StreamTcpPort
		} else {
			return "", err
		}
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = stream.StreamTcpPort
	}

	scheme := "rabbitmq-stream"
	if cfg.UseTLS || cfg.TLSConfig != nil {
		scheme = "rabbitmq-stream+tls"
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
		Host:   net.JoinHostPort(host, port),
		Path:   vhost,
	}

	if cfg.Username != "" {
		u.User = url.UserPassword(cfg.Username, cfg.Password)
	}

	return u.String(), nil
}

func parseStreamAddress(address string) (string, int, error) {
	if strings.Contains(address, "://") {
		u, err := url.Parse(address)
		if err != nil {
			return "", 0, err
		}
		host, portStr, err := net.SplitHostPort(u.Host)
		if err != nil {
			return "", 0, err
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return "", 0, err
		}
		return host, port, nil
	}
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}
