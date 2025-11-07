package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hadi77ir/go-mq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestBrokerPublishConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "test.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        10,
		QueueDurable:    false,
		PublishMode:     PublishModePersistentPushPull,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "test.route", "test-queue"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	msg := mq.Message{
		Key:         "k1",
		Body:        []byte("hello world"),
		Headers:     map[string]string{"foo": "bar"},
		ContentType: "text/plain",
		Timestamp:   time.Now(),
	}

	if err := broker.Publish(ctx, "test.route", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := broker.Consume(ctx, "test.route", "test-queue", "")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	delivery, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	if string(delivery.Message.Body) != "hello world" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}
	if delivery.Metadata.Queue != "test-queue" {
		t.Fatalf("unexpected queue: %s", delivery.Metadata.Queue)
	}

	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	secondCtx, secondCancel := context.WithTimeout(ctx, time.Second)
	defer secondCancel()

	_, err = consumer.Receive(secondCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error after ack, got %v", err)
	}

	if err := broker.DeleteQueue(ctx, "", "test-queue"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
}

func TestPublishModePubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "pubsub.exchange",
		ExchangeType:    "fanout",
		DeclareExchange: true,
		Prefetch:        10,
		PublishMode:     PublishModePubSub,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	// Create two consumers first (for pub-sub, consumers must be created before publishing)
	consumer1, err := broker.Consume(ctx, "test.topic", "", "consumer1")
	if err != nil {
		t.Fatalf("Consume consumer1: %v", err)
	}
	defer consumer1.Close() // nolint:errcheck

	consumer2, err := broker.Consume(ctx, "test.topic", "", "consumer2")
	if err != nil {
		t.Fatalf("Consume consumer2: %v", err)
	}
	defer consumer2.Close() // nolint:errcheck

	// Give consumers time to bind
	time.Sleep(100 * time.Millisecond)

	msg := mq.Message{
		Body: []byte("pubsub message"),
	}

	// Publish a message
	if err := broker.Publish(ctx, "test.topic", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	// Both consumers should receive the message
	delivery1, err := consumer1.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive consumer1: %v", err)
	}
	if string(delivery1.Message.Body) != "pubsub message" {
		t.Fatalf("consumer1 unexpected body: %s", delivery1.Message.Body)
	}

	delivery2, err := consumer2.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive consumer2: %v", err)
	}
	if string(delivery2.Message.Body) != "pubsub message" {
		t.Fatalf("consumer2 unexpected body: %s", delivery2.Message.Body)
	}
}

func TestPublishModePersistentPubSub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "persistent.pubsub.exchange",
		ExchangeType:    "fanout",
		DeclareExchange: true,
		Prefetch:        10,
		PublishMode:     PublishModePersistentPubSub,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	// Create consumer first
	consumer, err := broker.Consume(ctx, "test.topic", "", "persistent-consumer")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	// Give consumer time to bind
	time.Sleep(100 * time.Millisecond)

	msg := mq.Message{
		Body: []byte("persistent pubsub message"),
	}

	// Publish a message
	if err := broker.Publish(ctx, "test.topic", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	delivery, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(delivery.Message.Body) != "persistent pubsub message" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}

	// Test that acks work
	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestPublishModeStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	streamPort, err := container.MappedPort(ctx, "5552/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort stream: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "streams.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        5,
		PublishMode:     PublishModeStreams,
		Stream: StreamConfig{
			Addresses:             []string{host + ":" + streamPort.Port()},
			Partitions:            3,
			MaxLengthBytes:        1 << 20,
			MaxAge:                5 * time.Minute,
			MaxProducersPerClient: 5,
			MaxConsumersPerClient: 5,
		},
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	queue := "streams-queue"
	if err := broker.CreateQueue(ctx, "streams.route", queue); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	consumer1, err := broker.Consume(ctx, "streams.route", queue, "stream-consumer1")
	if err != nil {
		t.Fatalf("Consume consumer1: %v", err)
	}
	defer consumer1.Close() // nolint:errcheck

	consumer2, err := broker.Consume(ctx, "streams.route", queue, "stream-consumer2")
	if err != nil {
		t.Fatalf("Consume consumer2: %v", err)
	}
	defer consumer2.Close() // nolint:errcheck

	time.Sleep(250 * time.Millisecond)

	for i := 0; i < 4; i++ {
		msg := mq.Message{Body: []byte(fmt.Sprintf("stream-msg-%d", i))}
		if err := broker.Publish(ctx, "streams.route", msg); err != nil {
			t.Fatalf("Publish stream-msg-%d: %v", i, err)
		}
	}

	if err := broker.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	received := map[string]bool{}
	for len(received) < 4 {
		select {
		case <-receiveCtx.Done():
			t.Fatalf("timeout waiting for messages, received %d/4", len(received))
		default:
		}

		for _, cons := range []mq.Consumer{consumer1, consumer2} {
			delivery, err := cons.Receive(receiveCtx)
			if err != nil {
				continue
			}
			body := string(delivery.Message.Body)
			if received[body] {
				t.Fatalf("duplicate message: %s", body)
			}
			received[body] = true
			if err := delivery.Ack(ctx); err != nil {
				t.Fatalf("Ack %s: %v", body, err)
			}
			if len(received) == 4 {
				break
			}
		}
	}
}

func TestPublishModePushPull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "pushpull.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        10,
		PublishMode:     PublishModePushPull,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "test.route", "pushpull-queue"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// Publish two messages
	msg1 := mq.Message{Body: []byte("message1")}
	msg2 := mq.Message{Body: []byte("message2")}

	if err := broker.Publish(ctx, "test.route", msg1); err != nil {
		t.Fatalf("Publish msg1: %v", err)
	}
	if err := broker.Publish(ctx, "test.route", msg2); err != nil {
		t.Fatalf("Publish msg2: %v", err)
	}

	// Create consumer with specific queue
	consumer, err := broker.Consume(ctx, "test.route", "pushpull-queue", "")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	// Should receive messages in order (work queue pattern)
	delivery1, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive msg1: %v", err)
	}
	if string(delivery1.Message.Body) != "message1" {
		t.Fatalf("unexpected body: %s", delivery1.Message.Body)
	}

	delivery2, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive msg2: %v", err)
	}
	if string(delivery2.Message.Body) != "message2" {
		t.Fatalf("unexpected body: %s", delivery2.Message.Body)
	}

	// Test acks
	if err := delivery1.Ack(ctx); err != nil {
		t.Fatalf("Ack msg1: %v", err)
	}
	if err := delivery2.Ack(ctx); err != nil {
		t.Fatalf("Ack msg2: %v", err)
	}
}

func TestPublishModePersistentPushPull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "persistent.pushpull.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        10,
		PublishMode:     PublishModePersistentPushPull,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "test.route", "persistent-queue"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	msg := mq.Message{
		Body: []byte("persistent pushpull message"),
	}

	if err := broker.Publish(ctx, "test.route", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := broker.Consume(ctx, "test.route", "persistent-queue", "")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	delivery, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(delivery.Message.Body) != "persistent pushpull message" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}

	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestConsistencyAutoAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "test.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        10,
		QueueDurable:    false,
		PublishMode:     PublishModePersistentPushPull,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "autoack.route", "autoack-queue"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// Test AutoAck=true
	consumer, err := broker.Consume(ctx, "autoack.route", "autoack-queue", "", mq.WithAutoAck(true))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	if err := broker.Publish(ctx, "autoack.route", mq.Message{Body: []byte("test1")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	delivery, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(delivery.Message.Body) != "test1" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}

	// With AutoAck, message should already be acked
	if err := broker.Publish(ctx, "autoack.route", mq.Message{Body: []byte("test2")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	delivery2, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive second: %v", err)
	}
	if string(delivery2.Message.Body) != "test2" {
		t.Fatalf("unexpected body: %s", delivery2.Message.Body)
	}
}

func TestConsistencyPublishOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := startRabbitMQ(ctx)
	if err != nil {
		t.Fatalf("startRabbitMQ: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{host + ":" + port.Port()},
			Username:  "guest",
			Password:  "guest",
		},
		MaxConnections:  3,
		Exchange:        "test.exchange",
		ExchangeType:    "direct",
		DeclareExchange: true,
		Prefetch:        10,
		QueueDurable:    false,
		PublishMode:     PublishModePersistentPushPull,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "opts.route", "opts-queue"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// Test PublishOptions
	msg := mq.Message{
		Body:        []byte("test-opts"),
		Headers:     map[string]string{"msg-header": "msg-value"},
		ContentType: "application/json",
	}

	if err := broker.Publish(ctx, "opts.route", msg,
		mq.WithHeaders(map[string]string{"custom-header": "value"}),
		mq.WithReplyTo("reply.queue"),
		mq.WithCorrelationID("corr-123"),
		WithPersistent(true),
		WithExpiration(5*time.Second),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	consumer, err := broker.Consume(ctx, "opts.route", "opts-queue", "")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 30*time.Second)
	defer receiveCancel()

	delivery, err := consumer.Receive(receiveCtx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	if string(delivery.Message.Body) != "test-opts" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}
	// Headers should be merged
	if delivery.Message.Headers["custom-header"] != "value" {
		t.Fatalf("missing custom-header")
	}
	if delivery.Message.Headers["msg-header"] != "msg-value" {
		t.Fatalf("missing msg-header")
	}
}

func writeStreamPluginFile() (string, error) {
	content := `[rabbitmq_federation,rabbitmq_management,rabbitmq_management_agent,rabbitmq_prometheus,rabbitmq_stream,rabbitmq_web_dispatch].` + "\n"
	tmpFile, err := os.CreateTemp("", "enabled_plugins*.erl")
	if err != nil {
		return "", err
	}
	if _, err := tmpFile.WriteString(content); err != nil {
		tmpFile.Close()
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	return tmpFile.Name(), nil
}

func startRabbitMQ(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.13-management",
		ExposedPorts: []string{"5672/tcp", "5552/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete"),
	}

	pluginFile, err := writeStreamPluginFile()
	if err != nil {
		return nil, err
	}
	defer os.Remove(pluginFile)
	req.Files = []testcontainers.ContainerFile{
		{
			HostFilePath:      pluginFile,
			ContainerFilePath: "/etc/rabbitmq/enabled_plugins",
			FileMode:          0o644,
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	return container, nil
}
