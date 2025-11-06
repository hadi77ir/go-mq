package mqnats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/hadi77ir/go-mq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestJetStreamPublishConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := startNATS(ctx)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, port := containerAddress(t, ctx, container, "4222/tcp")

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
		},
		PublishMode:   PublishModeJetStream,
		StreamName:    "orders",
		SubjectPrefix: "orders",
		PullBatch:     1,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "created", "workers"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	consumer, err := broker.Consume(ctx, "created", "workers", "", mq.WithDeadLetterTopic("dead"))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	msg := mq.Message{
		Body:        []byte("hello"),
		ContentType: "text/plain",
		Headers:     map[string]string{"x-trace": "1"},
		Timestamp:   time.Now(),
	}

	if err := broker.Publish(ctx, "created", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	delivery, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(delivery.Message.Body) != "hello" {
		t.Fatalf("unexpected body %s", delivery.Message.Body)
	}
	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if err := broker.Publish(ctx, "created", mq.Message{Body: []byte("retry")}); err != nil {
		t.Fatalf("Publish retry: %v", err)
	}

	delivery, err = consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive retry: %v", err)
	}
	if err := delivery.Nack(ctx, true); err != nil {
		t.Fatalf("Nack(true): %v", err)
	}

	delivery, err = consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive after nack: %v", err)
	}
	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack after nack: %v", err)
	}

	if err := broker.Publish(ctx, "created", mq.Message{Body: []byte("dead")}); err != nil {
		t.Fatalf("Publish dead: %v", err)
	}

	delivery, err = consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive dead candidate: %v", err)
	}
	if err := delivery.Nack(ctx, false); err != nil {
		t.Fatalf("Nack(false): %v", err)
	}

	dlConsumer, err := broker.Consume(ctx, "dead", "dlq", "", WithStartFromOldest(true))
	if err != nil {
		t.Fatalf("Consume dead-letter: %v", err)
	}
	defer dlConsumer.Close() // nolint:errcheck

	dlCtx, dlCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlCancel()
	dlDelivery, err := dlConsumer.Receive(dlCtx)
	if err != nil {
		t.Fatalf("Receive dead-letter: %v", err)
	}
	if string(dlDelivery.Message.Body) != "dead" {
		t.Fatalf("unexpected dead-letter body %s", dlDelivery.Message.Body)
	}
	if err := dlDelivery.Ack(ctx); err != nil {
		t.Fatalf("Ack dead-letter: %v", err)
	}

	if err := broker.DeleteQueue(ctx, "created", "workers"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
}

func TestCorePublishConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := startNATS(ctx)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, port := containerAddress(t, ctx, container, "4222/tcp")

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
		},
		PublishMode:   PublishModeCore,
		SubjectPrefix: "core",
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	consumer, err := broker.Consume(ctx, "updates", "workers", "")
	if err != nil {
		t.Fatalf("Consume core: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	if err := broker.Publish(ctx, "updates", mq.Message{Body: []byte("core-msg")}); err != nil {
		t.Fatalf("Publish core: %v", err)
	}

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	delivery, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive core: %v", err)
	}
	if string(delivery.Message.Body) != "core-msg" {
		t.Fatalf("unexpected core payload %s", delivery.Message.Body)
	}
}

func startNATS(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "docker.io/nats:2.10-alpine",
		ExposedPorts: []string{"4222/tcp"},
		Cmd:          []string{"-js"},
		WaitingFor:   wait.ForLog("Server is ready"),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func TestConsistencyAutoAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := startNATS(ctx)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, port := containerAddress(t, ctx, container, "4222/tcp")

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
		},
		PublishMode:   PublishModeJetStream,
		StreamName:    "autoadk",
		SubjectPrefix: "test",
		PullBatch:     1,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	if err := broker.CreateQueue(ctx, "autoadk", "workers"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	// Test AutoAck=true
	consumer, err := broker.Consume(ctx, "autoadk", "workers", "", mq.WithAutoAck(true))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	if err := broker.Publish(ctx, "autoadk", mq.Message{Body: []byte("test1")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	delivery, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if string(delivery.Message.Body) != "test1" {
		t.Fatalf("unexpected body: %s", delivery.Message.Body)
	}

	// With AutoAck, message should already be acked
	if err := broker.Publish(ctx, "autoadk", mq.Message{Body: []byte("test2")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	delivery2, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive second: %v", err)
	}
	if string(delivery2.Message.Body) != "test2" {
		t.Fatalf("unexpected body: %s", delivery2.Message.Body)
	}
}

func TestConsistencyStartFromOldest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := startNATS(ctx)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, port := containerAddress(t, ctx, container, "4222/tcp")

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
		},
		PublishMode:   PublishModeJetStream,
		StreamName:    "oldest",
		SubjectPrefix: "test",
		PullBatch:     1,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	// Publish messages before creating consumer
	if err := broker.Publish(ctx, "oldest", mq.Message{Body: []byte("old1")}); err != nil {
		t.Fatalf("Publish old1: %v", err)
	}
	if err := broker.Publish(ctx, "oldest", mq.Message{Body: []byte("old2")}); err != nil {
		t.Fatalf("Publish old2: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Create consumer directly with StartFromOldest=true
	// This will create a consumer with DeliverAllPolicy
	consumer, err := broker.Consume(ctx, "oldest", "workers", "", WithStartFromOldest(true))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()

	// With StartFromOldest, should receive messages from the beginning
	delivery, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	body := string(delivery.Message.Body)
	if body != "old1" && body != "old2" {
		t.Fatalf("expected 'old1' or 'old2', got %s", body)
	}
	if err := delivery.Ack(ctx); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	delivery2, err := consumer.Receive(rctx)
	if err != nil {
		t.Fatalf("Receive second: %v", err)
	}
	body2 := string(delivery2.Message.Body)
	if body2 == body {
		t.Fatalf("received duplicate message: %s", body2)
	}
	if body2 != "old1" && body2 != "old2" {
		t.Fatalf("expected 'old1' or 'old2', got %s", body2)
	}
}

func TestConsistencyPublishOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := startNATS(ctx)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer func() {
		_ = container.Terminate(context.Background())
	}()

	host, port := containerAddress(t, ctx, container, "4222/tcp")

	cfg := Config{
		Connection: mq.Config{
			Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
		},
		PublishMode:   PublishModeJetStream,
		StreamName:    "opts",
		SubjectPrefix: "test",
		PullBatch:     1,
	}

	broker, err := NewBroker(ctx, cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	defer broker.Close(context.Background()) // nolint:errcheck

	// Create queue first
	if err := broker.CreateQueue(ctx, "opts", "workers"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	consumer, err := broker.Consume(ctx, "opts", "workers", "")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer consumer.Close() // nolint:errcheck

	time.Sleep(100 * time.Millisecond) // Give consumer time to be ready

	// Test PublishOptions Headers and CorrelationID
	msg := mq.Message{
		Body:        []byte("test-opts"),
		Headers:     map[string]string{"msg-header": "msg-value"},
		ContentType: "application/json",
	}

	if err := broker.Publish(ctx, "opts", msg,
		mq.WithHeaders(map[string]string{"custom-header": "value"}),
		mq.WithReplyTo("reply.subject"),
		mq.WithCorrelationID("corr-123"),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	defer rcancel()
	delivery, err := consumer.Receive(rctx)
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

func containerAddress(t *testing.T, ctx context.Context, container testcontainers.Container, port string) (string, string) {
	t.Helper()
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container.Host: %v", err)
	}
	mapped, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		t.Fatalf("container.MappedPort: %v", err)
	}
	return host, mapped.Port()
}
