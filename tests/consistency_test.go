package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/hadi77ir/go-mq"
	mqnats "github.com/hadi77ir/go-mq/nats"
	mqrabbitmq "github.com/hadi77ir/go-mq/rabbitmq"
	mqvalkey "github.com/hadi77ir/go-mq/valkey"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type brokerSetup struct {
	name       string
	mode       string
	routingKey string // Topic name/routing key for this setup
	setup      func(context.Context, *testing.T) (mq.Broker, func())
	cleanup    func(context.Context, *testing.T)
}

func TestConsistencyAutoAck(t *testing.T) {
	setups := getAllBrokerSetups(t, "autoadk")

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			broker, cleanup := setup.setup(ctx, t)
			defer cleanup()
			defer broker.Close(context.Background()) // nolint:errcheck

			// Test AutoAck=true - messages should be automatically acknowledged
			topic := setup.routingKey
			queue := "test-group"

			var consumer mq.Consumer
			var err error

			// Try to create queue, but ignore ErrNotSupported
			err = broker.CreateQueue(ctx, topic, queue)
			if err != nil && !errors.Is(err, mq.ErrNotSupported) {
				t.Fatalf("CreateQueue: %v", err)
			}

			// If CreateQueue returned ErrNotSupported, consume without queue
			if errors.Is(err, mq.ErrNotSupported) {
				consumer, err = broker.Consume(ctx, topic, "", "", mq.WithAutoAck(true))
			} else {
				consumer, err = broker.Consume(ctx, topic, queue, "", mq.WithAutoAck(true))
			}
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			defer consumer.Close() // nolint:errcheck

			if err := broker.Publish(ctx, topic, mq.Message{Body: []byte("test1")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
			defer recvCancel()
			delivery, err := consumer.Receive(recvCtx)
			if err != nil {
				t.Fatalf("Receive: %v", err)
			}
			if string(delivery.Message.Body) != "test1" {
				t.Fatalf("unexpected body: %s", delivery.Message.Body)
			}

			// With AutoAck, message should already be acked, so receiving again should get next message
			if err := broker.Publish(ctx, topic, mq.Message{Body: []byte("test2")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			delivery2, err := consumer.Receive(recvCtx)
			if err != nil {
				t.Fatalf("Receive second: %v", err)
			}
			if string(delivery2.Message.Body) != "test2" {
				t.Fatalf("unexpected body: %s", delivery2.Message.Body)
			}
		})
	}
}

func TestConsistencyStartFromOldest(t *testing.T) {
	// StartFromOldest only works with modes that support queues
	// Note: Valkey Streams has limitations with "0-0" in XREADGROUP (only reads pending entries)
	setups := []brokerSetup{
		// Skip Valkey Streams due to "0-0" limitation with XREADGROUP
		setupNATS("JetStream", t, "oldest"),
		setupRabbitMQ("", t, "oldest"),
	}

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			broker, cleanup := setup.setup(ctx, t)
			defer cleanup()
			defer broker.Close(context.Background()) // nolint:errcheck

			topic := setup.routingKey
			queue := "oldest-group"

			// Create queue first
			if err := broker.CreateQueue(ctx, topic, queue); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}

			// Publish messages before creating consumer
			if err := broker.Publish(ctx, topic, mq.Message{Body: []byte("old1")}); err != nil {
				t.Fatalf("Publish old1: %v", err)
			}
			if err := broker.Publish(ctx, topic, mq.Message{Body: []byte("old2")}); err != nil {
				t.Fatalf("Publish old2: %v", err)
			}

			time.Sleep(200 * time.Millisecond)

			// Test StartFromOldest=true - should receive messages from the beginning
			// Note: This is a NATS-specific option, but other brokers will ignore it
			consumer, err := broker.Consume(ctx, topic, queue, "", mqnats.WithStartFromOldest(true))
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			defer consumer.Close() // nolint:errcheck

			recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
			defer recvCancel()

			delivery, err := consumer.Receive(recvCtx)
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

			delivery2, err := consumer.Receive(recvCtx)
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
			if err := delivery2.Ack(ctx); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

func TestConsistencyPublishOptions(t *testing.T) {
	setups := getAllBrokerSetups(t, "opts")

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			broker, cleanup := setup.setup(ctx, t)
			defer cleanup()
			defer broker.Close(context.Background()) // nolint:errcheck

			topic := setup.routingKey
			queue := "opts-group"

			var consumer mq.Consumer
			var err error

			// Try to create queue, but ignore ErrNotSupported
			err = broker.CreateQueue(ctx, topic, queue)
			if err != nil && !errors.Is(err, mq.ErrNotSupported) {
				t.Fatalf("CreateQueue: %v", err)
			}

			// If CreateQueue returned ErrNotSupported, consume without queue
			if errors.Is(err, mq.ErrNotSupported) {
				consumer, err = broker.Consume(ctx, topic, "", "")
				if err != nil {
					t.Fatalf("Consume: %v", err)
				}
				defer consumer.Close() // nolint:errcheck
				// Give subscription time to establish for pubsub/list modes
				time.Sleep(200 * time.Millisecond)
			} else {
				consumer, err = broker.Consume(ctx, topic, queue, "")
				if err != nil {
					t.Fatalf("Consume: %v", err)
				}
				defer consumer.Close() // nolint:errcheck
				time.Sleep(100 * time.Millisecond)
			}

			// Test PublishOptions
			msg := mq.Message{
				Body:        []byte("test-opts"),
				Headers:     map[string]string{"msg-header": "msg-value"},
				ContentType: "application/json",
			}

			if err := broker.Publish(ctx, topic, msg,
				mq.WithHeaders(map[string]string{"custom-header": "value"}),
				mq.WithReplyTo("reply.queue"),
				mq.WithCorrelationID("corr-123"),
			); err != nil {
				t.Fatalf("Publish: %v", err)
			}

			recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
			defer recvCancel()
			delivery, err := consumer.Receive(recvCtx)
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
		})
	}
}

// Helper functions for creating broker setups

func getAllBrokerSetups(t *testing.T, testPrefix string) []brokerSetup {
	return []brokerSetup{
		setupValkey("Streams", t, testPrefix),
		setupValkey("PubSub", t, testPrefix),
		setupValkey("List", t, testPrefix),
		setupNATS("Core", t, testPrefix),
		setupNATS("JetStream", t, testPrefix),
		setupRabbitMQ("", t, testPrefix),
	}
}

func setupValkey(mode string, t *testing.T, testPrefix string) brokerSetup {
	routingKey := testPrefix + "-test"
	return brokerSetup{
		name:       "Valkey",
		mode:       mode,
		routingKey: routingKey,
		setup: func(ctx context.Context, t *testing.T) (mq.Broker, func()) {
			container, err := startValkey(ctx)
			if err != nil {
				t.Fatalf("startValkey: %v", err)
			}
			cleanup := func() {
				_ = container.Terminate(context.Background())
			}

			host, port := containerAddress(t, ctx, container, "6379/tcp")
			cfg := mqvalkey.Config{
				Connection: mq.Config{
					Addresses: []string{host + ":" + port},
				},
				AutoCreateStream: true,
				AutoCreateGroup:  true,
				BatchSize:        1,
			}

			switch mode {
			case "Streams":
				cfg.PublishMode = mqvalkey.PublishModeStreams
			case "PubSub":
				cfg.PublishMode = mqvalkey.PublishModePubSub
			case "List":
				cfg.PublishMode = mqvalkey.PublishModeList
			default:
				cfg.PublishMode = mqvalkey.PublishModeStreams
			}

			broker, err := mqvalkey.NewBroker(ctx, cfg)
			if err != nil {
				cleanup()
				t.Fatalf("NewBroker: %v", err)
			}
			return broker, cleanup
		},
	}
}

func setupNATS(mode string, t *testing.T, testPrefix string) brokerSetup {
	routingKey := testPrefix + "-test"
	return brokerSetup{
		name:       "NATS",
		mode:       mode,
		routingKey: routingKey,
		setup: func(ctx context.Context, t *testing.T) (mq.Broker, func()) {
			container, err := startNATS(ctx)
			if err != nil {
				t.Fatalf("startNATS: %v", err)
			}
			cleanup := func() {
				_ = container.Terminate(context.Background())
			}

			host, port := containerAddress(t, ctx, container, "4222/tcp")
			cfg := mqnats.Config{
				Connection: mq.Config{
					Addresses: []string{fmt.Sprintf("%s:%s", host, port)},
				},
				SubjectPrefix: "test",
				PullBatch:     1,
			}

			switch mode {
			case "Core":
				cfg.PublishMode = mqnats.PublishModeCore
			case "JetStream":
				cfg.PublishMode = mqnats.PublishModeJetStream
				cfg.StreamName = "test-stream"
			default:
				cfg.PublishMode = mqnats.PublishModeJetStream
				cfg.StreamName = "test-stream"
			}

			broker, err := mqnats.NewBroker(ctx, cfg)
			if err != nil {
				cleanup()
				t.Fatalf("NewBroker: %v", err)
			}
			return broker, cleanup
		},
	}
}

func setupRabbitMQ(mode string, t *testing.T, testPrefix string) brokerSetup {
	routingKey := testPrefix + ".route"
	return brokerSetup{
		name:       "RabbitMQ",
		mode:       mode,
		routingKey: routingKey,
		setup: func(ctx context.Context, t *testing.T) (mq.Broker, func()) {
			container, err := startRabbitMQ(ctx)
			if err != nil {
				t.Fatalf("startRabbitMQ: %v", err)
			}
			cleanup := func() {
				_ = container.Terminate(context.Background())
			}

			host, err := container.Host(ctx)
			if err != nil {
				cleanup()
				t.Fatalf("container.Host: %v", err)
			}
			port, err := container.MappedPort(ctx, "5672/tcp")
			if err != nil {
				cleanup()
				t.Fatalf("container.MappedPort: %v", err)
			}

			cfg := mqrabbitmq.Config{
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
			}

			broker, err := mqrabbitmq.NewBroker(ctx, cfg)
			if err != nil {
				cleanup()
				t.Fatalf("NewBroker: %v", err)
			}
			return broker, cleanup
		},
	}
}

// Helper functions for starting testcontainers

func startValkey(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:7.2",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startNATS(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "nats:2.10-alpine",
		Cmd:          []string{"-js"},
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Server is ready"),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func startRabbitMQ(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.13-management",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete"),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
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
