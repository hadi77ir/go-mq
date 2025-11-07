package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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

// Shared container instances
var (
	sharedContainers struct {
		sync.Mutex
		valkey       testcontainers.Container
		nats         testcontainers.Container
		rabbitmq     testcontainers.Container
		valkeyOnce   sync.Once
		natsOnce     sync.Once
		rabbitmqOnce sync.Once
		valkeyErr    error
		natsErr      error
		rabbitmqErr  error
	}
)

func initSharedContainers(ctx context.Context) {
	sharedContainers.valkeyOnce.Do(func() {
		sharedContainers.valkey, sharedContainers.valkeyErr = startValkey(ctx)
	})
	sharedContainers.natsOnce.Do(func() {
		sharedContainers.nats, sharedContainers.natsErr = startNATS(ctx)
	})
	sharedContainers.rabbitmqOnce.Do(func() {
		sharedContainers.rabbitmq, sharedContainers.rabbitmqErr = startRabbitMQ(ctx)
	})
}

func cleanupSharedContainers(ctx context.Context) {
	sharedContainers.Lock()
	defer sharedContainers.Unlock()

	if sharedContainers.valkey != nil {
		_ = sharedContainers.valkey.Terminate(ctx)
	}
	if sharedContainers.nats != nil {
		_ = sharedContainers.nats.Terminate(ctx)
	}
	if sharedContainers.rabbitmq != nil {
		_ = sharedContainers.rabbitmq.Terminate(ctx)
	}
}

type brokerSetup struct {
	name       string
	mode       string
	routingKey string // Topic name/routing key for this setup
	setup      func(context.Context, *testing.T) (mq.Broker, func())
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Initialize shared containers
	initSharedContainers(ctx)

	// Run tests
	code := m.Run()

	// Cleanup shared containers
	cleanupSharedContainers(ctx)

	// Exit with test result code
	os.Exit(code)
}

func TestConsistencyAutoAck(t *testing.T) {
	setups := getAllBrokerSetups(t, "autoack")

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
			queue := fmt.Sprintf("autoack-%s-%s", setup.name, setup.mode)
			if queue == "autoack-RabbitMQ-" {
				queue = "autoack-RabbitMQ-default"
			}

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
		// StartFromOldest not applicable to RabbitMQ Streams (consumer groups track offsets centrally)
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
			queue := fmt.Sprintf("oldest-%s-%s", setup.name, setup.mode)

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
			queue := fmt.Sprintf("opts-%s-%s", setup.name, setup.mode)
			if queue == "opts-RabbitMQ-" {
				queue = "opts-RabbitMQ-default"
			}

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

func TestConsistencyMultiConsumer(t *testing.T) {
	// Test work queue pattern: messages should be distributed among consumers
	// Skip pub-sub modes as they broadcast to all consumers
	setups := []brokerSetup{
		setupValkey("Streams", t, "multiconsumer"),
		setupValkey("List", t, "multiconsumer"),
		setupNATS("JetStream", t, "multiconsumer"),
		setupRabbitMQ("", t, "multiconsumer"),
		setupRabbitMQ("PushPull", t, "multiconsumer"),
		setupRabbitMQ("PersistentPushPull", t, "multiconsumer"),
	}

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			// Create multiple broker instances to ensure multiple connections
			broker1, cleanup1 := setup.setup(ctx, t)
			defer cleanup1()
			defer broker1.Close(context.Background()) // nolint:errcheck

			broker2, cleanup2 := setup.setup(ctx, t)
			defer cleanup2()
			defer broker2.Close(context.Background()) // nolint:errcheck

			topic := setup.routingKey
			queue := fmt.Sprintf("multiconsumer-%s-%s", setup.name, setup.mode)
			if queue == "multiconsumer-RabbitMQ-" {
				queue = "multiconsumer-RabbitMQ-default"
			}

			// Create queue using first broker
			if err := broker1.CreateQueue(ctx, topic, queue); err != nil {
				if errors.Is(err, mq.ErrNotSupported) {
					t.Skip("CreateQueue not supported for this broker/mode")
				}
				t.Fatalf("CreateQueue: %v", err)
			}

			// Flush to ensure queue creation is complete
			if err := broker1.Flush(ctx); err != nil {
				t.Fatalf("Flush broker1: %v", err)
			}

			// Create consumers from different brokers to ensure multiple connections
			consumer1, err := broker1.Consume(ctx, topic, queue, "consumer1")
			if err != nil {
				t.Fatalf("Consume consumer1: %v", err)
			}
			defer consumer1.Close() // nolint:errcheck

			consumer2, err := broker2.Consume(ctx, topic, queue, "consumer2")
			if err != nil {
				t.Fatalf("Consume consumer2: %v", err)
			}
			defer consumer2.Close() // nolint:errcheck

			// Flush both brokers to ensure consumers are ready
			if err := broker1.Flush(ctx); err != nil {
				t.Fatalf("Flush broker1 after consume: %v", err)
			}
			if err := broker2.Flush(ctx); err != nil {
				t.Fatalf("Flush broker2 after consume: %v", err)
			}

			// Give consumers time to bind
			time.Sleep(200 * time.Millisecond)

			// Publish 4 messages using both brokers to ensure multiple connections
			for i := 1; i <= 4; i++ {
				msg := mq.Message{Body: []byte(fmt.Sprintf("msg%d", i))}
				// Alternate between brokers for publishing
				var pubBroker mq.Broker
				if i%2 == 0 {
					pubBroker = broker2
				} else {
					pubBroker = broker1
				}
				if err := pubBroker.Publish(ctx, topic, msg); err != nil {
					t.Fatalf("Publish msg%d: %v", i, err)
				}
			}

			// Flush both brokers to ensure all messages are published
			if err := broker1.Flush(ctx); err != nil {
				t.Fatalf("Flush broker1 after publish: %v", err)
			}
			if err := broker2.Flush(ctx); err != nil {
				t.Fatalf("Flush broker2 after publish: %v", err)
			}

			recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
			defer recvCancel()

			// Track which messages each consumer receives
			consumer1Msgs := make(map[string]bool)
			consumer2Msgs := make(map[string]bool)

			// Both consumers should receive messages (work queue distribution)
			// We expect each message to be received by exactly one consumer
			receivedCount := 0
			const receiveAttemptTimeout = 500 * time.Millisecond
			type consumeResult struct {
				consumerID int
				delivery   *mq.Delivery
				err        error
			}
			for receivedCount < 4 {
				select {
				case <-recvCtx.Done():
					t.Fatalf("timeout waiting for messages, received %d/4", receivedCount)
				default:
					results := make(chan consumeResult, 2)
					startReceive := func(id int, c mq.Consumer) context.CancelFunc {
						attemptCtx, attemptCancel := context.WithTimeout(recvCtx, receiveAttemptTimeout)
						go func() {
							delivery, err := c.Receive(attemptCtx)
							results <- consumeResult{
								consumerID: id,
								delivery:   delivery,
								err:        err,
							}
						}()
						return attemptCancel
					}

					cancel1 := startReceive(1, consumer1)
					cancel2 := startReceive(2, consumer2)

					success := false
					for i := 0; i < 2; i++ {
						select {
						case res := <-results:
							if res.err == nil && res.delivery != nil {
								body := string(res.delivery.Message.Body)
								if res.consumerID == 1 {
									consumer1Msgs[body] = true
									if err := res.delivery.Ack(ctx); err != nil {
										cancel1()
										cancel2()
										t.Fatalf("Ack consumer1: %v", err)
									}
									cancel2()
								} else {
									consumer2Msgs[body] = true
									if err := res.delivery.Ack(ctx); err != nil {
										cancel1()
										cancel2()
										t.Fatalf("Ack consumer2: %v", err)
									}
									cancel1()
								}
								receivedCount++
								success = true
							}
						case <-recvCtx.Done():
							cancel1()
							cancel2()
							t.Fatalf("timeout waiting for messages, received %d/4", receivedCount)
						}
					}

					cancel1()
					cancel2()

					if success {
						continue
					}

					// Both failed, wait a bit
					time.Sleep(100 * time.Millisecond)

					// Drain any remaining results to avoid goroutine leaks
				drainLoop:
					for {
						select {
						case <-results:
						default:
							break drainLoop
						}
					}
				}
			}

			// Verify all messages were received
			allMsgs := make(map[string]bool)
			for msg := range consumer1Msgs {
				allMsgs[msg] = true
			}
			for msg := range consumer2Msgs {
				if allMsgs[msg] {
					t.Fatalf("message %s received by both consumers (should be distributed)", msg)
				}
				allMsgs[msg] = true
			}

			for i := 1; i <= 4; i++ {
				expected := fmt.Sprintf("msg%d", i)
				if !allMsgs[expected] {
					t.Fatalf("message %s not received by any consumer", expected)
				}
			}

			// Verify both consumers received at least one message
			if len(consumer1Msgs) == 0 {
				t.Fatalf("consumer1 received no messages")
			}
			if len(consumer2Msgs) == 0 {
				t.Fatalf("consumer2 received no messages")
			}
		})
	}
}

func TestConsistencyMultiPublisher(t *testing.T) {
	// Test concurrent publishing from multiple publishers using multiple broker instances
	setups := getAllBrokerSetups(t, "multipublisher")

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			// Create multiple broker instances to ensure multiple connections
			broker1, cleanup1 := setup.setup(ctx, t)
			defer cleanup1()
			defer broker1.Close(context.Background()) // nolint:errcheck

			broker2, cleanup2 := setup.setup(ctx, t)
			defer cleanup2()
			defer broker2.Close(context.Background()) // nolint:errcheck

			broker3, cleanup3 := setup.setup(ctx, t)
			defer cleanup3()
			defer broker3.Close(context.Background()) // nolint:errcheck

			brokers := []mq.Broker{broker1, broker2, broker3}

			topic := setup.routingKey
			queue := fmt.Sprintf("multipublisher-%s-%s", setup.name, setup.mode)
			if queue == "multipublisher-RabbitMQ-" {
				queue = "multipublisher-RabbitMQ-default"
			}

			// Create queue using first broker
			err := broker1.CreateQueue(ctx, topic, queue)
			if err != nil && !errors.Is(err, mq.ErrNotSupported) {
				t.Fatalf("CreateQueue: %v", err)
			}

			// Flush to ensure queue creation is complete
			if err := broker1.Flush(ctx); err != nil {
				t.Fatalf("Flush broker1: %v", err)
			}

			// Create consumer using first broker
			var consumer mq.Consumer
			if errors.Is(err, mq.ErrNotSupported) {
				consumer, err = broker1.Consume(ctx, topic, "", "")
			} else {
				consumer, err = broker1.Consume(ctx, topic, queue, "")
			}
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			defer consumer.Close() // nolint:errcheck

			// Flush to ensure consumer is ready
			if err := broker1.Flush(ctx); err != nil {
				t.Fatalf("Flush broker1 after consume: %v", err)
			}

			// Give consumer time to bind
			time.Sleep(200 * time.Millisecond)

			// Create 3 publishers that publish concurrently using different brokers
			numPublishers := 3
			messagesPerPublisher := 5
			totalMessages := numPublishers * messagesPerPublisher

			publishErr := make(chan error, totalMessages)
			var wg sync.WaitGroup

			// Start concurrent publishers, each using a different broker instance
			for p := 0; p < numPublishers; p++ {
				wg.Add(1)
				pubBroker := brokers[p]
				go func(publisherID int, broker mq.Broker) {
					defer wg.Done()
					for i := 0; i < messagesPerPublisher; i++ {
						msg := mq.Message{
							Body: []byte(fmt.Sprintf("publisher%d-msg%d", publisherID, i)),
						}
						if err := broker.Publish(ctx, topic, msg); err != nil {
							publishErr <- fmt.Errorf("publisher%d msg%d: %w", publisherID, i, err)
						}
					}
					// Flush this broker to ensure all messages are published
					if err := broker.Flush(ctx); err != nil {
						publishErr <- fmt.Errorf("publisher%d flush: %w", publisherID, err)
					}
				}(p, pubBroker)
			}

			// Wait for all publishers to finish
			wg.Wait()
			close(publishErr)

			// Check for publish errors
			for err := range publishErr {
				t.Fatalf("Publish error: %v", err)
			}

			// Receive all messages
			recvCtx, recvCancel := context.WithTimeout(ctx, 30*time.Second)
			defer recvCancel()

			receivedMsgs := make(map[string]bool)
			for len(receivedMsgs) < totalMessages {
				delivery, err := consumer.Receive(recvCtx)
				if err != nil {
					t.Fatalf("Receive: %v (received %d/%d)", err, len(receivedMsgs), totalMessages)
				}

				body := string(delivery.Message.Body)
				if receivedMsgs[body] {
					t.Fatalf("duplicate message received: %s", body)
				}
				receivedMsgs[body] = true

				if err := delivery.Ack(ctx); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}

			// Verify all messages were received
			for p := 0; p < numPublishers; p++ {
				for i := 0; i < messagesPerPublisher; i++ {
					expected := fmt.Sprintf("publisher%d-msg%d", p, i)
					if !receivedMsgs[expected] {
						t.Fatalf("message not received: %s", expected)
					}
				}
			}
		})
	}
}

func TestConsistencyPubSubMultiConsumer(t *testing.T) {
	// Test pub-sub pattern consistency: all consumers should receive all messages
	// This test verifies that pub-sub behavior is consistent across different broker implementations
	// Each consumer uses a separate broker instance to ensure multiple connections
	setups := []brokerSetup{
		setupValkey("PubSub", t, "pubsubmulticonsumer"),
		setupNATS("Core", t, "pubsubmulticonsumer"),
		setupRabbitMQ("PubSub", t, "pubsubmulticonsumer"),
		setupRabbitMQ("PersistentPubSub", t, "pubsubmulticonsumer"),
	}

	for _, setup := range setups {
		testName := setup.name
		if setup.mode != "" {
			testName = setup.name + "/" + setup.mode
		}
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			topic := setup.routingKey
			numConsumers := 3
			messages := []string{"msg1", "msg2", "msg3"}

			// Create one broker instance per consumer to ensure multiple connections
			brokers := make([]mq.Broker, numConsumers)
			cleanups := make([]func(), numConsumers)
			for i := 0; i < numConsumers; i++ {
				broker, cleanup := setup.setup(ctx, t)
				brokers[i] = broker
				cleanups[i] = cleanup
				defer cleanup()
				defer broker.Close(context.Background()) // nolint:errcheck
			}

			// Create consumers, each from a different broker instance
			consumers := make([]mq.Consumer, numConsumers)
			for i := 0; i < numConsumers; i++ {
				consumerName := fmt.Sprintf("consumer%d", i+1)
				consumer, err := brokers[i].Consume(ctx, topic, "", consumerName)
				if err != nil {
					t.Fatalf("Consume %s: %v", consumerName, err)
				}
				consumers[i] = consumer
				defer consumer.Close() // nolint:errcheck

				// Flush each broker to ensure consumer is ready
				if err := brokers[i].Flush(ctx); err != nil {
					t.Fatalf("Flush broker%d: %v", i+1, err)
				}
			}

			// Give consumers time to bind and subscribe
			time.Sleep(2 * time.Second)

			// Start receiving from all consumers concurrently before publishing
			// This ensures all consumers are actively listening when messages arrive
			recvCtx, recvCancel := context.WithTimeout(ctx, 60*time.Second)
			defer recvCancel()

			consumerMsgs := make([]map[string]bool, numConsumers)
			consumerErrs := make([]error, numConsumers)
			var wg sync.WaitGroup

			for i, consumer := range consumers {
				consumerMsgs[i] = make(map[string]bool)
				wg.Add(1)
				go func(idx int, cons mq.Consumer) {
					defer wg.Done()
					receivedCount := 0
					for receivedCount < len(messages) {
						delivery, err := cons.Receive(recvCtx)
						if err != nil {
							consumerErrs[idx] = fmt.Errorf("Consumer%d Receive: %v (received %d/%d)", idx+1, err, receivedCount, len(messages))
							return
						}

						body := string(delivery.Message.Body)
						// Skip any test/ready messages that might have been sent
						if strings.HasPrefix(body, "__ready") {
							_ = delivery.Ack(ctx) // Ignore errors
							continue
						}

						if consumerMsgs[idx][body] {
							consumerErrs[idx] = fmt.Errorf("Consumer%d received duplicate: %s", idx+1, body)
							return
						}
						consumerMsgs[idx][body] = true
						receivedCount++

						// Ack if supported (some pub-sub modes don't support acks)
						_ = delivery.Ack(ctx) // Ignore errors for pub-sub modes
					}
				}(i, consumer)
			}

			// Wait for all receive goroutines to start
			time.Sleep(500 * time.Millisecond)

			// Publish messages using the first broker instance
			// In pub-sub, all consumers should receive all messages regardless of which broker publishes
			for _, msgBody := range messages {
				msg := mq.Message{Body: []byte(msgBody)}
				if err := brokers[0].Publish(ctx, topic, msg); err != nil {
					t.Fatalf("Publish %s: %v", msgBody, err)
				}
				// Flush after each publish to ensure message is sent
				if err := brokers[0].Flush(ctx); err != nil {
					t.Fatalf("Flush after publish %s: %v", msgBody, err)
				}
				// Small delay between publishes to ensure message distribution
				time.Sleep(200 * time.Millisecond)
			}

			wg.Wait()

			// Verify consistency: all consumers should have received all messages
			for i := range consumers {
				if consumerErrs[i] != nil {
					t.Fatalf("Consumer%d error: %v", i+1, consumerErrs[i])
				}

				// Verify all messages were received by this consumer
				for _, expected := range messages {
					if !consumerMsgs[i][expected] {
						t.Fatalf("Consumer%d did not receive: %s (pub-sub should deliver to all consumers)", i+1, expected)
					}
				}

				// Verify no duplicate messages
				if len(consumerMsgs[i]) != len(messages) {
					t.Fatalf("Consumer%d received %d unique messages, expected %d", i+1, len(consumerMsgs[i]), len(messages))
				}
			}

			// Verify consistency across consumers: all should have received the same set of messages
			for i := 1; i < numConsumers; i++ {
				for msg := range consumerMsgs[0] {
					if !consumerMsgs[i][msg] {
						t.Fatalf("Inconsistent pub-sub behavior: Consumer1 received %s but Consumer%d did not", msg, i+1)
					}
				}
				for msg := range consumerMsgs[i] {
					if !consumerMsgs[0][msg] {
						t.Fatalf("Inconsistent pub-sub behavior: Consumer%d received %s but Consumer1 did not", i+1, msg)
					}
				}
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
		setupRabbitMQ("Streams", t, testPrefix),
		setupRabbitMQ("PubSub", t, testPrefix),
		setupRabbitMQ("PersistentPubSub", t, testPrefix),
		setupRabbitMQ("PushPull", t, testPrefix),
		setupRabbitMQ("PersistentPushPull", t, testPrefix),
	}
}

func setupValkey(mode string, t *testing.T, testPrefix string) brokerSetup {
	// Use mode-specific prefix to avoid WRONGTYPE errors when sharing container
	// Different modes use different Redis data structures (streams, pubsub channels, lists)
	routingKey := fmt.Sprintf("%s-%s-test", mode, testPrefix)
	return brokerSetup{
		name:       "Valkey",
		mode:       mode,
		routingKey: routingKey,
		setup: func(ctx context.Context, t *testing.T) (mq.Broker, func()) {
			initSharedContainers(ctx)
			if sharedContainers.valkeyErr != nil {
				t.Fatalf("startValkey: %v", sharedContainers.valkeyErr)
			}
			container := sharedContainers.valkey
			cleanup := func() {
				// No-op, container is shared
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
			initSharedContainers(ctx)
			if sharedContainers.natsErr != nil {
				t.Fatalf("startNATS: %v", sharedContainers.natsErr)
			}
			container := sharedContainers.nats
			cleanup := func() {
				// No-op, container is shared
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
			initSharedContainers(ctx)
			if sharedContainers.rabbitmqErr != nil {
				t.Fatalf("startRabbitMQ: %v", sharedContainers.rabbitmqErr)
			}
			container := sharedContainers.rabbitmq
			cleanup := func() {
				// No-op, container is shared
			}

			host, err := container.Host(ctx)
			if err != nil {
				t.Fatalf("container.Host: %v", err)
			}
			port, err := container.MappedPort(ctx, "5672/tcp")
			if err != nil {
				t.Fatalf("container.MappedPort: %v", err)
			}
			var streamPort nat.Port
			if mode == "Streams" {
				streamPort, err = container.MappedPort(ctx, "5552/tcp")
				if err != nil {
					t.Fatalf("container.MappedPort stream: %v", err)
				}
			}

			// Use different exchanges for different modes to avoid conflicts
			exchangeName := "test.exchange"
			if mode != "" {
				exchangeName = fmt.Sprintf("test.%s.exchange", mode)
			}

			// Increase connection pool for pub-sub modes to handle multiple consumers
			maxConnections := 5
			if mode == "PubSub" || mode == "PersistentPubSub" {
				maxConnections = 10 // More connections needed for pub-sub with multiple consumers
			}

			cfg := mqrabbitmq.Config{
				Connection: mq.Config{
					Addresses: []string{host + ":" + port.Port()},
					Username:  "guest",
					Password:  "guest",
				},
				MaxConnections:  maxConnections,
				Exchange:        exchangeName,
				ExchangeType:    "direct",
				DeclareExchange: true,
				Prefetch:        10,
				QueueDurable:    false,
			}

			switch mode {
			case "PubSub":
				cfg.PublishMode = mqrabbitmq.PublishModePubSub
				cfg.ExchangeType = "fanout"
			case "PersistentPubSub":
				cfg.PublishMode = mqrabbitmq.PublishModePersistentPubSub
				cfg.ExchangeType = "fanout"
			case "Streams":
				cfg.PublishMode = mqrabbitmq.PublishModeStreams
				cfg.ExchangeType = "direct"
				cfg.QueueDurable = true
				cfg.Stream = mqrabbitmq.StreamConfig{
					Addresses:             []string{fmt.Sprintf("%s:%s", host, streamPort.Port())},
					Partitions:            3,
					MaxLengthBytes:        1 << 22,
					MaxSegmentSizeBytes:   1 << 20,
					MaxAge:                10 * time.Minute,
					MaxProducersPerClient: 5,
					MaxConsumersPerClient: 5,
				}
			case "PushPull":
				cfg.PublishMode = mqrabbitmq.PublishModePushPull
				cfg.ExchangeType = "direct"
			case "PersistentPushPull":
				cfg.PublishMode = mqrabbitmq.PublishModePersistentPushPull
				cfg.ExchangeType = "direct"
			default:
				cfg.PublishMode = mqrabbitmq.PublishModePersistentPushPull
				cfg.ExchangeType = "direct"
			}

			broker, err := mqrabbitmq.NewBroker(ctx, cfg)
			if err != nil {
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
		ExposedPorts: []string{"5672/tcp", "5552/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete"),
	}

	pluginFile, err := writeStreamPluginFile()
	if err != nil {
		return nil, err
	}
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
