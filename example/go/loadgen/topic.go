package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// createTopicIfNotExists creates topic with the given partition count if it
// does not already exist, and returns the topic's actual partition count -
// which may differ from the requested count if the topic already existed.
// Mirrors the helper in ../producer.go, trimmed to use time.Duration
// directly.
func createTopicIfNotExists(brokerURL, topic string, partitions int, timeout time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokerURL)
	if err != nil {
		return 0, fmt.Errorf("failed to dial leader: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("failed to close connection: %v\n", err)
		}
	}()

	// Check whether the topic already exists by listing all topics, rather
	// than querying metadata for this specific topic name - the latter can
	// trigger Kafka's auto-create-on-metadata-request behavior on brokers
	// with auto.create.topics.enable=true, silently creating the topic with
	// the broker's default partition count instead of -num-partitions.
	if existing, err := conn.ReadPartitions(); err == nil {
		count := 0
		for _, p := range existing {
			if p.Topic == topic {
				count++
			}
		}
		if count > 0 {
			return count, nil
		}
	}

	controller, err := conn.Controller()
	if err != nil {
		return 0, fmt.Errorf("failed to get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
	defer cancel2()
	controllerConn, err := kafka.DialContext(ctx2, "tcp", controllerAddr)
	if err != nil {
		return 0, fmt.Errorf("failed to dial controller: %w", err)
	}
	defer func() {
		if err := controllerConn.Close(); err != nil {
			fmt.Printf("failed to close controller connection: %v\n", err)
		}
	}()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create topic: %w", err)
	}
	return partitions, nil
}

// waitForTopicLeaders blocks until every partition of topic has an elected
// leader, or timeout elapses. partitions should be the topic's actual
// partition count (e.g. as returned by createTopicIfNotExists), not
// necessarily the -num-partitions flag value, since a pre-existing topic may
// have a different partition count than requested.
func waitForTopicLeaders(brokerURL, topic string, partitions int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		allReady := true
		for p := 0; p < partitions; p++ {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("timed out waiting for leaders for topic %s", topic)
			}
			attemptTimeout := 2 * time.Second
			if remaining < attemptTimeout {
				attemptTimeout = remaining
			}
			ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
			conn, err := kafka.DialLeader(ctx, "tcp", brokerURL, topic, p)
			cancel()
			if err != nil {
				allReady = false
				break
			}
			_ = conn.Close()
		}

		if allReady {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for leaders for topic %s", topic)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
