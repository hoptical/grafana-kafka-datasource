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
// does not already exist. Mirrors the helper in ../producer.go, trimmed to
// use time.Duration directly.
func createTopicIfNotExists(brokerURL, topic string, partitions int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokerURL)
	if err != nil {
		return fmt.Errorf("failed to dial leader: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("failed to close connection: %v\n", err)
		}
	}()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
	defer cancel2()
	controllerConn, err := kafka.DialContext(ctx2, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
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
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

// waitForTopicLeaders blocks until every partition of topic has an elected
// leader, or timeout elapses.
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
