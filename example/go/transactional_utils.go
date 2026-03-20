package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// transactionalProducer wraps a Sarama transactional sync producer.
type transactionalProducer struct {
	producer sarama.SyncProducer
	client   sarama.Client
}

func newTransactionalProducer(brokers []string, transactionalID string) (*transactionalProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Producer.Transaction.ID = transactionalID
	cfg.Metadata.Retry.Max = 5
	cfg.Metadata.Retry.Backoff = 500 * time.Millisecond
	cfg.Metadata.RefreshFrequency = 30 * time.Second
	cfg.Net.MaxOpenRequests = 1
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}
	return &transactionalProducer{producer: producer, client: client}, nil
}

func (tp *transactionalProducer) WriteMessage(ctx context.Context, topic string, key, value []byte) error {
	if tp == nil || tp.producer == nil {
		return fmt.Errorf("transactional producer is not initialized")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := tp.producer.BeginTxn(); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	_, _, err := tp.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
	if err != nil {
		_ = tp.producer.AbortTxn()
		return fmt.Errorf("failed to send transactional message: %w", err)
	}

	if err := tp.producer.CommitTxn(); err != nil {
		_ = tp.producer.AbortTxn()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func isRetryableTransactionalError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sarama.ErrNotLeaderForPartition) ||
		errors.Is(err, sarama.ErrLeaderNotAvailable) ||
		errors.Is(err, sarama.ErrNotEnoughReplicas) ||
		errors.Is(err, sarama.ErrNotEnoughReplicasAfterAppend) ||
		errors.Is(err, sarama.ErrOffsetsLoadInProgress) ||
		errors.Is(err, sarama.ErrConsumerCoordinatorNotAvailable) ||
		errors.Is(err, sarama.ErrNotCoordinatorForConsumer) ||
		errors.Is(err, sarama.ErrUnknownTopicOrPartition) ||
		errors.Is(err, sarama.ErrRequestTimedOut) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "metadata is out of date") ||
		strings.Contains(msg, "not the leader") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "coordinator load in progress")
}

func requiresTransactionalProducerRecreation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sarama.ErrOutOfOrderSequenceNumber) ||
		errors.Is(err, sarama.ErrUnknownProducerID) ||
		errors.Is(err, sarama.ErrInvalidProducerEpoch) ||
		errors.Is(err, sarama.ErrInvalidProducerIDMapping) {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "out of order sequence") ||
		strings.Contains(msg, "unknown producer id") ||
		strings.Contains(msg, "invalid producer epoch")
}

func (tp *transactionalProducer) Close() error {
	if tp == nil {
		return nil
	}
	var firstErr error
	if tp.producer != nil {
		if err := tp.producer.Close(); err != nil {
			firstErr = err
		}
	}
	if tp.client != nil {
		if err := tp.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
