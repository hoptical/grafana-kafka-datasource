package main

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// transactionalProducer wraps a Sarama transactional sync producer.
type transactionalProducer struct {
	producer sarama.SyncProducer
}

func newTransactionalProducer(brokers []string, transactionalID string) (*transactionalProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Producer.Transaction.ID = transactionalID
	cfg.Net.MaxOpenRequests = 1
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create transactional producer: %w", err)
	}
	return &transactionalProducer{producer: producer}, nil
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

func (tp *transactionalProducer) Close() error {
	if tp == nil || tp.producer == nil {
		return nil
	}
	return tp.producer.Close()
}
