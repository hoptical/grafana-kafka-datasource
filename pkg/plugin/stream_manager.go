package plugin

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// StreamManager handles the streaming logic for Kafka messages.
type StreamManager struct {
	client KafkaClientAPI
}

// NewStreamManager creates a new StreamManager instance.
func NewStreamManager(client KafkaClientAPI) *StreamManager {
	return &StreamManager{client: client}
}

// decodeAvroMessage decodes an Avro message using the appropriate schema
func (sm *StreamManager) decodeAvroMessage(data []byte, qm queryModel) (interface{}, error) {
	var schema string
	var err error

	if qm.AvroSchemaSource == "inlineSchema" && qm.AvroSchema != "" {
		// Use inline schema
		schema = qm.AvroSchema
	} else {
		// Use Schema Registry
		schemaRegistryUrl := sm.client.GetSchemaRegistryUrl()
		if schemaRegistryUrl == "" {
			return nil, fmt.Errorf("Schema Registry URL not configured")
		}

		subject := kafka_client.GetSubjectName(qm.Topic, sm.client.GetAvroSubjectNamingStrategy())
		schemaClient := kafka_client.NewSchemaRegistryClient(
			schemaRegistryUrl,
			sm.client.GetSchemaRegistryUsername(),
			sm.client.GetSchemaRegistryPassword(),
		)
		schema, err = schemaClient.GetLatestSchema(subject)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema from registry: %w", err)
		}
	}

	// Decode the Avro message
	return kafka_client.DecodeAvroMessage(data, schema)
}

// ProcessMessage converts a Kafka message into a Grafana data frame.
func (sm *StreamManager) ProcessMessage(
	msg kafka_client.KafkaMessage,
	partition int32,
	partitions []int32,
	qm queryModel,
) (*data.Frame, error) {
	// Check if message needs Avro decoding
	messageValue := msg.Value
	if qm.MessageFormat == "avro" && msg.Value == nil && len(msg.RawValue) > 0 {
		// Try to decode as Avro
		decoded, err := sm.decodeAvroMessage(msg.RawValue, qm)
		if err != nil {
			log.DefaultLogger.Warn("Failed to decode Avro message, falling back to raw bytes", "error", err)
			// Fall back to treating raw bytes as string
			messageValue = string(msg.RawValue)
		} else {
			messageValue = decoded
		}
	}

	frame := data.NewFrame("response")

	// Add time field
	frame.Fields = append(frame.Fields, data.NewField("time", nil, make([]time.Time, 1)))

	var frameTime time.Time
	if qm.TimestampMode == "now" {
		frameTime = time.Now()
	} else {
		frameTime = msg.Timestamp
	}
	frame.Fields[0].Set(0, frameTime)

	// Add partition field when consuming from multiple partitions
	if len(partitions) > 1 {
		frame.Fields = append(frame.Fields, data.NewField("partition", nil, make([]int32, 1)))
		frame.Fields[1].Set(0, partition)
	}

	// Add offset field
	offsetFieldIndex := len(frame.Fields)
	frame.Fields = append(frame.Fields, data.NewField("offset", nil, make([]int64, 1)))
	frame.Fields[offsetFieldIndex].Set(0, msg.Offset)

	// Flatten and process message values
	flat := make(map[string]interface{})

	// Handle top-level arrays by wrapping them in an object
	if arr, ok := messageValue.([]interface{}); ok {
		// For top-level arrays, create indexed keys for each element
		wrappedValue := make(map[string]interface{})
		for i, element := range arr {
			key := fmt.Sprintf("item_%d", i)
			wrappedValue[key] = element
		}
		messageValue = wrappedValue
	}

	FlattenJSON("", messageValue, flat, 0, defaultFlattenMaxDepth, defaultFlattenFieldCap)

	fieldBuilder := NewFieldBuilder()
	fieldIndex := len(frame.Fields)

	// Collect keys and sort them for deterministic field ordering
	keys := make([]string, 0, len(flat))
	for key := range flat {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := flat[key]
		fieldBuilder.AddValueToFrame(frame, key, value, fieldIndex)
		fieldIndex++
	}

	return frame, nil
}

// StartPartitionReaders starts goroutines to read from each partition and sends messages to the channel.
func (sm *StreamManager) StartPartitionReaders(
	ctx context.Context,
	partitions []int32,
	qm queryModel,
	messagesCh chan<- messageWithPartition,
) {
	for _, partition := range partitions {
		go sm.readFromPartition(ctx, partition, qm, messagesCh)
	}
}

// readFromPartition reads messages from a single partition.
func (sm *StreamManager) readFromPartition(
	ctx context.Context,
	partition int32,
	qm queryModel,
	messagesCh chan<- messageWithPartition,
) {
	reader, err := sm.client.NewStreamReader(ctx, qm.Topic, partition, qm.AutoOffsetReset, qm.LastN)
	if err != nil {
		log.DefaultLogger.Error("Failed to create stream reader", "topic", qm.Topic, "partition", partition, "error", err)
		return
	}
	if reader == nil {
		log.DefaultLogger.Error("Stream reader is nil", "topic", qm.Topic, "partition", partition)
		return
	}
	defer reader.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := sm.client.ConsumerPull(ctx, reader)
			if err != nil {
				log.DefaultLogger.Error("Error reading from partition", "partition", partition, "error", err)
				continue
			}

			select {
			case messagesCh <- messageWithPartition{msg: msg, partition: partition}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// ValidateAndGetPartitions validates the query and returns the list of partitions to consume from.
func (sm *StreamManager) ValidateAndGetPartitions(ctx context.Context, qm queryModel) ([]int32, error) {
	switch v := qm.Partition.(type) {
	case float64: // JSON numbers are parsed as float64
		// Validate topic exists and selected partition is within range
		if v != math.Trunc(v) {
			return nil, fmt.Errorf("partition must be an integer, got %v", v)
		}
		allPartitions, err := sm.client.GetTopicPartitions(ctx, qm.Topic)
		if err != nil {
			return nil, sm.handleTopicError(err, qm.Topic)
		}
		sel := int32(v)
		count := int32(len(allPartitions))
		if sel < 0 || sel >= count {
			return nil, fmt.Errorf("partition %d out of range [0..%d) for topic %s", sel, count, qm.Topic)
		}
		return []int32{sel}, nil
	case string:
		if v == "all" {
			// Get all partitions for the topic
			allPartitions, err := sm.client.GetTopicPartitions(ctx, qm.Topic)
			if err != nil {
				return nil, sm.handleTopicError(err, qm.Topic)
			}
			return allPartitions, nil
		} else {
			return nil, fmt.Errorf("invalid partition value: %s", v)
		}
	default:
		return nil, fmt.Errorf("invalid partition type: %T", v)
	}
}

// handleTopicError handles errors when fetching topic information.
func (sm *StreamManager) handleTopicError(err error, topicName string) error {
	if errors.Is(err, kafka_client.ErrTopicNotFound) {
		return fmt.Errorf("topic %s %w", topicName, kafka_client.ErrTopicNotFound)
	}
	return fmt.Errorf("failed to get topic partitions: %w", err)
}
