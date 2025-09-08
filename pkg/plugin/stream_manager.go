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

// ProcessMessageToFrame converts a Kafka message into a Grafana data frame.
// This is a shared function that can be used by both streaming and data query handlers.
func ProcessMessageToFrame(client KafkaClientAPI, msg kafka_client.KafkaMessage, partition int32, partitions []int32, qm queryModel) (*data.Frame, error) {
	log.DefaultLogger.Debug("Processing message",
		"partition", partition,
		"offset", msg.Offset,
		"messageFormat", qm.MessageFormat,
		"rawValueLength", len(msg.RawValue),
		"hasParsedValue", msg.Value != nil,
		"hasError", msg.Error != nil)

	// If there's an error in the message, create a frame with error information
	if msg.Error != nil {
		log.DefaultLogger.Warn("Processing message with error",
			"partition", partition,
			"offset", msg.Offset,
			"error", msg.Error)

		frame := data.NewFrame("response")

		// Add time field
		frame.Fields = append(frame.Fields, data.NewField("time", nil, make([]time.Time, 1)))
		frame.Fields[0].Set(0, msg.Timestamp)

		// Add partition field when consuming from multiple partitions
		if len(partitions) > 1 {
			frame.Fields = append(frame.Fields, data.NewField("partition", nil, make([]int32, 1)))
			frame.Fields[1].Set(0, partition)
		}

		// Add offset field
		offsetFieldIndex := len(frame.Fields)
		frame.Fields = append(frame.Fields, data.NewField("offset", nil, make([]int64, 1)))
		frame.Fields[offsetFieldIndex].Set(0, msg.Offset)

		// Add error field
		errorFieldIndex := len(frame.Fields)
		frame.Fields = append(frame.Fields, data.NewField("error", nil, make([]string, 1)))
		frame.Fields[errorFieldIndex].Set(0, msg.Error.Error())

		return frame, nil
	}

	// Check if message needs Avro decoding
	messageValue := msg.Value
	if qm.MessageFormat == "avro" && msg.Value == nil && len(msg.RawValue) > 0 {
		log.DefaultLogger.Info("Attempting Avro decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue),
			"topic", qm.Topic,
			"avroSchemaSource", qm.AvroSchemaSource)

		// Try to decode as Avro
		decoded, err := decodeAvroMessage(client, msg.RawValue, qm)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Avro message, falling back to raw bytes",
				"error", err,
				"rawValueLength", len(msg.RawValue),
				"partition", partition,
				"offset", msg.Offset)
			// Fall back to treating raw bytes as string
			messageValue = string(msg.RawValue)
		} else {
			log.DefaultLogger.Info("Avro decoding successful",
				"partition", partition,
				"offset", msg.Offset,
				"decodedType", fmt.Sprintf("%T", decoded))
			messageValue = decoded
		}
	} else if qm.MessageFormat == "avro" {
		log.DefaultLogger.Debug("Avro format specified but message already parsed or no raw data",
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	} else {
		log.DefaultLogger.Debug("Using non-Avro message format",
			"messageFormat", qm.MessageFormat,
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
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

	log.DefaultLogger.Debug("Processing message value for flattening",
		"messageValueType", fmt.Sprintf("%T", messageValue),
		"isMap", fmt.Sprintf("%t", func() bool {
			_, ok := messageValue.(map[string]interface{})
			return ok
		}()))

	FlattenJSON("", messageValue, flat, 0, defaultFlattenMaxDepth, defaultFlattenFieldCap)

	log.DefaultLogger.Debug("Flattened message data",
		"flattenedFieldCount", len(flat),
		"flattenedKeys", func() []string {
			keys := make([]string, 0, len(flat))
			for k := range flat {
				keys = append(keys, k)
			}
			return keys
		}())

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

// decodeAvroMessage decodes an Avro message using the appropriate schema
func decodeAvroMessage(client KafkaClientAPI, data []byte, qm queryModel) (interface{}, error) {
	log.DefaultLogger.Info("Starting Avro message decoding",
		"dataLength", len(data),
		"topic", qm.Topic,
		"avroSchemaSource", qm.AvroSchemaSource,
		"avroSchemaLength", len(qm.AvroSchema),
		"partition", qm.Partition)

	var schema string
	var err error

	if qm.AvroSchemaSource == "inlineSchema" && qm.AvroSchema != "" {
		// Use inline schema
		schema = qm.AvroSchema
		log.DefaultLogger.Info("Using inline Avro schema",
			"schemaLength", len(schema),
			"schemaPreview", func() string {
				if len(schema) > 100 {
					return schema[:100] + "..."
				}
				return schema
			}())
	} else {
		log.DefaultLogger.Warn("Falling back to Schema Registry",
			"avroSchemaSource", qm.AvroSchemaSource,
			"avroSchemaEmpty", qm.AvroSchema == "",
			"reason", "Either avroSchemaSource is not 'inlineSchema' or avroSchema is empty")
		// Use Schema Registry
		schemaRegistryUrl := client.GetSchemaRegistryUrl()
		log.DefaultLogger.Debug("Attempting to get schema from registry",
			"schemaRegistryUrl", schemaRegistryUrl,
			"hasUsername", client.GetSchemaRegistryUsername() != "",
			"hasPassword", client.GetSchemaRegistryPassword() != "",
			"topic", qm.Topic)

		if schemaRegistryUrl == "" {
			log.DefaultLogger.Error("Schema Registry URL not configured")
			return nil, fmt.Errorf("schema registry URL not configured")
		}

		subject := kafka_client.GetSubjectName(qm.Topic, client.GetAvroSubjectNamingStrategy())
		log.DefaultLogger.Debug("Generated subject name",
			"subject", subject,
			"strategy", client.GetAvroSubjectNamingStrategy())

		schemaClient := kafka_client.NewSchemaRegistryClient(
			schemaRegistryUrl,
			client.GetSchemaRegistryUsername(),
			client.GetSchemaRegistryPassword(),
		)

		log.DefaultLogger.Debug("Fetching latest schema from registry", "subject", subject)
		schema, err = schemaClient.GetLatestSchema(subject)
		if err != nil {
			log.DefaultLogger.Error("Failed to get schema from registry",
				"subject", subject,
				"error", err)
			return nil, fmt.Errorf("failed to get schema from registry: %w", err)
		}
		log.DefaultLogger.Debug("Successfully retrieved schema from registry",
			"subject", subject,
			"schemaLength", len(schema))
	}

	// Decode the Avro message
	log.DefaultLogger.Debug("Decoding Avro message with schema")
	decoded, err := kafka_client.DecodeAvroMessage(data, schema)
	if err != nil {
		log.DefaultLogger.Error("Avro decoding failed", "error", err)
		return nil, err
	}

	log.DefaultLogger.Debug("Avro message decoded successfully", "decodedType", fmt.Sprintf("%T", decoded))
	return decoded, nil
}

// ProcessMessage converts a Kafka message into a Grafana data frame.
func (sm *StreamManager) ProcessMessage(
	msg kafka_client.KafkaMessage,
	partition int32,
	partitions []int32,
	qm queryModel,
) (*data.Frame, error) {
	return ProcessMessageToFrame(sm.client, msg, partition, partitions, qm)
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
	log.DefaultLogger.Info("Starting partition reader",
		"topic", qm.Topic,
		"partition", partition,
		"autoOffsetReset", qm.AutoOffsetReset,
		"lastN", qm.LastN)

	reader, err := sm.client.NewStreamReader(ctx, qm.Topic, partition, qm.AutoOffsetReset, qm.LastN)
	if err != nil {
		log.DefaultLogger.Error("Failed to create stream reader", "topic", qm.Topic, "partition", partition, "error", err)
		return
	}
	if reader == nil {
		log.DefaultLogger.Error("Stream reader is nil", "topic", qm.Topic, "partition", partition)
		return
	}
	defer func() {
		if reader != nil {
			reader.Close()
		}
	}()

	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Info("Partition reader stopping",
				"partition", partition,
				"totalMessages", messageCount)
			return
		default:
			log.DefaultLogger.Debug("Attempting to read message from partition", "partition", partition)
			msg, err := sm.client.ConsumerPull(ctx, reader, qm.MessageFormat)
			if err != nil {
				log.DefaultLogger.Error("Error reading from partition",
					"partition", partition,
					"error", err)
				continue
			}

			messageCount++
			log.DefaultLogger.Debug("Successfully read message from partition",
				"partition", partition,
				"messageCount", messageCount,
				"offset", msg.Offset,
				"hasParsedValue", msg.Value != nil,
				"rawValueLength", len(msg.RawValue))

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
