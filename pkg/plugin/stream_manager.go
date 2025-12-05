package plugin

import (
	"bytes"
	"context"
	"encoding/json"
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
	client          KafkaClientAPI
	flattenMaxDepth int
	flattenFieldCap int
	fieldBuilder    *FieldBuilder // Maintains type registry across messages
}

// createErrorFrame creates a data frame containing error information
func createErrorFrame(msg kafka_client.KafkaMessage, partition int32, partitions []int32, err error) (*data.Frame, error) {
	log.DefaultLogger.Warn("Creating error frame for message",
		"partition", partition,
		"offset", msg.Offset,
		"error", err)

	frame := data.NewFrame("response")

	// Add time field
	frame.Fields = append(frame.Fields, data.NewField("time", nil, make([]time.Time, 1)))
	frame.Fields[0].Set(0, msg.Timestamp)

	// Add partition field
	frame.Fields = append(frame.Fields, data.NewField("partition", nil, make([]int32, 1)))
	frame.Fields[1].Set(0, partition)

	// Add offset field
	offsetFieldIndex := len(frame.Fields)
	frame.Fields = append(frame.Fields, data.NewField("offset", nil, make([]int64, 1)))
	frame.Fields[offsetFieldIndex].Set(0, msg.Offset)

	// Add error field
	errorFieldIndex := len(frame.Fields)
	frame.Fields = append(frame.Fields, data.NewField("error", nil, make([]string, 1)))
	frame.Fields[errorFieldIndex].Set(0, err.Error())

	return frame, nil
}

// StreamConfig holds the configuration for streaming that can be updated dynamically.
type StreamConfig struct {
	MessageFormat    string
	AvroSchemaSource string
	AvroSchema       string
	AutoOffsetReset  string
	TimestampMode    string
	LastN            int32 // Added to track lastN changes
}

// NewStreamManager creates a new StreamManager instance.
func NewStreamManager(client KafkaClientAPI, flattenMaxDepth, flattenFieldCap int) *StreamManager {
	return &StreamManager{
		client:          client,
		flattenMaxDepth: flattenMaxDepth,
		flattenFieldCap: flattenFieldCap,
		fieldBuilder:    NewFieldBuilder(),
	}
}

// UpdateStreamConfig updates the streaming configuration dynamically.
func (sm *StreamManager) UpdateStreamConfig(config *StreamConfig, newMessageFormat string) {
	config.MessageFormat = newMessageFormat
	log.DefaultLogger.Info("Updated stream message format", "newFormat", newMessageFormat)
}

// ProcessMessageToFrame converts a Kafka message into a Grafana data frame.
// This is a shared function that can be used by both streaming and data query handlers.
func ProcessMessageToFrame(client KafkaClientAPI, msg kafka_client.KafkaMessage, partition int32, partitions []int32, config *StreamConfig, topic string) (*data.Frame, error) {
	log.DefaultLogger.Debug("Processing message",
		"partition", partition,
		"offset", msg.Offset,
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

		// Add partition field
		frame.Fields = append(frame.Fields, data.NewField("partition", nil, make([]int32, 1)))
		frame.Fields[1].Set(0, partition)

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
	log.DefaultLogger.Debug("Processing message with configuration",
		"partition", partition,
		"offset", msg.Offset,
		"configMessageFormat", config.MessageFormat,
		"configAvroSchemaSource", config.AvroSchemaSource)

	messageValue := msg.Value
	if config.MessageFormat == "avro" && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting Avro decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue),
			"topic", topic,
			"avroSchemaSource", config.AvroSchemaSource)

		// Try to decode as Avro
		decoded, err := decodeAvroMessage(client, msg.RawValue, config, topic)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Avro message",
				"error", err,
				"rawValueLength", len(msg.RawValue),
				"partition", partition,
				"offset", msg.Offset)
			// Return error frame instead of falling back to raw bytes
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("avro decoding failed: %w", err))
		} else {
			log.DefaultLogger.Debug("Avro decoding successful",
				"partition", partition,
				"offset", msg.Offset,
				"decodedType", fmt.Sprintf("%T", decoded))
			messageValue = decoded
		}
	} else if config.MessageFormat == "avro" {
		log.DefaultLogger.Debug("Avro format specified but no raw data available",
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	} else if config.MessageFormat == "json" && msg.Value == nil && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting JSON decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue))

		// Try to decode as JSON since the consumer didn't parse it
		var v interface{}
		dec := json.NewDecoder(bytes.NewReader(msg.RawValue))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			log.DefaultLogger.Error("Failed to decode JSON message",
				"error", err,
				"rawValueLength", len(msg.RawValue),
				"partition", partition,
				"offset", msg.Offset)
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("json decoding failed: %w", err))
		} else {
			// Accept both objects and arrays at the top level
			switch v := v.(type) {
			case map[string]interface{}, []interface{}:
				log.DefaultLogger.Debug("JSON decoding successful",
					"partition", partition,
					"offset", msg.Offset,
					"decodedType", fmt.Sprintf("%T", v))
				messageValue = v
			default:
				log.DefaultLogger.Error("JSON decoded but not object/array",
					"partition", partition,
					"offset", msg.Offset,
					"decodedType", fmt.Sprintf("%T", v))
				return createErrorFrame(msg, partition, partitions, fmt.Errorf("decoded JSON is not a valid object or array: %T", v))
			}
		}
	} else {
		log.DefaultLogger.Debug("Using pre-decoded message value or non-Avro format",
			"partition", partition,
			"offset", msg.Offset,
			"configMessageFormat", config.MessageFormat,
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	}

	frame := data.NewFrame("response")

	// Add time field
	timeField := data.NewField("time", nil, make([]time.Time, 1))
	var frameTime time.Time
	if config.TimestampMode == "now" {
		frameTime = time.Now()
	} else {
		frameTime = msg.Timestamp
	}
	timeField.Set(0, frameTime)

	fields := []*data.Field{timeField}

	// Add partition field when consuming from multiple partitions
	if len(partitions) > 1 {
		partitionField := data.NewField("partition", nil, make([]int32, 1))
		partitionField.Set(0, partition)
		fields = append(fields, partitionField)
	}

	// Add offset field
	offsetField := data.NewField("offset", nil, make([]int64, 1))
	offsetField.Set(0, msg.Offset)
	fields = append(fields, offsetField)

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

	// Collect keys and sort them for deterministic field ordering
	keys := make([]string, 0, len(flat))
	for key := range flat {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Pre-allocate fields slice
	msgFieldStart := len(fields)
	totalFields := len(fields) + len(keys)
	frame.Fields = make([]*data.Field, totalFields)
	copy(frame.Fields, fields)

	fieldBuilder := NewFieldBuilder()

	// Add message fields by direct assignment
	for i, key := range keys {
		value := flat[key]
		fieldIdx := msgFieldStart + i
		fieldBuilder.AddValueToFrame(frame, key, value, fieldIdx)
	}

	return frame, nil
}

// decodeAvroMessage decodes an Avro message using the appropriate schema
func decodeAvroMessage(client KafkaClientAPI, data []byte, config *StreamConfig, topic string) (interface{}, error) {
	log.DefaultLogger.Debug("Starting Avro message decoding",
		"dataLength", len(data),
		"topic", topic,
		"avroSchemaSource", config.AvroSchemaSource,
		"avroSchemaLength", len(config.AvroSchema),
		"partition", "unknown") // Note: partition not available here

	// Add detailed debugging for configuration
	log.DefaultLogger.Debug("Detailed Avro config debugging",
		"AvroSchemaSource", config.AvroSchemaSource,
		"AvroSchemaSourceType", fmt.Sprintf("%T", config.AvroSchemaSource),
		"AvroSchemaEmpty", config.AvroSchema == "",
		"AvroSchemaSourceEquals", config.AvroSchemaSource == "inlineSchema")

	var schema string
	var err error

	if config.AvroSchemaSource == "inlineSchema" && config.AvroSchema != "" {
		// Use inline schema
		schema = config.AvroSchema
		log.DefaultLogger.Debug("Using inline Avro schema",
			"schemaLength", len(schema),
			"schemaPreview", func() string {
				if len(schema) > 100 {
					return schema[:100] + "..."
				}
				return schema
			}())
	} else if config.AvroSchemaSource == "inlineSchema" && config.AvroSchema == "" {
		// Inline schema was selected but no schema provided - this is an error
		log.DefaultLogger.Error("Inline Avro schema selected but no schema provided")
		return nil, fmt.Errorf("inline Avro schema selected but no schema provided - please provide a valid Avro schema")
	} else {
		log.DefaultLogger.Debug("Using Schema Registry for Avro schema",
			"avroSchemaSource", config.AvroSchemaSource,
			"schemaRegistryConfigured", client.GetSchemaRegistryUrl() != "")
		// Use Schema Registry
		schemaRegistryUrl := client.GetSchemaRegistryUrl()
		log.DefaultLogger.Debug("Attempting to get schema from registry",
			"schemaRegistryUrl", schemaRegistryUrl,
			"hasUsername", client.GetSchemaRegistryUsername() != "",
			"hasPassword", client.GetSchemaRegistryPassword() != "",
			"topic", topic)

		if schemaRegistryUrl == "" {
			log.DefaultLogger.Error("Schema Registry URL not configured")
			return nil, fmt.Errorf("schema registry URL not configured")
		}

		subject := kafka_client.GetSubjectName(topic, client.GetAvroSubjectNamingStrategy()) // Note: using actual topic
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
	config *StreamConfig,
	topic string,
) (*data.Frame, error) {
	return ProcessMessageToFrame(sm.client, msg, partition, partitions, config, topic)
}

// StartPartitionReaders starts goroutines to read from each partition and sends messages to the channel.
func (sm *StreamManager) StartPartitionReaders(
	ctx context.Context,
	partitions []int32,
	qm queryModel,
	config *StreamConfig,
	messagesCh chan<- messageWithPartition,
) {
	for _, partition := range partitions {
		go sm.readFromPartition(ctx, partition, qm, config, messagesCh)
	}
}

// readFromPartition reads messages from a single partition.
func (sm *StreamManager) readFromPartition(
	ctx context.Context,
	partition int32,
	qm queryModel,
	config *StreamConfig,
	messagesCh chan<- messageWithPartition,
) {
	log.DefaultLogger.Info("Starting partition reader",
		"topic", qm.Topic,
		"partition", partition,
		"autoOffsetReset", config.AutoOffsetReset,
		"lastN", qm.LastN)

	reader, err := sm.client.NewStreamReader(ctx, qm.Topic, partition, config.AutoOffsetReset, qm.LastN)
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

			// Add a timeout context to prevent infinite blocking
			msgCtx, msgCancel := context.WithTimeout(ctx, messageReadTimeout)
			msg, err := sm.client.ConsumerPull(msgCtx, reader, config.MessageFormat)
			msgCancel()

			if err != nil {
				log.DefaultLogger.Error("Error reading from partition",
					"partition", partition,
					"error", err)

				// Check if it's a timeout error - if so, continue to next iteration
				// to prevent stream from freezing
				if errors.Is(err, context.DeadlineExceeded) {
					log.DefaultLogger.Debug("Read timeout on partition, continuing to next iteration",
						"partition", partition)
					// Brief pause before retrying
					select {
					case <-time.After(retryDelayAfterError):
					case <-ctx.Done():
						return
					}
					continue
				}

				// Create an error message to send to the frontend
				errorMsg := kafka_client.KafkaMessage{
					Offset:    -1, // Use -1 to indicate this is an error message
					Timestamp: time.Now(),
					Error:     err,
				}

				select {
				case messagesCh <- messageWithPartition{msg: errorMsg, partition: partition}:
					log.DefaultLogger.Info("Sent error message to channel", "partition", partition, "error", err)
				case <-ctx.Done():
					return
				}
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
	log.DefaultLogger.Info("ValidateAndGetPartitions called", "topic", qm.Topic, "partition", qm.Partition, "partitionType", fmt.Sprintf("%T", qm.Partition))
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
		log.DefaultLogger.Info("Available partitions", "topic", qm.Topic, "partitions", allPartitions)
		sel := int32(v)
		count := int32(len(allPartitions))
		log.DefaultLogger.Info("Partition validation", "selected", sel, "count", count, "validRange", fmt.Sprintf("[0..%d)", count))
		if sel < 0 || sel >= count {
			return nil, fmt.Errorf("partition %d out of range [0..%d) for topic %s", sel, count, qm.Topic)
		}
		result := []int32{sel}
		log.DefaultLogger.Info("Returning partitions for single partition", "partitions", result)
		return result, nil
	case string:
		if v == "all" {
			// Get all partitions for the topic
			allPartitions, err := sm.client.GetTopicPartitions(ctx, qm.Topic)
			if err != nil {
				return nil, sm.handleTopicError(err, qm.Topic)
			}
			log.DefaultLogger.Info("Returning all partitions", "partitions", allPartitions)
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
