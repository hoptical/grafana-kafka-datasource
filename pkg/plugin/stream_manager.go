package plugin

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// StreamManager handles the streaming logic for Kafka messages.
type StreamManager struct {
	client               KafkaClientAPI
	flattenMaxDepth      int
	flattenFieldCap      int
	fieldBuilder         *FieldBuilder                      // Maintains type registry across messages
	schemaCache          map[string]string                  // Cache for schemas by subject name
	schemaRegistryClient *kafka_client.SchemaRegistryClient // Cached Schema Registry client
	mu                   sync.RWMutex
}

// createErrorFrame creates a data frame containing error information
func createErrorFrame(msg kafka_client.KafkaMessage, partition int32, partitions []int32, err error, config *StreamConfig, topic string) (*data.Frame, error) {
	log.DefaultLogger.Warn("Creating error frame for message",
		"partition", partition,
		"offset", msg.Offset,
		"error", err)

	frame := data.NewFrame("response")

	if config != nil {
		if config.RefID != "" {
			frame.RefID = config.RefID
		}
		if config.Alias != "" {
			frame.Name = formatAlias(config.Alias, config, topic, partition, "")
		}
	}

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

	// Apply alias to fields if configured, skipping time field (consistent with ProcessMessage)
	if config != nil && config.Alias != "" {
		for _, field := range frame.Fields {
			if field.Name == "time" {
				continue
			}
			if field.Config == nil {
				field.Config = &data.FieldConfig{}
			}
			formatted := formatAlias(config.Alias, config, topic, partition, field.Name)
			field.Config.DisplayNameFromDS = formatted
		}
	}

	return frame, nil
}

// StreamConfig holds the configuration for streaming that can be updated dynamically.
type StreamConfig struct {
	MessageFormat        string
	AvroSchemaSource     string
	AvroSchema           string
	ProtobufSchemaSource string
	ProtobufSchema       string
	AutoOffsetReset      string
	TimestampMode        string
	LastN                int32 // Added to track lastN changes
	RefID                string
	Alias                string
}

// NewStreamManager creates a new StreamManager instance.
func NewStreamManager(client KafkaClientAPI, flattenMaxDepth, flattenFieldCap int) *StreamManager {
	return &StreamManager{
		client:          client,
		flattenMaxDepth: flattenMaxDepth,
		flattenFieldCap: flattenFieldCap,
		fieldBuilder:    NewFieldBuilder(),
		schemaCache:     make(map[string]string),
	}
}

// ProcessMessageToFrame converts a Kafka message into a Grafana data frame.
// This is a shared function that can be used by both streaming and data query handlers.
// Note: fieldBuilder should be a persistent instance (e.g., from StreamManager) to maintain
// type registry across messages for proper null value handling.
func ProcessMessageToFrame(client KafkaClientAPI, msg kafka_client.KafkaMessage, partition int32, partitions []int32, config *StreamConfig, topic string, flattenMaxDepth int, flattenFieldCap int, fieldBuilder *FieldBuilder) (*data.Frame, error) {
	log.DefaultLogger.Debug("Processing message",
		"partition", partition,
		"offset", msg.Offset,
		"rawValueLength", len(msg.RawValue),
		"hasParsedValue", msg.Value != nil,
		"hasError", msg.Error != nil,
		"flattenMaxDepth", flattenMaxDepth,
		"flattenFieldCap", flattenFieldCap)

	// If there's an error in the message, create a frame with error information
	if msg.Error != nil {
		return createErrorFrame(msg, partition, partitions, msg.Error, config, topic)
	}

	// Check if message needs Avro/Protobuf decoding
	messageFormat := config.MessageFormat
	avroSchemaSource := config.AvroSchemaSource
	protobufSchemaSource := config.ProtobufSchemaSource

	log.DefaultLogger.Debug("Processing message with configuration",
		"partition", partition,
		"offset", msg.Offset,
		"configMessageFormat", messageFormat,
		"configAvroSchemaSource", avroSchemaSource,
		"configProtobufSchemaSource", protobufSchemaSource)

	messageValue := msg.Value
	if messageFormat == "avro" && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting Avro decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue),
			"topic", topic,
			"avroSchemaSource", avroSchemaSource)

		// Try to decode as Avro
		// Note: nil StreamManager means no caching available in this context
		decoded, err := decodeAvroMessage(nil, client, msg.RawValue, config, topic)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Avro message",
				"error", err,
				"rawValueLength", len(msg.RawValue),
				"partition", partition,
				"offset", msg.Offset)
			// Return error frame instead of falling back to raw bytes
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("avro decoding failed: %w", err), config, topic)
		} else {
			log.DefaultLogger.Debug("Avro decoding successful",
				"partition", partition,
				"offset", msg.Offset,
				"decodedType", fmt.Sprintf("%T", decoded))
			messageValue = decoded
		}
	} else if messageFormat == "protobuf" && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting Protobuf decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue),
			"topic", topic,
			"protobufSchemaSource", protobufSchemaSource)

		decoded, err := decodeProtobufMessage(nil, client, msg.RawValue, config, topic)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Protobuf message",
				"error", err,
				"rawValueLength", len(msg.RawValue),
				"partition", partition,
				"offset", msg.Offset)
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("protobuf decoding failed: %w", err), config, topic)
		}
		log.DefaultLogger.Debug("Protobuf decoding successful",
			"partition", partition,
			"offset", msg.Offset,
			"decodedType", fmt.Sprintf("%T", decoded))
		messageValue = decoded
	} else if messageFormat == "avro" {
		log.DefaultLogger.Debug("Avro format specified but no raw data available",
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	} else if messageFormat == "protobuf" {
		log.DefaultLogger.Debug("Protobuf format specified but no raw data available",
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	} else if messageFormat == "json" && msg.Value == nil && len(msg.RawValue) > 0 {
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
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("json decoding failed: %w", err), config, topic)
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
				return createErrorFrame(msg, partition, partitions, fmt.Errorf("decoded JSON is not a valid object or array: %T", v), config, topic)
			}
		}
	} else {
		messageFormat := config.MessageFormat
		log.DefaultLogger.Debug("Using pre-decoded message value or non-Avro format",
			"partition", partition,
			"offset", msg.Offset,
			"configMessageFormat", messageFormat,
			"hasParsedValue", msg.Value != nil,
			"rawValueLength", len(msg.RawValue))
	}

	frame := data.NewFrame("response")

	// Add time field
	timeField := data.NewField("time", nil, make([]time.Time, 1))
	timestampMode := config.TimestampMode
	var frameTime time.Time
	if timestampMode == "now" {
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

	// Unwrap Avro union types before flattening (for Avro messages)
	// This must happen BEFORE flattening to avoid creating keys like "value1.double"
	isAvro := config.MessageFormat == "avro"

	if isAvro {
		messageValue = UnwrapAvroUnions(messageValue)
	}

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

	// Use passed-in flatten configuration parameters
	FlattenJSON("", messageValue, flat, 0, flattenMaxDepth, flattenFieldCap)

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

	// Use the passed-in fieldBuilder to maintain type registry across messages
	// This is critical for proper null value handling in Avro/Protobuf messages

	// Add message fields by direct assignment
	for i, key := range keys {
		value := flat[key]
		fieldIdx := msgFieldStart + i
		fieldBuilder.AddValueToFrame(frame, key, value, fieldIdx)
	}

	return frame, nil
}

// decodeAvroMessage decodes an Avro message using the appropriate schema
// If sm is provided, it uses cached Schema Registry client and schema cache for better performance
func decodeAvroMessage(sm *StreamManager, client KafkaClientAPI, data []byte, config *StreamConfig, topic string) (interface{}, error) {
	avroSchemaSource := config.AvroSchemaSource
	avroSchema := config.AvroSchema

	log.DefaultLogger.Debug("Starting Avro message decoding",
		"dataLength", len(data),
		"topic", topic,
		"avroSchemaSource", avroSchemaSource,
		"avroSchemaLength", len(avroSchema),
		"partition", "unknown") // Note: partition not available here

	// Add detailed debugging for configuration
	log.DefaultLogger.Debug("Detailed Avro config debugging",
		"AvroSchemaSource", avroSchemaSource,
		"AvroSchemaSourceType", fmt.Sprintf("%T", avroSchemaSource),
		"AvroSchemaEmpty", avroSchema == "",
		"AvroSchemaSourceEquals", avroSchemaSource == "inlineSchema")

	var schema string
	var err error

	if avroSchemaSource == "inlineSchema" && avroSchema != "" {
		// Use inline schema
		schema = avroSchema
		log.DefaultLogger.Debug("Using inline Avro schema",
			"schemaLength", len(schema),
			"schemaPreview", func() string {
				if len(schema) > 100 {
					return schema[:100] + "..."
				}
				return schema
			}())
	} else if avroSchemaSource == "inlineSchema" && avroSchema == "" {
		// Inline schema was selected but no schema provided - this is an error
		log.DefaultLogger.Error("Inline Avro schema selected but no schema provided")
		return nil, fmt.Errorf("inline Avro schema selected but no schema provided - please provide a valid Avro schema")
	} else {
		log.DefaultLogger.Debug("Using Schema Registry for Avro schema",
			"avroSchemaSource", avroSchemaSource,
			"schemaRegistryConfigured", client.GetSchemaRegistryUrl() != "")
		// Use Schema Registry
		schemaRegistryUrl := client.GetSchemaRegistryUrl()

		// Redact credentials from URL for logging
		redactedUrl := schemaRegistryUrl
		if parsedUrl, err := url.Parse(schemaRegistryUrl); err == nil {
			if parsedUrl.User != nil {
				// Remove user info from URL for safe logging
				parsedUrl.User = nil
				redactedUrl = parsedUrl.String()
			}
		}

		log.DefaultLogger.Debug("Attempting to get schema from registry",
			"schemaRegistryUrl", redactedUrl,
			"hasUsername", client.GetSchemaRegistryUsername() != "",
			"hasPassword", client.GetSchemaRegistryPassword() != "",
			"topic", topic)

		if schemaRegistryUrl == "" {
			log.DefaultLogger.Error("Schema Registry URL not configured")
			return nil, fmt.Errorf("schema registry URL not configured")
		}

		subject := kafka_client.GetSubjectName(topic, client.GetSubjectNamingStrategy())
		log.DefaultLogger.Debug("Generated subject name",
			"subject", subject,
			"strategy", client.GetSubjectNamingStrategy())

		// Use cached Schema Registry client and schema if StreamManager is available
		if sm != nil {
			schema, err = sm.getSchemaFromRegistryWithCache("avro", schemaRegistryUrl,
				client.GetSchemaRegistryUsername(),
				client.GetSchemaRegistryPassword(),
				subject)
			if err != nil {
				log.DefaultLogger.Error("Failed to get schema from registry",
					"subject", subject,
					"error", err)
				return nil, fmt.Errorf("failed to get schema from registry: %w", err)
			}
		} else {
			// Fallback: create client per-message if StreamManager not available
			log.DefaultLogger.Debug("StreamManager not available, creating Schema Registry client per-message")

			// Get HTTP client from KafkaClient
			httpClient := client.GetHTTPClient()
			if httpClient == nil {
				log.DefaultLogger.Error("HTTP client not available in KafkaClient")
				return nil, fmt.Errorf("HTTP client not available for Schema Registry")
			}

			schemaClient := kafka_client.NewSchemaRegistryClient(
				schemaRegistryUrl,
				client.GetSchemaRegistryUsername(),
				client.GetSchemaRegistryPassword(),
				httpClient,
			)
			schema, err = schemaClient.GetLatestSchema(subject)
			if err != nil {
				log.DefaultLogger.Error("Failed to get schema from registry",
					"subject", subject,
					"error", err)
				return nil, fmt.Errorf("failed to get schema from registry: %w", err)
			}
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

// decodeProtobufMessage decodes a Protobuf message using the appropriate schema
// If sm is provided, it uses cached Schema Registry client and schema cache for better performance
func decodeProtobufMessage(sm *StreamManager, client KafkaClientAPI, data []byte, config *StreamConfig, topic string) (interface{}, error) {
	protobufSchemaSource := config.ProtobufSchemaSource
	protobufSchema := config.ProtobufSchema

	log.DefaultLogger.Debug("Starting Protobuf message decoding",
		"dataLength", len(data),
		"topic", topic,
		"protobufSchemaSource", protobufSchemaSource,
		"protobufSchemaLength", len(protobufSchema))

	var schema string
	var err error

	if protobufSchemaSource == "inlineSchema" && protobufSchema != "" {
		schema = protobufSchema
		log.DefaultLogger.Debug("Using inline Protobuf schema",
			"schemaLength", len(schema))
	} else if protobufSchemaSource == "inlineSchema" && protobufSchema == "" {
		log.DefaultLogger.Error("Inline Protobuf schema selected but no schema provided")
		return nil, fmt.Errorf("inline Protobuf schema selected but no schema provided - please provide a valid Protobuf schema")
	} else {
		schemaRegistryUrl := client.GetSchemaRegistryUrl()
		if schemaRegistryUrl == "" {
			log.DefaultLogger.Error("Schema Registry URL not configured")
			return nil, fmt.Errorf("schema registry URL not configured")
		}

		// Redact credentials from URL for logging
		redactedUrl := schemaRegistryUrl
		if parsedUrl, err := url.Parse(schemaRegistryUrl); err == nil {
			if parsedUrl.User != nil {
				parsedUrl.User = nil
				redactedUrl = parsedUrl.String()
			}
		}
		log.DefaultLogger.Debug("Using Schema Registry for Protobuf schema",
			"schemaRegistryUrl", redactedUrl,
			"topic", topic)

		// Extract schema ID from Confluent wire format if present
		isConfluentWireFormat := len(data) > 5 && data[0] == 0x00
		if isConfluentWireFormat {
			schemaID := int(binary.BigEndian.Uint32(data[1:5]))
			log.DefaultLogger.Debug("Extracted schema ID from Protobuf wire format", "schemaID", schemaID)
			schema, err = getProtobufSchemaByID(sm, client, schemaRegistryUrl, schemaID)
		} else {
			subject := kafka_client.GetSubjectName(topic, client.GetSubjectNamingStrategy())
			log.DefaultLogger.Debug("Fetching latest Protobuf schema for subject", "subject", subject)
			schema, err = getProtobufSchemaBySubject(sm, client, schemaRegistryUrl, subject)
		}
		if err != nil {
			return nil, err
		}
	}

	decoded, err := kafka_client.DecodeProtobufMessage(data, schema)
	if err != nil {
		return nil, err
	}

	return decoded, nil
}

// getProtobufSchemaByID retrieves a Protobuf schema by ID from Schema Registry
func getProtobufSchemaByID(sm *StreamManager, client KafkaClientAPI, registryUrl string, schemaID int) (string, error) {
	if sm != nil {
		return sm.getSchemaByIDFromRegistryWithCache("protobuf", registryUrl,
			client.GetSchemaRegistryUsername(),
			client.GetSchemaRegistryPassword(),
			schemaID)
	}

	httpClient := client.GetHTTPClient()
	if httpClient == nil {
		return "", fmt.Errorf("HTTP client not available for Schema Registry")
	}
	schemaClient := kafka_client.NewSchemaRegistryClient(
		registryUrl,
		client.GetSchemaRegistryUsername(),
		client.GetSchemaRegistryPassword(),
		httpClient,
	)
	return schemaClient.GetSchemaByID(schemaID)
}

// getProtobufSchemaBySubject retrieves the latest Protobuf schema for a subject from Schema Registry
func getProtobufSchemaBySubject(sm *StreamManager, client KafkaClientAPI, registryUrl string, subject string) (string, error) {
	if sm != nil {
		return sm.getSchemaFromRegistryWithCache("protobuf", registryUrl,
			client.GetSchemaRegistryUsername(),
			client.GetSchemaRegistryPassword(),
			subject)
	}

	httpClient := client.GetHTTPClient()
	if httpClient == nil {
		return "", fmt.Errorf("HTTP client not available for Schema Registry")
	}
	schemaClient := kafka_client.NewSchemaRegistryClient(
		registryUrl,
		client.GetSchemaRegistryUsername(),
		client.GetSchemaRegistryPassword(),
		httpClient,
	)
	return schemaClient.GetLatestSchema(subject)
}

// getSchemaWithCache retrieves a schema from the cache or fetches it from the registry
// This method provides schema caching for better performance in high-throughput scenarios
func (sm *StreamManager) getSchemaWithCache(cacheKey, subject string, schemaClient *kafka_client.SchemaRegistryClient) (string, error) {
	sm.mu.RLock()
	if cachedSchema, exists := sm.schemaCache[cacheKey]; exists {
		sm.mu.RUnlock()
		log.DefaultLogger.Debug("Using cached schema", "subject", subject)
		return cachedSchema, nil
	}
	sm.mu.RUnlock()

	// Fetch from registry
	log.DefaultLogger.Debug("Fetching schema from registry (not in cache)", "subject", subject)
	schema, err := schemaClient.GetLatestSchema(subject)
	if err != nil {
		return "", err
	}

	// Store in cache
	sm.mu.Lock()
	sm.schemaCache[cacheKey] = schema
	sm.mu.Unlock()

	log.DefaultLogger.Debug("Cached schema", "subject", subject, "schemaLength", len(schema))

	return schema, nil
}

func (sm *StreamManager) getSchemaByIDWithCache(cacheKey string, schemaID int, schemaClient *kafka_client.SchemaRegistryClient) (string, error) {
	sm.mu.RLock()
	if cachedSchema, exists := sm.schemaCache[cacheKey]; exists {
		sm.mu.RUnlock()
		log.DefaultLogger.Debug("Using cached schema", "schemaID", schemaID)
		return cachedSchema, nil
	}
	sm.mu.RUnlock()

	log.DefaultLogger.Debug("Fetching schema by ID from registry (not in cache)", "schemaID", schemaID)
	schema, err := schemaClient.GetSchemaByID(schemaID)
	if err != nil {
		return "", err
	}

	sm.mu.Lock()
	sm.schemaCache[cacheKey] = schema
	sm.mu.Unlock()

	log.DefaultLogger.Debug("Cached schema by ID", "schemaID", schemaID, "schemaLength", len(schema))

	return schema, nil
}

// getSchemaFromRegistryWithCache gets or creates the Schema Registry client and retrieves schema with caching
func (sm *StreamManager) getSchemaFromRegistryWithCache(format, registryUrl, username, password, subject string) (string, error) {
	schemaClient, err := sm.getSchemaRegistryClient(registryUrl, username, password)
	if err != nil {
		return "", err
	}

	cacheKey := fmt.Sprintf("%s:%s", format, subject)
	return sm.getSchemaWithCache(cacheKey, subject, schemaClient)
}

func (sm *StreamManager) getSchemaByIDFromRegistryWithCache(format, registryUrl, username, password string, schemaID int) (string, error) {
	schemaClient, err := sm.getSchemaRegistryClient(registryUrl, username, password)
	if err != nil {
		return "", err
	}

	cacheKey := fmt.Sprintf("%s:id:%d", format, schemaID)
	return sm.getSchemaByIDWithCache(cacheKey, schemaID, schemaClient)
}

func (sm *StreamManager) getSchemaRegistryClient(registryUrl, username, password string) (*kafka_client.SchemaRegistryClient, error) {
	sm.mu.RLock()
	schemaClient := sm.schemaRegistryClient
	sm.mu.RUnlock()

	if schemaClient != nil {
		return schemaClient, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.schemaRegistryClient == nil {
		log.DefaultLogger.Debug("Creating Schema Registry client (first use)")
		httpClient := sm.client.GetHTTPClient()
		if httpClient == nil {
			return nil, fmt.Errorf("HTTP client not available in KafkaClient")
		}
		sm.schemaRegistryClient = kafka_client.NewSchemaRegistryClient(registryUrl, username, password, httpClient)
	}

	return sm.schemaRegistryClient, nil
}

// ProcessMessage converts a Kafka message into a Grafana data frame.
// This method uses the StreamManager's configured flatten settings.
func (sm *StreamManager) ProcessMessage(
	msg kafka_client.KafkaMessage,
	partition int32,
	partitions []int32,
	config *StreamConfig,
	topic string,
) (*data.Frame, error) {
	log.DefaultLogger.Debug("Processing message with StreamManager",
		"partition", partition,
		"offset", msg.Offset,
		"flattenMaxDepth", sm.flattenMaxDepth,
		"flattenFieldCap", sm.flattenFieldCap)

	// If there's an error in the message, create a frame with error information
	if msg.Error != nil {
		return createErrorFrame(msg, partition, partitions, msg.Error, config, topic)
	}

	// Check if message needs Avro/Protobuf decoding first to determine if nil Value is expected
	messageFormat := config.MessageFormat

	// Check if Value is nil - this indicates parsing/decoding failure for non-Avro formats
	// For Avro/Protobuf format, Value is intentionally nil as decoding is deferred
	// Also allow nil values if there is raw data available (might be Avro data with wrong format)
	if msg.Value == nil && messageFormat != "avro" && messageFormat != "protobuf" && len(msg.RawValue) == 0 {
		return createErrorFrame(msg, partition, partitions, fmt.Errorf("message value is nil - possible decoding failure"), config, topic)
	}

	messageValue := msg.Value
	if messageFormat == "avro" && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting Avro decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue))

		decoded, err := decodeAvroMessage(sm, sm.client, msg.RawValue, config, topic)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Avro message", "error", err)
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("avro decoding failed: %w", err), config, topic)
		}
		messageValue = decoded
		log.DefaultLogger.Debug("Avro decoding successful")
	} else if messageFormat == "protobuf" && len(msg.RawValue) > 0 {
		log.DefaultLogger.Debug("Attempting Protobuf decoding for message",
			"partition", partition,
			"offset", msg.Offset,
			"rawValueLength", len(msg.RawValue))

		decoded, err := decodeProtobufMessage(sm, sm.client, msg.RawValue, config, topic)
		if err != nil {
			log.DefaultLogger.Error("Failed to decode Protobuf message", "error", err)
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("protobuf decoding failed: %w", err), config, topic)
		}
		messageValue = decoded
		log.DefaultLogger.Debug("Protobuf decoding successful")
	} else if messageFormat == "json" && msg.Value == nil && len(msg.RawValue) > 0 {
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
			return createErrorFrame(msg, partition, partitions, fmt.Errorf("json decoding failed: %w", err), config, topic)
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
				return createErrorFrame(msg, partition, partitions, fmt.Errorf("decoded JSON is not a valid object or array: %T", v), config, topic)
			}
		}
	}

	frame := data.NewFrame("response")
	if config.RefID != "" {
		frame.RefID = config.RefID
	}
	// Always set frame name if alias is present, using empty string for field placeholder
	if config.Alias != "" {
		formatted := formatAlias(config.Alias, config, topic, partition, "")
		log.DefaultLogger.Debug("Applying frame alias", "original", config.Alias, "formatted", formatted)
		frame.Name = formatted
	}

	// Add time field
	timeField := data.NewField("time", nil, make([]time.Time, 1))
	timestampMode := config.TimestampMode
	var frameTime time.Time
	if timestampMode == "now" {
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

	// Unwrap Avro union types before flattening (for Avro messages)
	// This must happen BEFORE flattening to avoid creating keys like "value1.double"
	isAvro := config.MessageFormat == "avro"

	if isAvro {
		messageValue = UnwrapAvroUnions(messageValue)
	}

	// Flatten and process message values using configured settings
	flat := make(map[string]interface{})

	// Handle top-level arrays by wrapping them in an object
	if arr, ok := messageValue.([]interface{}); ok {
		wrappedValue := make(map[string]interface{})
		for i, element := range arr {
			key := fmt.Sprintf("item_%d", i)
			wrappedValue[key] = element
		}
		messageValue = wrappedValue
	}

	// Use StreamManager's configured flatten settings
	FlattenJSON("", messageValue, flat, 0, sm.flattenMaxDepth, sm.flattenFieldCap)

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

	// Add message fields by direct assignment
	for i, key := range keys {
		value := flat[key]
		fieldIdx := msgFieldStart + i
		sm.fieldBuilder.AddValueToFrame(frame, key, value, fieldIdx)
	}

	// Always apply alias to fields if configured, skipping time field
	if config.Alias != "" {
		for _, field := range frame.Fields {
			if field.Name == "time" {
				continue
			}
			if field.Config == nil {
				field.Config = &data.FieldConfig{}
			}
			formatted := formatAlias(config.Alias, config, topic, partition, field.Name)
			field.Config.DisplayNameFromDS = formatted
		}
	}

	return frame, nil
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
	autoOffsetReset := config.AutoOffsetReset
	log.DefaultLogger.Debug("Starting partition reader",
		"topic", qm.Topic,
		"partition", partition,
		"autoOffsetReset", autoOffsetReset,
		"lastN", qm.LastN)

	reader, err := sm.client.NewStreamReader(ctx, qm.Topic, partition, autoOffsetReset, qm.LastN)
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
			if err := reader.Close(); err != nil {
				log.DefaultLogger.Error("failed to close reader", "error", err)
			}
		}
	}()

	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Debug("Partition reader stopping",
				"partition", partition,
				"totalMessages", messageCount)
			return
		default:
			log.DefaultLogger.Debug("Attempting to read message from partition", "partition", partition)

			// Add a timeout context to prevent infinite blocking
			msgCtx, msgCancel := context.WithTimeout(ctx, messageReadTimeout)
			messageFormat := config.MessageFormat
			msg, err := sm.client.ConsumerPull(msgCtx, reader, messageFormat)
			msgCancel() // Cancel immediately after use to avoid resource leaks in long-running loop

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
					log.DefaultLogger.Debug("Sent error message to channel", "partition", partition, "error", err)
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
	log.DefaultLogger.Debug("ValidateAndGetPartitions called", "topic", qm.Topic, "partition", qm.Partition, "partitionType", fmt.Sprintf("%T", qm.Partition))
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
		log.DefaultLogger.Debug("Available partitions", "topic", qm.Topic, "partitions", allPartitions)
		sel := int32(v)
		count := int32(len(allPartitions))
		log.DefaultLogger.Debug("Partition validation", "selected", sel, "count", count, "validRange", fmt.Sprintf("[0..%d)", count))
		if sel < 0 || sel >= count {
			return nil, fmt.Errorf("partition %d out of range [0..%d) for topic %s", sel, count, qm.Topic)
		}
		result := []int32{sel}
		log.DefaultLogger.Debug("Returning partitions for single partition", "partitions", result)
		return result, nil
	case string:
		if v == "all" {
			// Get all partitions for the topic
			allPartitions, err := sm.client.GetTopicPartitions(ctx, qm.Topic)
			if err != nil {
				return nil, sm.handleTopicError(err, qm.Topic)
			}
			log.DefaultLogger.Debug("Returning all partitions", "partitions", allPartitions)
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

// formatAlias replaces placeholders in the alias string with actual values.
func formatAlias(alias string, config *StreamConfig, topic string, partition int32, fieldName string) string {
	// Simple replacement for now - can use regex if more complex logic needed
	alias = strings.ReplaceAll(alias, "{{topic}}", topic)
	alias = strings.ReplaceAll(alias, "{{partition}}", fmt.Sprintf("%d", partition))
	alias = strings.ReplaceAll(alias, "{{refid}}", config.RefID)

	if fieldName != "" {
		alias = strings.ReplaceAll(alias, "{{field}}", fieldName)
	} else {
		alias = strings.ReplaceAll(alias, "{{field}}", "")
	}
	return alias
}
