package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/linkedin/goavro/v2"
)

// SchemaRegistryClient handles communication with Confluent Schema Registry
type SchemaRegistryClient struct {
	BaseURL string
	Client  *http.Client
}

// NewSchemaRegistryClient creates a new schema registry client
func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		BaseURL: baseURL,
		Client:  &http.Client{},
	}
}

// RegisterSchema registers a schema with the registry and returns the schema ID
func (src *SchemaRegistryClient) RegisterSchema(subject string, schema string) (int, error) {
	payload := map[string]string{
		"schema": schema,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", src.BaseURL, subject)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := src.Client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registration failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid response format: missing id field")
	}

	return int(id), nil
}

// EncodeWithSchemaID encodes data using Confluent wire format with schema ID
func (src *SchemaRegistryClient) EncodeWithSchemaID(schemaID int, data interface{}, codec *goavro.Codec) ([]byte, error) {
	// Encode the data using the codec
	native, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data must be a map[string]interface{}")
	}

	binaryData, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data: %w", err)
	}

	// Create Confluent wire format: magic byte (0) + schema ID (4 bytes) + avro data
	var buf bytes.Buffer

	// Magic byte
	buf.WriteByte(0)

	// Schema ID (big endian)
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaID))
	buf.Write(schemaIDBytes)

	// Avro data
	buf.Write(binaryData)

	return buf.Bytes(), nil
}

// AvroSchema defines the schema for Avro encoding
type AvroSchema struct {
	Schema string
	Codec  *goavro.Codec
}

// NewAvroSchema creates a new Avro schema from a schema string
func NewAvroSchema(schemaStr string) (*AvroSchema, error) {
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create codec: %w", err)
	}
	return &AvroSchema{
		Schema: schemaStr,
		Codec:  codec,
	}, nil
}

// Encode encodes data using the Avro schema
func (as *AvroSchema) Encode(data interface{}) ([]byte, error) {
	native, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("data must be a map[string]interface{}")
	}

	binary, err := as.Codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data: %w", err)
	}

	return binary, nil
}

// Predefined schemas for different message shapes
const (
	FlatSchema = `{
		"type": "record",
		"name": "FlatMessage",
		"fields": [
			{"name": "host_name", "type": "string"},
			{"name": "host_ip", "type": "string"},
			{"name": "metrics_cpu_load", "type": "double"},
			{"name": "metrics_cpu_temp", "type": "double"},
			{"name": "metrics_mem_used", "type": "int"},
			{"name": "metrics_mem_free", "type": "int"},
			{"name": "value1", "type": "double"},
			{"name": "value2", "type": "double"},
			{"name": "tags", "type": {"type": "array", "items": "string"}}
		]
	}`

	NestedSchema = `{
		"type": "record",
		"name": "NestedMessage",
		"fields": [
			{"name": "host", "type": {
				"type": "record",
				"name": "Host",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "ip", "type": "string"}
				]
			}},
			{"name": "metrics", "type": {
				"type": "record",
				"name": "Metrics",
				"fields": [
					{"name": "cpu", "type": {
						"type": "record",
						"name": "CPU",
						"fields": [
							{"name": "load", "type": "double"},
							{"name": "temp", "type": "double"}
						]
					}},
					{"name": "mem", "type": {
						"type": "record",
						"name": "Memory",
						"fields": [
							{"name": "used", "type": "int"},
							{"name": "free", "type": "int"}
						]
					}}
				]
			}},
			{"name": "value1", "type": "double"},
			{"name": "value2", "type": "double"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "alerts", "type": {
				"type": "array",
				"items": {
					"type": "record",
					"name": "Alert",
					"fields": [
						{"name": "type", "type": "string"},
						{"name": "severity", "type": "string"},
						{"name": "value", "type": "double"}
					]
				}
			}},
			{"name": "processes", "type": {"type": "array", "items": "string"}}
		]
	}`

	ListItemSchema = `{
		"type": "record",
		"name": "ListItem",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "type", "type": "string"},
			{"name": "host", "type": {
				"type": "record",
				"name": "Host",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "ip", "type": "string"}
				]
			}},
			{"name": "value", "type": ["double", "null"]},
			{"name": "timestamp", "type": "long"},
			{"name": "message", "type": ["string", "null"]},
			{"name": "level", "type": ["string", "null"]},
			{"name": "tags", "type": ["array", "null"], "items": "string"}
		]
	}`
)

// GetAvroSchema returns the appropriate Avro schema for the given shape
func GetAvroSchema(shape string) (*AvroSchema, error) {
	switch shape {
	case "flat":
		return NewAvroSchema(FlatSchema)
	case "nested":
		return NewAvroSchema(NestedSchema)
	default:
		return nil, fmt.Errorf("unsupported shape for Avro: %s", shape)
	}
}

// ConvertToAvroData converts the Go data structure to Avro-compatible format
func ConvertToAvroData(shape string, data interface{}, verbose bool) (interface{}, error) {
	switch shape {
	case "flat":
		if m, ok := data.(map[string]interface{}); ok {
			// Data should already be in correct Avro format from producer
			if verbose {
				fmt.Printf("[PRODUCER DEBUG] Flat data conversion - input keys: %v\n", getMapKeys(m))
			}
			return m, nil
		}
	case "nested":
		if m, ok := data.(map[string]interface{}); ok {
			if verbose {
				fmt.Printf("[PRODUCER DEBUG] Nested data conversion - input keys: %v\n", getMapKeys(m))
			}
			return m, nil // Already in correct format
		}
	}
	return nil, fmt.Errorf("cannot convert data for shape: %s", shape)
}

// getMapKeys returns a slice of keys from a map for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// EncodeAvroMessage encodes a message using Avro
func EncodeAvroMessage(shape string, data interface{}, schemaRegistryURL string, topic string, verbose bool) ([]byte, error) {
	if verbose {
		fmt.Printf("[PRODUCER DEBUG] Starting Avro encoding for shape: %s\n", shape)
		if schemaRegistryURL != "" {
			fmt.Printf("[PRODUCER DEBUG] Using schema registry: %s\n", schemaRegistryURL)
		} else {
			fmt.Printf("[PRODUCER DEBUG] Using inline schema\n")
		}
	}

	// Get the inline schema first
	schema, err := GetAvroSchema(shape)
	if err != nil {
		return nil, err
	}

	avroData, err := ConvertToAvroData(shape, data, verbose)
	if err != nil {
		return nil, err
	}

	if verbose {
		fmt.Printf("[PRODUCER DEBUG] Avro data prepared, type: %T\n", avroData)
	}

	// If schema registry URL is provided, use schema registry
	if schemaRegistryURL != "" {
		client := NewSchemaRegistryClient(schemaRegistryURL)
		subject := fmt.Sprintf("%s-value", topic) // Kafka convention: topic-value

		if verbose {
			fmt.Printf("[PRODUCER DEBUG] Registering schema with registry for topic: %s, subject: %s\n", topic, subject)
		}

		schemaID, err := client.RegisterSchema(subject, schema.Schema)
		if err != nil {
			if verbose {
				fmt.Printf("[PRODUCER DEBUG] Schema registry registration failed: %v\n", err)
				fmt.Printf("[PRODUCER DEBUG] Falling back to inline schema\n")
			}
			// Fall back to inline encoding
			return schema.Encode(avroData)
		}

		if verbose {
			fmt.Printf("[PRODUCER DEBUG] Schema registered with ID: %d\n", schemaID)
		}

		// Encode with Confluent wire format
		encoded, err := client.EncodeWithSchemaID(schemaID, avroData, schema.Codec)
		if err != nil {
			return nil, fmt.Errorf("failed to encode with schema registry: %w", err)
		}

		if verbose {
			fmt.Printf("[PRODUCER DEBUG] Schema registry encoding successful, encoded length: %d bytes\n", len(encoded))
			fmt.Printf("[PRODUCER DEBUG] First 20 bytes: %x\n", encoded[:min(20, len(encoded))])
		}

		return encoded, nil
	}

	// Use inline schema encoding
	encoded, err := schema.Encode(avroData)
	if err != nil {
		return nil, err
	}

	if verbose {
		fmt.Printf("[PRODUCER DEBUG] Inline Avro encoding successful, encoded length: %d bytes\n", len(encoded))
		fmt.Printf("[PRODUCER DEBUG] First 20 bytes: %x\n", encoded[:min(20, len(encoded))])
	}

	return encoded, nil
}


