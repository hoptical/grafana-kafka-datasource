package main

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
)

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
func ConvertToAvroData(shape string, data interface{}) (interface{}, error) {
	switch shape {
	case "flat":
		if m, ok := data.(map[string]interface{}); ok {
			// Data should already be in correct Avro format from producer
			fmt.Printf("[PRODUCER DEBUG] Flat data conversion - input keys: %v\n", getMapKeys(m))
			return m, nil
		}
	case "nested":
		if m, ok := data.(map[string]interface{}); ok {
			fmt.Printf("[PRODUCER DEBUG] Nested data conversion - input keys: %v\n", getMapKeys(m))
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
func EncodeAvroMessage(shape string, data interface{}) ([]byte, error) {
	fmt.Printf("[PRODUCER DEBUG] Starting Avro encoding for shape: %s\n", shape)

	schema, err := GetAvroSchema(shape)
	if err != nil {
		return nil, err
	}

	avroData, err := ConvertToAvroData(shape, data)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[PRODUCER DEBUG] Avro data prepared, type: %T\n", avroData)

	encoded, err := schema.Encode(avroData)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[PRODUCER DEBUG] Avro encoding successful, encoded length: %d bytes\n", len(encoded))
	fmt.Printf("[PRODUCER DEBUG] First 20 bytes: %x\n", encoded[:min(20, len(encoded))])

	return encoded, nil
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
