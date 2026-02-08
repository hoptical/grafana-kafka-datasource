package kafka_client

import (
	"encoding/binary"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const simpleProtoSchema = `syntax = "proto3";

package testdata;

message TestMessage {
  string name = 1;
  int64 age = 2;
}
`

func TestDecodeProtobufMessage_Plain(t *testing.T) {
	parsed, err := ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	msg.Set(parsed.Message.Fields().ByName("name"), protoreflect.ValueOfString("alice"))
	msg.Set(parsed.Message.Fields().ByName("age"), protoreflect.ValueOfInt64(33))

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	decoded, err := DecodeProtobufMessage(data, simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}

	result, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded result is not a map")
	}

	if result["name"] != "alice" {
		t.Errorf("expected name to be alice, got %v", result["name"])
	}
	if result["age"] != int64(33) {
		t.Errorf("expected age to be 33, got %v", result["age"])
	}
}

func TestDecodeProtobufMessage_ConfluentWireFormat(t *testing.T) {
	parsed, err := ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	msg.Set(parsed.Message.Fields().ByName("name"), protoreflect.ValueOfString("bob"))
	msg.Set(parsed.Message.Fields().ByName("age"), protoreflect.ValueOfInt64(42))

	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	wire := make([]byte, 0, 8+len(payload))
	wire = append(wire, 0)
	schemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaID, 101)
	wire = append(wire, schemaID...)
	wire = append(wire, protowire.AppendVarint(nil, 1)...)
	wire = append(wire, protowire.AppendVarint(nil, 0)...)
	wire = append(wire, payload...)

	decoded, err := DecodeProtobufMessage(wire, simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to decode wire format message: %v", err)
	}

	result, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded result is not a map")
	}

	if result["name"] != "bob" {
		t.Errorf("expected name to be bob, got %v", result["name"])
	}
	if result["age"] != int64(42) {
		t.Errorf("expected age to be 42, got %v", result["age"])
	}
}

func TestDecodeProtobufMessage_ConfluentWireFormatWithVarintZero(t *testing.T) {
	// Test decoding wire format with varint(0) as used by Kafka Cloud and some producers
	parsed, err := ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	msg.Set(parsed.Message.Fields().ByName("name"), protoreflect.ValueOfString("kafka-cloud"))
	msg.Set(parsed.Message.Fields().ByName("age"), protoreflect.ValueOfInt64(99))

	payload, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	// Create wire format: magic byte + schema ID + varint(0) + payload
	wire := make([]byte, 0, 6+len(payload))
	wire = append(wire, 0) // magic byte
	schemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaID, 99491) // Kafka Cloud schema ID from user's example
	wire = append(wire, schemaID...)
	wire = append(wire, protowire.AppendVarint(nil, 0)...) // varint(0) - use first message
	wire = append(wire, payload...)

	decoded, err := DecodeProtobufMessage(wire, simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to decode wire format message with varint(0): %v", err)
	}

	result, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded result is not a map")
	}

	if result["name"] != "kafka-cloud" {
		t.Errorf("expected name to be kafka-cloud, got %v", result["name"])
	}
	if result["age"] != int64(99) {
		t.Errorf("expected age to be 99, got %v", result["age"])
	}
}

func TestDecodeProtobufMessage_InvalidSchema(t *testing.T) {
	_, err := DecodeProtobufMessage([]byte{0x01, 0x02}, "invalid proto")
	if err == nil {
		t.Fatalf("expected error for invalid schema")
	}
}

func TestDecodeProtobufMessage_MissingFieldsAsNil(t *testing.T) {
	// Test that missing optional fields are included as nil for schema stability
	parsed, err := ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	// Only set name, leave age unset
	msg.Set(parsed.Message.Fields().ByName("name"), protoreflect.ValueOfString("charlie"))

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	decoded, err := DecodeProtobufMessage(data, simpleProtoSchema)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}

	result, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded result is not a map")
	}

	// Verify name is present
	if result["name"] != "charlie" {
		t.Errorf("expected name to be charlie, got %v", result["name"])
	}

	// Verify age field exists in map and defaults to 0 for proto3 scalar without presence
	ageValue, ageExists := result["age"]
	if !ageExists {
		t.Errorf("expected age field to exist in result map for schema stability")
	}
	if ageValue != int64(0) {
		t.Errorf("expected age to be 0 for default value, got %v", ageValue)
	}
}

func TestDecodeProtobufMessage_DefaultValuesWithoutPresence(t *testing.T) {
	schema := `syntax = "proto3";

message DefaultValues {
  int64 count = 1;
  string name = 2;
  optional int32 opt = 3;
}
`

	parsed, err := ParseProtobufSchema(schema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	decoded, err := DecodeProtobufMessage(data, schema)
	if err != nil {
		t.Fatalf("failed to decode message: %v", err)
	}

	result, ok := decoded.(map[string]interface{})
	if !ok {
		t.Fatalf("decoded result is not a map")
	}

	if result["count"] != int64(0) {
		t.Errorf("expected count to be 0, got %v", result["count"])
	}
	if result["name"] != "" {
		t.Errorf("expected name to be empty string, got %v", result["name"])
	}
	if result["opt"] != nil {
		t.Errorf("expected opt to be nil when unset, got %v", result["opt"])
	}
}
