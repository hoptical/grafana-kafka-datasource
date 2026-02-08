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

func TestDecodeProtobufMessage_InvalidSchema(t *testing.T) {
	_, err := DecodeProtobufMessage([]byte{0x01, 0x02}, "invalid proto")
	if err == nil {
		t.Fatalf("expected error for invalid schema")
	}
}
