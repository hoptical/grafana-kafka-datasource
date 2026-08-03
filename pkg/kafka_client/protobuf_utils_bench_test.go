package kafka_client

import (
	"encoding/binary"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildBenchProtobufPayload encodes a fixture message once (outside the timed
// benchmark loop) against simpleProtoSchema (defined in protobuf_utils_test.go).
func buildBenchProtobufPayload(b *testing.B) []byte {
	b.Helper()
	decoder := NewMessageDecoder(MessageDecoderOptions{})
	parsed, err := decoder.ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		b.Fatalf("failed to parse schema: %v", err)
	}
	msg := dynamicpb.NewMessage(parsed.Message)
	msg.Set(parsed.Message.Fields().ByName("name"), protoreflect.ValueOfString("alice"))
	msg.Set(parsed.Message.Fields().ByName("age"), protoreflect.ValueOfInt64(33))
	payload, err := proto.Marshal(msg)
	if err != nil {
		b.Fatalf("failed to marshal fixture: %v", err)
	}
	return payload
}

func buildBenchConfluentProtobufPayload(b *testing.B) []byte {
	b.Helper()
	payload := buildBenchProtobufPayload(b)
	wire := make([]byte, 0, 8+len(payload))
	wire = append(wire, 0)
	schemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaID, 101)
	wire = append(wire, schemaID...)
	// count-prefixed indexes: count=1, index=0
	wire = append(wire, protowire.AppendVarint(nil, 1)...)
	wire = append(wire, protowire.AppendVarint(nil, 0)...)
	wire = append(wire, payload...)
	return wire
}

// BenchmarkDecodeProtobufMessage measures the full per-message decode cost,
// including ParseProtobufSchema's protocompile.Compiler.Compile call, which
// currently runs on EVERY message (the schema string is cached upstream in
// StreamManager, but the compiled descriptor is not).
func BenchmarkDecodeProtobufMessage_Plain(b *testing.B) {
	decoder := NewMessageDecoder(MessageDecoderOptions{})
	payload := buildBenchProtobufPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decoder.DecodeProtobufMessage(payload, simpleProtoSchema); err != nil {
			b.Fatalf("DecodeProtobufMessage failed: %v", err)
		}
	}
}

func BenchmarkDecodeProtobufMessage_Plain_NoCache(b *testing.B) {
	decoder := NewMessageDecoder(MessageDecoderOptions{DisableProtobufSchemaCache: true})
	payload := buildBenchProtobufPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decoder.DecodeProtobufMessage(payload, simpleProtoSchema); err != nil {
			b.Fatalf("DecodeProtobufMessage failed: %v", err)
		}
	}
}

func BenchmarkDecodeProtobufMessage_ConfluentWireFormat(b *testing.B) {
	decoder := NewMessageDecoder(MessageDecoderOptions{})
	payload := buildBenchConfluentProtobufPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decoder.DecodeProtobufMessage(payload, simpleProtoSchema); err != nil {
			b.Fatalf("DecodeProtobufMessage failed: %v", err)
		}
	}
}

func BenchmarkParseProtobufSchema(b *testing.B) {
	decoder := NewMessageDecoder(MessageDecoderOptions{DisableProtobufSchemaCache: true})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decoder.ParseProtobufSchema(simpleProtoSchema); err != nil {
			b.Fatalf("ParseProtobufSchema failed: %v", err)
		}
	}
}

// BenchmarkDecodeProtobufMessage_DecodeOnly isolates just the unmarshal +
// map-conversion cost using a descriptor parsed once outside the loop,
// simulating what decode cost would look like if the descriptor were cached.
func BenchmarkDecodeProtobufMessage_DecodeOnly(b *testing.B) {
	payload := buildBenchProtobufPayload(b)
	decoder := NewMessageDecoder(MessageDecoderOptions{})
	parsed, err := decoder.ParseProtobufSchema(simpleProtoSchema)
	if err != nil {
		b.Fatalf("failed to parse schema: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := dynamicpb.NewMessage(parsed.Message)
		if err := proto.Unmarshal(payload, msg); err != nil {
			b.Fatalf("proto.Unmarshal failed: %v", err)
		}
		_ = protobufMessageToMap(msg.ProtoReflect())
	}
}
