package kafka_client

import (
	"testing"

	"github.com/linkedin/goavro/v2"
)

const benchAvroSchema = `{
	"type": "record",
	"name": "SensorReading",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "value", "type": "double"},
		{"name": "count", "type": "long"}
	]
}`

// buildBenchAvroPayload encodes a fixture record once (outside the timed
// benchmark loop) using the real codec, so DecodeAvroMessage is exercised
// against a realistic binary payload.
func buildBenchAvroPayload(b *testing.B) []byte {
	b.Helper()
	codec, err := goavro.NewCodec(benchAvroSchema)
	if err != nil {
		b.Fatalf("failed to build avro codec: %v", err)
	}
	native := map[string]interface{}{"id": "sensor-01", "value": 21.5, "count": int64(42)}
	payload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		b.Fatalf("failed to encode avro fixture: %v", err)
	}
	return payload
}

// BenchmarkDecodeAvroMessage measures the full per-message decode cost,
// including the goavro.NewCodec(schema) call that DecodeAvroMessage currently
// performs on every invocation (no codec caching).
func BenchmarkDecodeAvroMessage(b *testing.B) {
	payload := buildBenchAvroPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeAvroMessage(payload, benchAvroSchema); err != nil {
			b.Fatalf("DecodeAvroMessage failed: %v", err)
		}
	}
}

// BenchmarkDecodeAvroMessage_CodecOnly isolates just the schema compilation
// cost (goavro.NewCodec), to quantify how much of BenchmarkDecodeAvroMessage's
// cost is recompilation vs. actual NativeFromBinary decoding.
func BenchmarkDecodeAvroMessage_CodecOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := goavro.NewCodec(benchAvroSchema); err != nil {
			b.Fatalf("goavro.NewCodec failed: %v", err)
		}
	}
}

// BenchmarkDecodeAvroMessage_DecodeOnly isolates just the binary decode cost
// using a codec built once outside the loop, simulating what DecodeAvroMessage's
// cost would look like if the codec were cached.
func BenchmarkDecodeAvroMessage_DecodeOnly(b *testing.B) {
	payload := buildBenchAvroPayload(b)
	codec, err := goavro.NewCodec(benchAvroSchema)
	if err != nil {
		b.Fatalf("failed to build avro codec: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := codec.NativeFromBinary(payload); err != nil {
			b.Fatalf("NativeFromBinary failed: %v", err)
		}
	}
}
