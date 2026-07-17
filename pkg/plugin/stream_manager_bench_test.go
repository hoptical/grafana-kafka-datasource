package plugin

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/hoptical/grafana-kafka-datasource/pkg/perfflags"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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

const benchProtoSchema = `syntax = "proto3";

package benchdata;

message SensorReading {
  string id = 1;
  double value = 2;
  int64 count = 3;
}
`

// newBenchStreamManager returns a StreamManager wired to a no-op mockStreamClient,
// suitable for benchmarking ProcessMessage/ProcessMessageFrames without any network I/O.
func newBenchStreamManager() *StreamManager {
	return NewStreamManager(&mockStreamClient{}, defaultFlattenMaxDepth, defaultFlattenFieldCap)
}

func benchKafkaMessage(value interface{}, rawValue []byte) kafka_client.KafkaMessage {
	return kafka_client.KafkaMessage{
		Value:     value,
		RawValue:  rawValue,
		Timestamp: time.Now(),
		Offset:    1,
	}
}

func runProcessMessageBench(b *testing.B, sm *StreamManager, msg kafka_client.KafkaMessage, config *StreamConfig) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "bench-topic"); err != nil {
			b.Fatalf("ProcessMessage failed: %v", err)
		}
	}
}

func BenchmarkProcessMessage_JSON(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "json", TimestampMode: "message"}
	// Simulate the eager JSON decode already performed by kafka_client.ConsumerPull.
	msg := benchKafkaMessage(buildFlatJSON(20), nil)
	runProcessMessageBench(b, sm, msg, config)
}

// BenchmarkProcessMessage_JSON_FieldOrderCacheDisabled reproduces the pre-fix
// behavior (always collect+sort keys from scratch) via perfflags.FieldOrderCache,
// for before/after comparison against BenchmarkProcessMessage_JSON without
// needing a separate git checkout. Run both with `-benchmem` and compare with
// benchstat, or set KAFKA_DS_PERF_DISABLE_FIELD_ORDER_CACHE=true out-of-process.
func BenchmarkProcessMessage_JSON_FieldOrderCacheDisabled(b *testing.B) {
	perfflags.FieldOrderCache.SetDisabledForTest(true)
	defer perfflags.FieldOrderCache.SetDisabledForTest(false)

	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "json", TimestampMode: "message"}
	msg := benchKafkaMessage(buildFlatJSON(20), nil)
	runProcessMessageBench(b, sm, msg, config)
}

func BenchmarkProcessMessage_Plaintext(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "plaintext", TimestampMode: "message"}
	msg := benchKafkaMessage(nil, []byte("2024-01-01T00:00:00Z sensor-01 value=21.5"))
	runProcessMessageBench(b, sm, msg, config)
}

func BenchmarkProcessMessage_Avro(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{
		MessageFormat:    "avro",
		AvroSchemaSource: "inlineSchema",
		AvroSchema:       benchAvroSchema,
		TimestampMode:    "message",
	}

	codec, err := goavro.NewCodec(benchAvroSchema)
	if err != nil {
		b.Fatalf("failed to build avro codec: %v", err)
	}
	native := map[string]interface{}{"id": "sensor-01", "value": 21.5, "count": int64(42)}
	binaryPayload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		b.Fatalf("failed to encode avro fixture: %v", err)
	}

	msg := benchKafkaMessage(nil, binaryPayload)
	runProcessMessageBench(b, sm, msg, config)
}

func BenchmarkProcessMessage_Protobuf(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{
		MessageFormat:        "protobuf",
		ProtobufSchemaSource: "inlineSchema",
		ProtobufSchema:       benchProtoSchema,
		TimestampMode:        "message",
	}

	parsed, err := kafka_client.ParseProtobufSchema(benchProtoSchema)
	if err != nil {
		b.Fatalf("failed to parse protobuf schema: %v", err)
	}
	dm := dynamicpb.NewMessage(parsed.Message)
	dm.Set(parsed.Message.Fields().ByName("id"), protoreflect.ValueOfString("sensor-01"))
	dm.Set(parsed.Message.Fields().ByName("value"), protoreflect.ValueOfFloat64(21.5))
	dm.Set(parsed.Message.Fields().ByName("count"), protoreflect.ValueOfInt64(42))
	payload, err := proto.Marshal(dm)
	if err != nil {
		b.Fatalf("failed to marshal protobuf fixture: %v", err)
	}

	msg := benchKafkaMessage(nil, payload)
	runProcessMessageBench(b, sm, msg, config)
}

// BenchmarkProcessMessage_ParseProtobufSchemaOnly isolates the schema
// compilation cost alone (protocompile.Compiler.Compile), to quantify how much
// of BenchmarkProcessMessage_Protobuf's cost is recompilation vs. actual decode.
func BenchmarkProcessMessage_ParseProtobufSchemaOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := kafka_client.ParseProtobufSchema(benchProtoSchema); err != nil {
			b.Fatalf("ParseProtobufSchema failed: %v", err)
		}
	}
}

// BenchmarkDecodeAvroMessage_CodecOnly isolates the goavro.NewCodec compilation
// cost alone, to quantify how much of the per-message Avro decode cost is
// schema (re)compilation vs. actual binary decoding.
func BenchmarkDecodeAvroMessage_CodecOnly(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := goavro.NewCodec(benchAvroSchema); err != nil {
			b.Fatalf("goavro.NewCodec failed: %v", err)
		}
	}
}

func BenchmarkProcessMessageFrames_LineProtocol(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message"}
	raw := buildSyntheticLineProtocol(10, 3, 5)
	msg := benchKafkaMessage(nil, raw)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sm.ProcessMessageFrames(msg, 0, []int32{0}, config, "bench-topic"); err != nil {
			b.Fatalf("ProcessMessageFrames failed: %v", err)
		}
	}
}

// BenchmarkProcessMessage_JSON_RawDecode benchmarks the path where the
// message value has NOT been pre-decoded (msg.Value == nil, RawValue set),
// forcing ProcessMessage to run decodeTopLevelJSON itself.
func BenchmarkProcessMessage_JSON_RawDecode(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "json", TimestampMode: "message"}
	raw, err := json.Marshal(buildFlatJSON(20))
	if err != nil {
		b.Fatalf("failed to marshal fixture: %v", err)
	}
	msg := benchKafkaMessage(nil, raw)
	runProcessMessageBench(b, sm, msg, config)
}

func BenchmarkProcessMessage_JSON_Wide100(b *testing.B) {
	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "json", TimestampMode: "message"}
	msg := benchKafkaMessage(buildFlatJSON(100), nil)
	runProcessMessageBench(b, sm, msg, config)
}

func BenchmarkProcessMessage_JSON_Wide100_FieldOrderCacheDisabled(b *testing.B) {
	perfflags.FieldOrderCache.SetDisabledForTest(true)
	defer perfflags.FieldOrderCache.SetDisabledForTest(false)

	sm := newBenchStreamManager()
	config := &StreamConfig{MessageFormat: "json", TimestampMode: "message"}
	msg := benchKafkaMessage(buildFlatJSON(100), nil)
	runProcessMessageBench(b, sm, msg, config)
}
