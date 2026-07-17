package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// AvroSchema and ProtoSchema describe the same logical record
// (id, seq, value, sent_at_ns) in each format. Paste the matching schema
// into the plugin's datasource config (inline schema, not schema registry)
// to decode traffic produced by this tool.
const AvroSchema = `{
	"type": "record",
	"name": "LoadGenReading",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "seq", "type": "long"},
		{"name": "value", "type": "double"},
		{"name": "sent_at_ns", "type": "long"}
	]
}`

const ProtoSchema = `syntax = "proto3";

package loadgen;

message LoadGenReading {
  string id = 1;
  int64 seq = 2;
  double value = 3;
  int64 sent_at_ns = 4;
}
`

const sensorID = "sensor-01"

// payloadFunc builds one message payload for the given monotonically
// increasing sequence number. Any per-format schema compilation happens once
// before the returned closure is created, not on every call - this tool is
// deliberately built the same way as the plugin's fixed decode path (compile
// once, reuse per message) so it can drive realistic load.
type payloadFunc func(seq int64) ([]byte, error)

// newPayloadBuilder returns a payloadFunc for the requested format.
// jsonFields only applies to the "json" format: it pads the payload with N
// extra numeric fields (field_0..field_{N-1}) to let load tests exercise
// wide-schema topics (see the plugin's field-order-cache optimization, which
// matters most for topics with many fields).
func newPayloadBuilder(format string, jsonFields int) (payloadFunc, error) {
	switch format {
	case "json":
		return newJSONBuilder(jsonFields), nil
	case "avro":
		return newAvroBuilder()
	case "protobuf":
		return newProtobufBuilder()
	case "lineprotocol":
		return newLineProtocolBuilder(), nil
	case "plaintext":
		return newPlaintextBuilder(), nil
	default:
		return nil, fmt.Errorf("unsupported format %q", format)
	}
}

func newJSONBuilder(extraFields int) payloadFunc {
	return func(seq int64) ([]byte, error) {
		payload := map[string]interface{}{
			"id":         sensorID,
			"seq":        seq,
			"value":      rand.Float64() * 100,
			"sent_at_ns": time.Now().UnixNano(),
		}
		for i := 0; i < extraFields; i++ {
			payload[fmt.Sprintf("field_%d", i)] = rand.Float64() * 100
		}
		return json.Marshal(payload)
	}
}

func newAvroBuilder() (payloadFunc, error) {
	codec, err := goavro.NewCodec(AvroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to compile avro schema: %w", err)
	}
	return func(seq int64) ([]byte, error) {
		native := map[string]interface{}{
			"id":         sensorID,
			"seq":        seq,
			"value":      rand.Float64() * 100,
			"sent_at_ns": time.Now().UnixNano(),
		}
		return codec.BinaryFromNative(nil, native)
	}, nil
}

func newProtobufBuilder() (payloadFunc, error) {
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				if path == "inline.proto" {
					return io.NopCloser(strings.NewReader(ProtoSchema)), nil
				}
				return nil, fmt.Errorf("imports are not supported in inline schemas: %s", path)
			},
		},
	}
	files, err := compiler.Compile(context.Background(), "inline.proto")
	if err != nil {
		return nil, fmt.Errorf("failed to compile protobuf schema: %w", err)
	}
	fileDesc := files.FindFileByPath("inline.proto")
	if fileDesc == nil || fileDesc.Messages().Len() == 0 {
		return nil, fmt.Errorf("protobuf schema produced no usable message")
	}
	msgDesc := fileDesc.Messages().Get(0)

	idField := msgDesc.Fields().ByName("id")
	seqField := msgDesc.Fields().ByName("seq")
	valueField := msgDesc.Fields().ByName("value")
	sentAtField := msgDesc.Fields().ByName("sent_at_ns")
	if idField == nil || seqField == nil || valueField == nil || sentAtField == nil {
		return nil, fmt.Errorf("protobuf schema is missing an expected field")
	}

	return func(seq int64) ([]byte, error) {
		msg := dynamicpb.NewMessage(msgDesc)
		msg.Set(idField, protoreflect.ValueOfString(sensorID))
		msg.Set(seqField, protoreflect.ValueOfInt64(seq))
		msg.Set(valueField, protoreflect.ValueOfFloat64(rand.Float64()*100))
		msg.Set(sentAtField, protoreflect.ValueOfInt64(time.Now().UnixNano()))
		return proto.Marshal(msg)
	}, nil
}

func newLineProtocolBuilder() payloadFunc {
	return func(seq int64) ([]byte, error) {
		line := fmt.Sprintf(
			"loadgen,host=%s value=%f,seq=%di,sent_at_ns=%di %d\n",
			sensorID, rand.Float64()*100, seq, time.Now().UnixNano(), time.Now().Unix(),
		)
		return []byte(line), nil
	}
}

func newPlaintextBuilder() payloadFunc {
	return func(seq int64) ([]byte, error) {
		line := fmt.Sprintf("seq=%d value=%.4f sent_at_ns=%d", seq, rand.Float64()*100, time.Now().UnixNano())
		return []byte(line), nil
	}
}
