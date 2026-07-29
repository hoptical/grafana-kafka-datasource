package kafka_client

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/hoptical/grafana-kafka-datasource/pkg/perfflags"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	confluentWireMagicByte = 0x00
)

type ParsedProtobufSchema struct {
	File    protoreflect.FileDescriptor
	Message protoreflect.MessageDescriptor
}

// protobufSchemaCache caches parsed/compiled schemas by their raw schema
// string. Compiling a .proto schema (protocompile.Compiler.Compile) is
// expensive - benchmarks show it accounts for >90% of per-message protobuf
// decode time and allocations - while the resulting descriptors are
// immutable and safe for concurrent reuse, so compilation only needs to
// happen once per distinct schema.
//
// Set KAFKA_DS_PERF_DISABLE_PROTOBUF_SCHEMA_CACHE=true to disable this cache
// and reproduce the pre-fix behavior (see pkg/perfflags).
//
// Cache size is bounded to avoid unbounded growth in long-lived processes with
// high schema churn. Override size with
// KAFKA_DS_PERF_PROTOBUF_SCHEMA_CACHE_MAX_ENTRIES (default: 256).
var protobufSchemaCache = newLRUCache[*ParsedProtobufSchema](
	cacheSizeFromEnv("KAFKA_DS_PERF_PROTOBUF_SCHEMA_CACHE_MAX_ENTRIES", 256),
)

// ParseProtobufSchema parses a .proto schema and returns a default message descriptor.
// Imports are not supported for inline schemas; users should inline dependencies.
// Results are cached by schema string (see protobufSchemaCache), unless
// perfflags.ProtobufSchemaCache is disabled, in which case the schema is
// always recompiled, matching the plugin's pre-fix behavior.
func ParseProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
	if perfflags.ProtobufSchemaCache.Disabled() {
		return compileProtobufSchema(schema)
	}

	if cached, ok := protobufSchemaCache.Get(schema); ok {
		return cached, nil
	}

	parsed, err := compileProtobufSchema(schema)
	if err != nil {
		return nil, err
	}

	protobufSchemaCache.Add(schema, parsed)
	return parsed, nil
}

// compileProtobufSchema compiles schema from scratch, bypassing any cache.
func compileProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: func(path string) (io.ReadCloser, error) {
				if path == "inline.proto" {
					return io.NopCloser(strings.NewReader(schema)), nil
				}
				return nil, fmt.Errorf("imports are not supported in inline schemas: %s", path)
			},
		},
	}

	files, err := compiler.Compile(context.Background(), "inline.proto")
	if err != nil {
		return nil, fmt.Errorf("failed to parse protobuf schema: %w", err)
	}
	fileDesc := files.FindFileByPath("inline.proto")
	if fileDesc == nil {
		return nil, fmt.Errorf("protobuf schema did not produce any files")
	}
	if fileDesc.Messages().Len() == 0 {
		return nil, fmt.Errorf("protobuf schema contains no messages")
	}

	defaultMessage := defaultTopLevelMessage(fileDesc)
	if defaultMessage == nil {
		return nil, fmt.Errorf("protobuf schema contains no usable messages")
	}

	return &ParsedProtobufSchema{
		File:    fileDesc,
		Message: defaultMessage,
	}, nil
}

func defaultTopLevelMessage(fileDesc protoreflect.FileDescriptor) protoreflect.MessageDescriptor {
	count := fileDesc.Messages().Len()
	if count == 0 {
		return nil
	}
	// Confluent uses the first top-level message as default
	return fileDesc.Messages().Get(0)
}

// DecodeProtobufMessage decodes a protobuf message using the provided schema.
// It supports Confluent wire format (magic byte + schema ID + message indexes) and raw protobuf bytes.
func DecodeProtobufMessage(data []byte, schema string) (interface{}, error) {
	parsed, err := ParseProtobufSchema(schema)
	if err != nil {
		return nil, err
	}

	payload := data
	messageDesc := parsed.Message
	if len(data) > 5 && data[0] == confluentWireMagicByte {
		payload, messageDesc, err = extractConfluentProtobufPayload(data, parsed.File)
		if err != nil {
			return nil, err
		}
	}

	msg := dynamicpb.NewMessage(messageDesc)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("failed to decode protobuf message: %w", err)
	}

	return protobufMessageToMap(msg.ProtoReflect()), nil
}

func extractConfluentProtobufPayload(data []byte, fileDesc protoreflect.FileDescriptor) ([]byte, protoreflect.MessageDescriptor, error) {
	if len(data) <= 5 {
		return nil, nil, fmt.Errorf("protobuf wire format header too short")
	}

	// Skip magic byte + schema ID
	payload := data[5:]
	indexes, remaining, err := parseMessageIndexes(payload)
	if err != nil {
		return nil, nil, err
	}
	messageDesc, err := resolveMessageByIndexPath(fileDesc, indexes)
	if err != nil {
		return nil, nil, err
	}

	return remaining, messageDesc, nil
}

func parseMessageIndexes(data []byte) ([]int, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("protobuf wire format missing message index")
	}

	// Confluent encodes message indexes as count-prefixed 0-based varints
	// (varint count + N varint indexes), with a single-varint(0) optimization
	// for the default first message. We also accept a legacy terminated format
	// for compatibility with non-standard encoders.
	indexes, remaining, ok, err := parseCountPrefixedIndexes(data)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		return indexes, remaining, nil
	}

	// Fallback: legacy terminated format.
	if indexes, remaining, ok, err = parseTerminatedIndexes(data); err != nil {
		return nil, nil, err
	} else if ok {
		logMessageIndexFallback("parseTerminatedIndexes", data, remaining, indexes, "not matched", "matched")
		return indexes, remaining, nil
	}

	// Fallback: single varint(0) optimization for first message (valid Confluent format)
	index, n := protowire.ConsumeVarint(data)
	if n <= 0 {
		return nil, nil, fmt.Errorf("failed to parse protobuf message index")
	}
	// A single varint(0) is a valid Confluent optimization for the default first message
	if index == 0 {
		return []int{0}, data[n:], nil
	}
	// Non-zero single varint is non-standard - warn and treat as 0-based index
	indexInt, ok := safeUint64ToInt(index)
	if !ok {
		return nil, nil, fmt.Errorf("protobuf message index %d out of range", index)
	}
	indexes = []int{indexInt}
	remaining = data[n:]
	logMessageIndexFallback("single-varint-nonzero", data, remaining, indexes, "not matched", "not matched")
	return indexes, remaining, nil
}

// parseTerminatedIndexes reports a match via ok=true, a definite non-match via
// ok=false/err=nil (caller should try the next format), or a match on
// structure with corrupt content via err!=nil (caller must not fall through,
// since silently reinterpreting the same bytes under a different format would
// mask the corruption instead of rejecting it).
func parseTerminatedIndexes(data []byte) ([]int, []byte, bool, error) {
	indexes := make([]int, 0, 4)
	offset := 0
	for offset < len(data) {
		value, n := protowire.ConsumeVarint(data[offset:])
		if n <= 0 {
			return nil, nil, false, nil
		}
		offset += n
		if value == 0 {
			if len(indexes) == 0 {
				return nil, nil, false, nil
			}
			return indexes, data[offset:], true, nil
		}
		valueInt, ok := safeUint64ToInt(value)
		if !ok {
			return nil, nil, false, fmt.Errorf("protobuf terminated message index %d out of range", value)
		}
		indexes = append(indexes, valueInt)
		if len(indexes) > 16 {
			return nil, nil, false, nil
		}
	}

	return nil, nil, false, nil
}

// parseCountPrefixedIndexes has the same three-way contract as
// parseTerminatedIndexes: ok=true is a match, ok=false/err=nil means "try the
// next format," and err!=nil means the count prefix was structurally valid
// but one of its indexes overflowed - a hard failure, not a format mismatch.
func parseCountPrefixedIndexes(data []byte) ([]int, []byte, bool, error) {
	count, n := protowire.ConsumeVarint(data)
	if n <= 0 || count == 0 || count > 10 {
		return nil, nil, false, nil
	}
	countInt, ok := safeUint64ToInt(count)
	if !ok {
		return nil, nil, false, nil
	}

	indexes := make([]int, 0, countInt)
	offset := n
	for i := 0; i < countInt; i++ {
		idx, m := protowire.ConsumeVarint(data[offset:])
		if m <= 0 {
			return nil, nil, false, nil
		}
		idxInt, ok := safeUint64ToInt(idx)
		if !ok {
			return nil, nil, false, fmt.Errorf("protobuf count-prefixed message index %d out of range", idx)
		}
		indexes = append(indexes, idxInt)
		offset += m
		if offset > len(data) {
			return nil, nil, false, nil
		}
	}

	return indexes, data[offset:], true, nil
}

func safeUint64ToInt(v uint64) (int, bool) {
	const maxInt = int(^uint(0) >> 1)
	if v > uint64(maxInt) {
		return 0, false
	}
	return int(v), true
}

func logMessageIndexFallback(fallback string, raw []byte, remaining []byte, indexes []int, countPrefixedStatus, terminatedStatus string) {
	rawPrefix := raw
	if len(rawPrefix) > 32 {
		rawPrefix = rawPrefix[:32]
	}
	log.DefaultLogger.Debug("parseMessageIndexes fallback used",
		"fallback", fallback,
		"parseCountPrefixedIndexes", countPrefixedStatus,
		"parseTerminatedIndexes", terminatedStatus,
		"indexes", indexes,
		"rawPrefixHex", fmt.Sprintf("%x", rawPrefix),
		"rawLength", len(raw),
		"remainingLength", len(remaining))
}

func resolveMessageByIndexPath(fileDesc protoreflect.FileDescriptor, path []int) (protoreflect.MessageDescriptor, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("protobuf message index path is empty")
	}
	if path[0] < 0 || path[0] >= fileDesc.Messages().Len() {
		return nil, fmt.Errorf("protobuf message index %d out of range", path[0])
	}

	msg := fileDesc.Messages().Get(path[0])
	for i := 1; i < len(path); i++ {
		idx := path[i]
		if idx < 0 || idx >= msg.Messages().Len() {
			return nil, fmt.Errorf("protobuf nested message index %d out of range", idx)
		}
		msg = msg.Messages().Get(idx)
	}

	return msg, nil
}

func protobufMessageToMap(message protoreflect.Message) map[string]interface{} {
	out := make(map[string]interface{})
	fields := message.Descriptor().Fields()

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.IsList() {
			list := message.Get(field).List()
			if list.Len() == 0 {
				// Include empty lists as nil to maintain schema stability
				out[string(field.Name())] = nil
				continue
			}
			values := make([]interface{}, 0, list.Len())
			for j := 0; j < list.Len(); j++ {
				values = append(values, protobufValueToInterface(field, list.Get(j)))
			}
			out[string(field.Name())] = values
			continue
		}
		if field.IsMap() {
			m := message.Get(field).Map()
			if m.Len() == 0 {
				// Include empty maps as nil to maintain schema stability
				out[string(field.Name())] = nil
				continue
			}
			values := make(map[string]interface{})
			m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				values[fmt.Sprintf("%v", k.Interface())] = protobufValueToInterface(field.MapValue(), v)
				return true
			})
			out[string(field.Name())] = values
			continue
		}

		if !field.HasPresence() {
			// Proto3 scalars without presence should always emit a value (even defaults).
			out[string(field.Name())] = protobufValueToInterface(field, message.Get(field))
			continue
		}

		if !message.Has(field) {
			// Include missing presence-aware fields as nil to maintain schema stability
			out[string(field.Name())] = nil
			continue
		}
		out[string(field.Name())] = protobufValueToInterface(field, message.Get(field))
	}

	return out
}

func protobufValueToInterface(field protoreflect.FieldDescriptor, value protoreflect.Value) interface{} {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return value.Bool()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int64(value.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return uint64(value.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint()
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return value.Float()
	case protoreflect.StringKind:
		return value.String()
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(value.Bytes())
	case protoreflect.EnumKind:
		enumDesc := field.Enum().Values().ByNumber(value.Enum())
		if enumDesc != nil {
			return string(enumDesc.Name())
		}
		return int32(value.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protobufMessageToMap(value.Message())
	default:
		return value.Interface()
	}
}
