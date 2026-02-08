package kafka_client

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
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

// ParseProtobufSchema parses a .proto schema and returns a default message descriptor.
// Imports are not supported for inline schemas; users should inline dependencies.
func ParseProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
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
	return fileDesc.Messages().Get(count - 1)
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

	// Confluent encodes message indexes as 1-based varints terminated by 0.
	indexes, remaining, ok := parseTerminatedIndexes(data)
	if ok {
		return indexes, remaining, nil
	}

	// Fallback: Attempt count-prefixed indexes.
	if indexes, remaining, ok := parseCountPrefixedIndexes(data); ok {
		log.DefaultLogger.Debug("Parsed protobuf message indexes using count-prefixed format",
			"indexes", indexes)
		return indexes, remaining, nil
	}

	// Fallback: single index without terminator.
	index, n := protowire.ConsumeVarint(data)
	if n <= 0 {
		return nil, nil, fmt.Errorf("failed to parse protobuf message index")
	}
	// A single varint(0) means use the first message (index 0) - this is valid
	// in Confluent wire format for schemas with a single top-level message type.
	// Otherwise, indexes are 1-based, so subtract 1.
	if index == 0 {
		log.DefaultLogger.Debug("Parsed protobuf message index using single-varint format",
			"index", 0)
		return []int{0}, data[n:], nil
	}
	log.DefaultLogger.Debug("Parsed protobuf message index using single-varint format",
		"index", index-1)
	return []int{int(index - 1)}, data[n:], nil
}

func parseTerminatedIndexes(data []byte) ([]int, []byte, bool) {
	indexes := make([]int, 0, 4)
	offset := 0
	for offset < len(data) {
		value, n := protowire.ConsumeVarint(data[offset:])
		if n <= 0 {
			return nil, nil, false
		}
		offset += n
		if value == 0 {
			if len(indexes) == 0 {
				return nil, nil, false
			}
			return indexes, data[offset:], true
		}
		indexes = append(indexes, int(value-1))
		if len(indexes) > 16 {
			return nil, nil, false
		}
	}

	return nil, nil, false
}

func parseCountPrefixedIndexes(data []byte) ([]int, []byte, bool) {
	count, n := protowire.ConsumeVarint(data)
	if n <= 0 || count == 0 || count > 10 {
		return nil, nil, false
	}

	indexes := make([]int, 0, count)
	offset := n
	for i := 0; i < int(count); i++ {
		idx, m := protowire.ConsumeVarint(data[offset:])
		if m <= 0 {
			return nil, nil, false
		}
		indexes = append(indexes, int(idx))
		offset += m
		if offset > len(data) {
			return nil, nil, false
		}
	}

	return indexes, data[offset:], true
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
