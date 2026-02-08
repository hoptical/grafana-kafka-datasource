package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	protobufFlatSchema = `syntax = "proto3";

package samples;

message FlatMessage {
  string host_name = 1;
  string host_ip = 2;
  optional double metrics_cpu_load = 3;
  double metrics_cpu_temp = 4;
  int64 metrics_mem_used = 5;
  int64 metrics_mem_free = 6;
  optional double value1 = 7;
  optional double value2 = 8;
  repeated string tags = 9;
}
`
	protobufNestedSchema = `syntax = "proto3";

package samples;

message Host {
  string name = 1;
  string ip = 2;
}

message Cpu {
  optional double load = 1;
  double temp = 2;
}

message Mem {
  int64 used = 1;
  int64 free = 2;
}

message Metrics {
  Cpu cpu = 1;
  Mem mem = 2;
}

message Alert {
  string type = 1;
  string severity = 2;
  double value = 3;
}

message NestedMessage {
  Host host = 1;
  Metrics metrics = 2;
  optional double value1 = 3;
  optional double value2 = 4;
  repeated string tags = 5;
  repeated Alert alerts = 6;
  repeated string processes = 7;
}
`
)

type ProtobufSchemaRegistryClient struct {
	BaseURL  string
	Username string
	Password string
	Client   *http.Client
}

func NewProtobufSchemaRegistryClient(baseURL, username, password string) *ProtobufSchemaRegistryClient {
	return &ProtobufSchemaRegistryClient{
		BaseURL:  baseURL,
		Username: username,
		Password: password,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (src *ProtobufSchemaRegistryClient) RegisterSchema(subject string, schema string) (int, error) {
	payload := map[string]string{
		"schema":     schema,
		"schemaType": "PROTOBUF",
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

	if src.Username != "" && src.Password != "" {
		req.SetBasicAuth(src.Username, src.Password)
	}

	resp, err := src.Client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to register schema: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("failed to close response body: %v\n", err)
		}
	}()

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

func EncodeProtobufMessage(shape string, payload interface{}, schemaRegistryURL, schemaRegistryUser, schemaRegistryPass, topic string, verbose bool) ([]byte, error) {
	schema, messageName, err := getProtobufSchema(shape)
	if err != nil {
		return nil, err
	}

	parsed, err := parseProtobufSchema(schema, messageName)
	if err != nil {
		return nil, err
	}

	payloadMap, ok := payload.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("payload must be a map for Protobuf encoding")
	}

	msg := dynamicpb.NewMessage(parsed.Message)
	if err := applyProtobufData(msg, payloadMap); err != nil {
		return nil, err
	}

	binary, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode protobuf message: %w", err)
	}

	if schemaRegistryURL == "" {
		return binary, nil
	}

	subject := kafka_client.GetSubjectName(topic, "topicName")
	client := NewProtobufSchemaRegistryClient(schemaRegistryURL, schemaRegistryUser, schemaRegistryPass)
	schemaID, err := client.RegisterSchema(subject, schema)
	if err != nil {
		return nil, err
	}

	return encodeProtobufWireFormat(schemaID, parsed.MessageIndex, binary), nil
}

func getProtobufSchema(shape string) (string, string, error) {
	switch shape {
	case "flat":
		return protobufFlatSchema, "FlatMessage", nil
	case "nested":
		return protobufNestedSchema, "NestedMessage", nil
	default:
		return "", "", fmt.Errorf("unsupported shape for Protobuf: %s", shape)
	}
}

type parsedProto struct {
	File         protoreflect.FileDescriptor
	Message      protoreflect.MessageDescriptor
	MessageIndex int
}

func parseProtobufSchema(schema string, messageName string) (*parsedProto, error) {
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

	messageDesc, messageIndex, err := findTopLevelMessage(fileDesc, messageName)
	if err != nil {
		return nil, err
	}

	return &parsedProto{File: fileDesc, Message: messageDesc, MessageIndex: messageIndex}, nil
}

func findTopLevelMessage(fileDesc protoreflect.FileDescriptor, name string) (protoreflect.MessageDescriptor, int, error) {
	for i := 0; i < fileDesc.Messages().Len(); i++ {
		msg := fileDesc.Messages().Get(i)
		if string(msg.Name()) == name {
			return msg, i, nil
		}
	}
	return nil, -1, fmt.Errorf("protobuf schema does not contain message %q", name)
}

func encodeProtobufWireFormat(schemaID int, messageIndex int, payload []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(0)
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(schemaID))
	buf.Write(idBytes)
	// Confluent wire format: count-prefixed 0-based indexes
	buf.Write(protowire.AppendVarint(nil, 1))
	buf.Write(protowire.AppendVarint(nil, uint64(messageIndex)))
	buf.Write(payload)
	return buf.Bytes()
}

func applyProtobufData(msg *dynamicpb.Message, data map[string]interface{}) error {
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		value, ok := data[string(field.Name())]
		if !ok || value == nil {
			continue
		}

		if field.IsList() {
			list := msg.Mutable(field).List()
			items, err := toInterfaceSlice(value)
			if err != nil {
				return fmt.Errorf("field %s: %w", field.Name(), err)
			}
			for _, item := range items {
				if field.Kind() == protoreflect.MessageKind {
					childMap, ok := item.(map[string]interface{})
					if !ok {
						return fmt.Errorf("field %s expects object value", field.Name())
					}
					childMsg := dynamicpb.NewMessage(field.Message())
					if err := applyProtobufData(childMsg, childMap); err != nil {
						return err
					}
					list.Append(protoreflect.ValueOfMessage(childMsg))
					continue
				}
				value, err := toProtobufValue(field, item)
				if err != nil {
					return err
				}
				list.Append(value)
			}
			continue
		}

		if field.IsMap() {
			mapValue, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("field %s expects map value", field.Name())
			}
			m := msg.Mutable(field).Map()
			for k, v := range mapValue {
				mapKey := protoreflect.ValueOfString(k).MapKey()
				mapped, err := toProtobufValue(field.MapValue(), v)
				if err != nil {
					return err
				}
				m.Set(mapKey, mapped)
			}
			continue
		}

		if field.Kind() == protoreflect.MessageKind {
			childMap, ok := value.(map[string]interface{})
			if !ok {
				return fmt.Errorf("field %s expects object value", field.Name())
			}
			childMsg := msg.Mutable(field).Message()
			if err := applyProtobufData(childMsg.(*dynamicpb.Message), childMap); err != nil {
				return err
			}
			continue
		}

		mapped, err := toProtobufValue(field, value)
		if err != nil {
			return err
		}
		msg.Set(field, mapped)
	}

	return nil
}

func toInterfaceSlice(value interface{}) ([]interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case []string:
		items := make([]interface{}, 0, len(v))
		for _, item := range v {
			items = append(items, item)
		}
		return items, nil
	case []int:
		items := make([]interface{}, 0, len(v))
		for _, item := range v {
			items = append(items, item)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("invalid list value")
	}
}

func toProtobufValue(field protoreflect.FieldDescriptor, value interface{}) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(toBool(value)), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(int32(toInt64(value))), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(toInt64(value)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(uint32(toUint64(value))), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(toUint64(value)), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(toFloat64(value))), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(toFloat64(value)), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprintf("%v", value)), nil
	case protoreflect.EnumKind:
		enumNumber, err := enumNumberFromValue(field, value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfEnum(enumNumber), nil
	default:
		return protoreflect.ValueOfString(fmt.Sprintf("%v", value)), nil
	}
}

func enumNumberFromValue(field protoreflect.FieldDescriptor, value interface{}) (protoreflect.EnumNumber, error) {
	switch v := value.(type) {
	case string:
		enumValue := field.Enum().Values().ByName(protoreflect.Name(v))
		if enumValue == nil {
			return 0, fmt.Errorf("unknown enum name %q for field %s", v, field.Name())
		}
		return enumValue.Number(), nil
	case int:
		return enumNumberFromInt(field, int64(v))
	case int32:
		return enumNumberFromInt(field, int64(v))
	case int64:
		return enumNumberFromInt(field, v)
	case uint:
		return enumNumberFromInt(field, int64(v))
	case uint32:
		return enumNumberFromInt(field, int64(v))
	case uint64:
		return enumNumberFromInt(field, int64(v))
	case float64:
		return enumNumberFromInt(field, int64(v))
	default:
		return 0, fmt.Errorf("unsupported enum value type %T for field %s", value, field.Name())
	}
}

func enumNumberFromInt(field protoreflect.FieldDescriptor, value int64) (protoreflect.EnumNumber, error) {
	enumNumber := protoreflect.EnumNumber(value)
	if field.Enum().Values().ByNumber(enumNumber) == nil {
		return 0, fmt.Errorf("unknown enum value %d for field %s", value, field.Name())
	}
	return enumNumber, nil
}

func toBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true"
	default:
		return false
	}
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	case uint:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case int:
		return uint64(v)
	case int64:
		return uint64(v)
	case float64:
		return uint64(v)
	default:
		return 0
	}
}

func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float32:
		return float64(v)
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}
