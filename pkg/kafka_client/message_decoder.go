package kafka_client

import (
	"fmt"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	defaultAvroCodecCacheMaxEntries      = 256
	defaultProtobufSchemaCacheMaxEntries = 256
)

// MessageDecoderOptions controls the reusable decode helpers used by the
// plugin hot path and by benchmarks that need to reproduce pre-fix behavior.
type MessageDecoderOptions struct {
	DisableAvroCodecCache      bool
	AvroCodecCacheMaxEntries   int
	DisableProtobufSchemaCache bool
	ProtobufSchemaCacheMaxEntries int
}

// MessageDecoder owns the reusable decode caches for Avro and Protobuf.
// Production uses a single default decoder; benchmarks can construct explicit
// variants (for example, with caches disabled) without mutating package-global
// state.
type MessageDecoder struct {
	disableAvroCodecCache      bool
	avroCodecCache             *lruCache[*goavro.Codec]
	disableProtobufSchemaCache bool
	protobufSchemaCache        *lruCache[*ParsedProtobufSchema]
}

func DefaultMessageDecoder() *MessageDecoder {
	return defaultMessageDecoder
}

func NewMessageDecoder(options MessageDecoderOptions) *MessageDecoder {
	avroCacheMaxEntries := options.AvroCodecCacheMaxEntries
	if avroCacheMaxEntries < 1 {
		avroCacheMaxEntries = defaultAvroCodecCacheMaxEntries
	}

	protobufSchemaCacheMaxEntries := options.ProtobufSchemaCacheMaxEntries
	if protobufSchemaCacheMaxEntries < 1 {
		protobufSchemaCacheMaxEntries = defaultProtobufSchemaCacheMaxEntries
	}

	return &MessageDecoder{
		disableAvroCodecCache:      options.DisableAvroCodecCache,
		avroCodecCache:             newLRUCache[*goavro.Codec](avroCacheMaxEntries),
		disableProtobufSchemaCache: options.DisableProtobufSchemaCache,
		protobufSchemaCache:        newLRUCache[*ParsedProtobufSchema](protobufSchemaCacheMaxEntries),
	}
}

func (d *MessageDecoder) getAvroCodec(schema string) (*goavro.Codec, error) {
	if d.disableAvroCodecCache {
		return goavro.NewCodec(schema)
	}

	if cached, ok := d.avroCodecCache.Get(schema); ok {
		return cached, nil
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	d.avroCodecCache.Add(schema, codec)
	return codec, nil
}

func (d *MessageDecoder) DecodeAvroMessage(data []byte, schema string) (interface{}, error) {
	var avroData []byte

	if len(data) > 5 && data[0] == 0x00 {
		avroData = data[5:]
	} else {
		avroData = data
	}

	codec, err := d.getAvroCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	decoded, _, err := codec.NativeFromBinary(avroData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro message: %w", err)
	}

	return decoded, nil
}

func (d *MessageDecoder) ParseProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
	if d.disableProtobufSchemaCache {
		return compileProtobufSchema(schema)
	}

	if cached, ok := d.protobufSchemaCache.Get(schema); ok {
		return cached, nil
	}

	parsed, err := compileProtobufSchema(schema)
	if err != nil {
		return nil, err
	}

	d.protobufSchemaCache.Add(schema, parsed)
	return parsed, nil
}

func (d *MessageDecoder) DecodeProtobufMessage(data []byte, schema string) (interface{}, error) {
	parsed, err := d.ParseProtobufSchema(schema)
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

func (d *MessageDecoder) DefaultMessageDescriptor(schema string) (protoreflect.MessageDescriptor, error) {
	parsed, err := d.ParseProtobufSchema(schema)
	if err != nil {
		return nil, err
	}
	return parsed.Message, nil
}

var defaultMessageDecoder = NewMessageDecoder(MessageDecoderOptions{})