package plugin

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// FieldBuilder handles building Grafana data fields from various JSON value types.
type FieldBuilder struct {
	// typeRegistry tracks field types across messages to maintain schema consistency
	typeRegistry map[string]data.FieldType
}

// NewFieldBuilder creates a new instance of FieldBuilder.
func NewFieldBuilder() *FieldBuilder {
	return &FieldBuilder{
		typeRegistry: make(map[string]data.FieldType),
	}
}

// unwrapAvroUnion unwraps Avro union values from goavro's representation.
// According to goavro v2 behavior:
// - When decoding with goavro.Codec.NativeFromBinary(), union values are returned as:
//   - null values: plain Go nil (not wrapped)
//   - non-null values: plain Go values (int64, float64, string, etc.) (not wrapped)
//
// - The wrapping (map[string]interface{}{"type": value}) is only used during encoding
//
// However, some schema registry implementations or custom decoders may wrap values,
// so we handle both cases:
// - Wrapped: map[string]interface{}{"null": nil} or map[string]interface{}{"double": 42.0}
// - Unwrapped: nil or 42.0 directly
//
// This function detects and unwraps if needed, otherwise returns the value as-is.
func unwrapAvroUnion(key string, value interface{}) interface{} {
	// Check if this is a map that might be an Avro union wrapper
	if m, ok := value.(map[string]interface{}); ok && len(m) == 1 {
		// Check for null union type
		if _, hasNull := m["null"]; hasNull {
			log.DefaultLogger.Debug("Unwrapped Avro null union",
				"key", key)
			return nil
		}

		// Check for other union types and extract the value
		// Common Avro types: string, int, long, float, double, boolean, bytes
		for typeName, val := range m {
			// If it looks like an Avro type wrapper, extract the value
			switch typeName {
			case "string", "int", "long", "float", "double", "boolean", "bytes":
				log.DefaultLogger.Debug("Unwrapped Avro union value",
					"key", key,
					"avroType", typeName,
					"valueType", fmt.Sprintf("%T", val))
				return val
			}
		}
	}

	// Not an Avro union wrapper (or goavro already returned unwrapped value)
	// This is the normal case for goavro v2
	return value
}

// AddValueToFrame adds a key-value pair from flattened JSON to the data frame,
// creating the appropriate field type based on the value's type.
// Note: frame.Fields must be pre-allocated to at least fieldIndex+1 length.
func (fb *FieldBuilder) AddValueToFrame(frame *data.Frame, key string, value interface{}, fieldIndex int) {
	// Unwrap Avro union values before processing
	value = unwrapAvroUnion(key, value)
	switch v := value.(type) {
	case json.Number:
		fb.addJSONNumberField(frame, key, v, fieldIndex)
	case float64:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*float64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, v)
		log.DefaultLogger.Debug("Added float64 field",
			"key", key,
			"value", v)
	case float32:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*float64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, float64(v))
	case int:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*int64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, int64(v))
	case int8, int16, int32, int64:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*int64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, toInt64(v))
	case uint, uint8, uint16, uint32, uint64:
		fb.typeRegistry[key] = data.FieldTypeNullableUint64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*uint64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, toUint64(v))
	case bool:
		fb.typeRegistry[key] = data.FieldTypeNullableBool
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*bool, 1))
		frame.Fields[fieldIndex].SetConcrete(0, v)
	case string:
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*string, 1))
		frame.Fields[fieldIndex].SetConcrete(0, v)
	case nil:
		// Use type registry to maintain schema consistency across messages
		fieldType, exists := fb.typeRegistry[key]
		if !exists {
			// Default to nullable float64 for unknown fields (common for sensor/metric data)
			fieldType = data.FieldTypeNullableFloat64
			// Don't register the type yet - wait for first non-nil value
			log.DefaultLogger.Debug("Null value for unknown field, using default type",
				"key", key,
				"defaultType", fieldType)
		} else {
			log.DefaultLogger.Debug("Null value for known field, using registered type",
				"key", key,
				"registeredType", fieldType)
		}
		fb.createNullableField(frame, key, fieldType, fieldIndex)
	default:
		// Fallback to nullable string for any complex or unknown scalar
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*string, 1))
		frame.Fields[fieldIndex].SetConcrete(0, fmt.Sprintf("%v", v))
	}
}

// createNullableField creates a field with the specified nullable type and leaves the value as nil.
func (fb *FieldBuilder) createNullableField(frame *data.Frame, key string, fieldType data.FieldType, fieldIndex int) {
	switch fieldType {
	case data.FieldTypeNullableFloat64:
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*float64, 1))
	case data.FieldTypeNullableInt64:
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*int64, 1))
	case data.FieldTypeNullableUint64:
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*uint64, 1))
	case data.FieldTypeNullableBool:
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*bool, 1))
	case data.FieldTypeNullableString:
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*string, 1))
	default:
		// Fallback to nullable float64 if type is not recognized
		// Note: value is left as nil (no Set call needed)
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*float64, 1))
	}
}

// addJSONNumberField handles json.Number values, preserving their original numeric type.
func (fb *FieldBuilder) addJSONNumberField(frame *data.Frame, key string, num json.Number, fieldIndex int) {
	coerced := CoerceJSONNumber(num)
	switch cv := coerced.(type) {
	case int64:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*int64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, cv)
	case float64:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*float64, 1))
		frame.Fields[fieldIndex].SetConcrete(0, cv)
	default:
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields[fieldIndex] = data.NewField(key, nil, make([]*string, 1))
		frame.Fields[fieldIndex].SetConcrete(0, fmt.Sprintf("%v", cv))
	}
}
