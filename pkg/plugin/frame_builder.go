package plugin

import (
	"encoding/json"
	"fmt"

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

// AddValueToFrame adds a key-value pair from flattened JSON to the data frame,
// creating the appropriate field type based on the value's type.
func (fb *FieldBuilder) AddValueToFrame(frame *data.Frame, key string, value interface{}, fieldIndex int) {
	switch v := value.(type) {
	case json.Number:
		fb.addJSONNumberField(frame, key, v, fieldIndex)
	case float64:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*float64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, v)
	case float32:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*float64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, float64(v))
	case int:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*int64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, int64(v))
	case int8, int16, int32, int64:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*int64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, toInt64(v))
	case uint, uint8, uint16, uint32, uint64:
		fb.typeRegistry[key] = data.FieldTypeNullableUint64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*uint64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, toUint64(v))
	case bool:
		fb.typeRegistry[key] = data.FieldTypeNullableBool
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*bool, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, v)
	case string:
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*string, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, v)
	case nil:
		// Use type registry to maintain schema consistency across messages
		fieldType, exists := fb.typeRegistry[key]
		if !exists {
			// Default to nullable float64 for unknown fields (common for sensor/metric data)
			fieldType = data.FieldTypeNullableFloat64
			// Don't register the type yet - wait for first non-nil value
		}
		fb.createNullableField(frame, key, fieldType, fieldIndex)
	default:
		// Fallback to nullable string for any complex or unknown scalar
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*string, 1)))
		strVal := fmt.Sprintf("%v", v)
		frame.Fields[fieldIndex].SetConcrete(0, strVal)
	}
}

// createNullableField creates a field with the specified nullable type and leaves the value as nil.
func (fb *FieldBuilder) createNullableField(frame *data.Frame, key string, fieldType data.FieldType, fieldIndex int) {
	switch fieldType {
	case data.FieldTypeNullableFloat64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*float64, 1)))
	case data.FieldTypeNullableInt64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*int64, 1)))
	case data.FieldTypeNullableUint64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*uint64, 1)))
	case data.FieldTypeNullableBool:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*bool, 1)))
	case data.FieldTypeNullableString:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*string, 1)))
	default:
		// Fallback to nullable float64 if type is not recognized
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*float64, 1)))
	}
	// Leave value as nil at fieldIndex (no Set call needed)
}

// addJSONNumberField handles json.Number values, preserving their original numeric type.
func (fb *FieldBuilder) addJSONNumberField(frame *data.Frame, key string, num json.Number, fieldIndex int) {
	coerced := CoerceJSONNumber(num)
	switch cv := coerced.(type) {
	case int64:
		fb.typeRegistry[key] = data.FieldTypeNullableInt64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*int64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, cv)
	case float64:
		fb.typeRegistry[key] = data.FieldTypeNullableFloat64
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*float64, 1)))
		frame.Fields[fieldIndex].SetConcrete(0, cv)
	default:
		fb.typeRegistry[key] = data.FieldTypeNullableString
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]*string, 1)))
		strVal := fmt.Sprintf("%v", cv)
		frame.Fields[fieldIndex].SetConcrete(0, strVal)
	}
}
