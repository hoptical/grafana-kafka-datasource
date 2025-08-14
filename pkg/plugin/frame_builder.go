package plugin

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// FieldBuilder handles building Grafana data fields from various JSON value types.
type FieldBuilder struct{}

// NewFieldBuilder creates a new instance of FieldBuilder.
func NewFieldBuilder() *FieldBuilder {
	return &FieldBuilder{}
}

// AddValueToFrame adds a key-value pair from flattened JSON to the data frame,
// creating the appropriate field type based on the value's type.
func (fb *FieldBuilder) AddValueToFrame(frame *data.Frame, key string, value interface{}, fieldIndex int) {
	switch v := value.(type) {
	case json.Number:
		fb.addJSONNumberField(frame, key, v, fieldIndex)
	case float64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]float64, 1)))
		frame.Fields[fieldIndex].Set(0, v)
	case float32:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]float64, 1)))
		frame.Fields[fieldIndex].Set(0, float64(v))
	case int:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]int64, 1)))
		frame.Fields[fieldIndex].Set(0, int64(v))
	case int8, int16, int32, int64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]int64, 1)))
		frame.Fields[fieldIndex].Set(0, toInt64(v))
	case uint, uint8, uint16, uint32, uint64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]uint64, 1)))
		frame.Fields[fieldIndex].Set(0, toUint64(v))
	case bool:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]bool, 1)))
		frame.Fields[fieldIndex].Set(0, v)
	case string:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]string, 1)))
		frame.Fields[fieldIndex].Set(0, v)
	case nil:
		// Represent null as string "null" to avoid mixed-type columns
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]string, 1)))
		frame.Fields[fieldIndex].Set(0, "null")
	default:
		// Fallback to string for any complex or unknown scalar
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]string, 1)))
		frame.Fields[fieldIndex].Set(0, fmt.Sprintf("%v", v))
	}
}

// addJSONNumberField handles json.Number values, preserving their original numeric type.
func (fb *FieldBuilder) addJSONNumberField(frame *data.Frame, key string, num json.Number, fieldIndex int) {
	coerced := CoerceJSONNumber(num)
	switch cv := coerced.(type) {
	case int64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]int64, 1)))
		frame.Fields[fieldIndex].Set(0, cv)
	case float64:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]float64, 1)))
		frame.Fields[fieldIndex].Set(0, cv)
	default:
		frame.Fields = append(frame.Fields, data.NewField(key, nil, make([]string, 1)))
		frame.Fields[fieldIndex].Set(0, fmt.Sprintf("%v", cv))
	}
}
