package plugin

import (
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func TestFieldBuilder_AddValueToFrame(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		value         interface{}
		expectedType  string
		expectedValue interface{}
	}{
		{
			name:          "string value",
			key:           "message",
			value:         "hello world",
			expectedType:  "string",
			expectedValue: "hello world",
		},
		{
			name:          "int64 value",
			key:           "count",
			value:         int64(42),
			expectedType:  "int64",
			expectedValue: int64(42),
		},
		{
			name:          "float64 value",
			key:           "temperature",
			value:         float64(23.5),
			expectedType:  "float64",
			expectedValue: float64(23.5),
		},
		{
			name:          "bool value true",
			key:           "active",
			value:         true,
			expectedType:  "bool",
			expectedValue: true,
		},
		{
			name:          "bool value false",
			key:           "inactive",
			value:         false,
			expectedType:  "bool",
			expectedValue: false,
		},
		{
			name:          "nil value",
			key:           "empty",
			value:         nil,
			expectedType:  "string",
			expectedValue: "null",
		},
		{
			name:          "complex unsupported type",
			key:           "complex",
			value:         map[string]string{"nested": "object"},
			expectedType:  "string",
			expectedValue: "map[nested:object]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := NewFieldBuilder()
			frame := data.NewFrame("test")

			fb.AddValueToFrame(frame, tt.key, tt.value, 0)

			if len(frame.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(frame.Fields))
			}

			field := frame.Fields[0]
			if field.Name != tt.key {
				t.Errorf("Expected field name %s, got %s", tt.key, field.Name)
			}

			actualValue := field.At(0)
			if actualValue != tt.expectedValue {
				t.Errorf("Expected value %v (%T), got %v (%T)", tt.expectedValue, tt.expectedValue, actualValue, actualValue)
			}

			// Assert field type when specified
			if tt.expectedType != "" {
				typeMap := map[string]data.FieldType{
					"string":  data.FieldTypeString,
					"int64":   data.FieldTypeInt64,
					"float64": data.FieldTypeFloat64,
					"bool":    data.FieldTypeBool,
				}
				if expectedFT, ok := typeMap[tt.expectedType]; ok {
					if field.Type() != expectedFT {
						t.Errorf("Expected field type %v, got %v", expectedFT, field.Type())
					}
				}
			}
		})
	}
}

func TestFieldBuilder_JSONNumber(t *testing.T) {
	tests := []struct {
		name          string
		jsonNumber    string
		expectedType  string
		expectedValue interface{}
	}{
		{
			name:          "integer json number",
			jsonNumber:    "42",
			expectedType:  "int64",
			expectedValue: int64(42),
		},
		{
			name:          "float json number",
			jsonNumber:    "3.14",
			expectedType:  "float64",
			expectedValue: float64(3.14),
		},
		{
			name:          "scientific notation",
			jsonNumber:    "1e3",
			expectedType:  "float64",
			expectedValue: float64(1000),
		},
		{
			name:          "invalid json number",
			jsonNumber:    "not-a-number",
			expectedType:  "string",
			expectedValue: "not-a-number",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := NewFieldBuilder()
			frame := data.NewFrame("test")
			num := json.Number(tt.jsonNumber)
			fb.AddValueToFrame(frame, "test_field", num, 0)
			if len(frame.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(frame.Fields))
			}
			field := frame.Fields[0]
			actualValue := field.At(0)
			if actualValue != tt.expectedValue {
				t.Errorf("Expected value %v (%T), got %v (%T)", tt.expectedValue, tt.expectedValue, actualValue, actualValue)
			}
			typeMap := map[string]data.FieldType{
				"string":  data.FieldTypeString,
				"int64":   data.FieldTypeInt64,
				"float64": data.FieldTypeFloat64,
			}
			if expectedFT, ok := typeMap[tt.expectedType]; ok {
				if field.Type() != expectedFT {
					t.Errorf("Expected field type %v, got %v", expectedFT, field.Type())
				}
			}
		})
	}
}

func TestFieldBuilder_IntegerTypes(t *testing.T) {
	fb := NewFieldBuilder()
	frame := data.NewFrame("test")

	// Test various integer types
	values := map[string]interface{}{
		"int_val":     int(42),
		"int8_val":    int8(8),
		"int16_val":   int16(16),
		"int32_val":   int32(32),
		"int64_val":   int64(64),
		"uint_val":    uint(42),
		"uint8_val":   uint8(8),
		"uint16_val":  uint16(16),
		"uint32_val":  uint32(32),
		"uint64_val":  uint64(64),
		"float32_val": float32(32.5),
	}

	fieldIndex := 0
	for key, value := range values {
		fb.AddValueToFrame(frame, key, value, fieldIndex)
		fieldIndex++
	}

	// Verify all fields were created
	if len(frame.Fields) != len(values) {
		t.Errorf("Expected %d fields, got %d", len(values), len(frame.Fields))
	}

	// Check specific type conversions
	expectedTypes := map[string]data.FieldType{
		"int_val":     data.FieldTypeInt64,
		"int8_val":    data.FieldTypeInt64,
		"int16_val":   data.FieldTypeInt64,
		"int32_val":   data.FieldTypeInt64,
		"int64_val":   data.FieldTypeInt64,
		"uint_val":    data.FieldTypeUint64,
		"uint8_val":   data.FieldTypeUint64,
		"uint16_val":  data.FieldTypeUint64,
		"uint32_val":  data.FieldTypeUint64,
		"uint64_val":  data.FieldTypeUint64,
		"float32_val": data.FieldTypeFloat64,
	}

	for _, field := range frame.Fields {
		if expectedType, exists := expectedTypes[field.Name]; exists {
			actualType := field.Type()
			if actualType != expectedType {
				t.Errorf("Field %s: expected type %v, got %v", field.Name, expectedType, actualType)
			}
		}
	}
}

func TestNewFieldBuilder(t *testing.T) {
	fb := NewFieldBuilder()
	if fb == nil {
		t.Error("NewFieldBuilder should return a non-nil FieldBuilder")
	}
}
