package plugin

import (
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// newFrameWithCapacity creates a frame with pre-allocated fields slice.
func newFrameWithCapacity(name string, numFields int) *data.Frame {
	frame := data.NewFrame(name)
	frame.Fields = make([]*data.Field, numFields)
	return frame
}

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
			name:          "nil value defaults to nullable float64",
			key:           "empty",
			value:         nil,
			expectedType:  "nullable-float64",
			expectedValue: (*float64)(nil),
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
			frame := newFrameWithCapacity("test", 1)

			fb.AddValueToFrame(frame, tt.key, tt.value, 0)

			if len(frame.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(frame.Fields))
			}

			field := frame.Fields[0]
			if field.Name != tt.key {
				t.Errorf("Expected field name %s, got %s", tt.key, field.Name)
			}

			actualValue := field.At(0)
			// For nullable types, we need to compare the dereferenced values
			var compareValue interface{}
			switch v := actualValue.(type) {
			case *string:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*string)(nil)
				}
			case *int64:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*int64)(nil)
				}
			case *float64:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*float64)(nil)
				}
			case *bool:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*bool)(nil)
				}
			default:
				compareValue = actualValue
			}
			if compareValue != tt.expectedValue {
				t.Errorf("Expected value %v (%T), got %v (%T)", tt.expectedValue, tt.expectedValue, compareValue, compareValue)
			} // Assert field type when specified
			if tt.expectedType != "" {
				typeMap := map[string]data.FieldType{
					"string":           data.FieldTypeNullableString,
					"int64":            data.FieldTypeNullableInt64,
					"float64":          data.FieldTypeNullableFloat64,
					"bool":             data.FieldTypeNullableBool,
					"nullable-float64": data.FieldTypeNullableFloat64,
					"nullable-int64":   data.FieldTypeNullableInt64,
					"nullable-string":  data.FieldTypeNullableString,
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
			frame := newFrameWithCapacity("test", 1)
			num := json.Number(tt.jsonNumber)
			fb.AddValueToFrame(frame, "test_field", num, 0)
			if len(frame.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(frame.Fields))
			}
			field := frame.Fields[0]
			actualValue := field.At(0)
			// For nullable types, we need to compare the dereferenced values
			var compareValue interface{}
			switch v := actualValue.(type) {
			case *string:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*string)(nil)
				}
			case *int64:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*int64)(nil)
				}
			case *float64:
				if v != nil {
					compareValue = *v
				} else {
					compareValue = (*float64)(nil)
				}
			default:
				compareValue = actualValue
			}
			if compareValue != tt.expectedValue {
				t.Errorf("Expected value %v (%T), got %v (%T)", tt.expectedValue, tt.expectedValue, compareValue, compareValue)
			}
			typeMap := map[string]data.FieldType{
				"string":  data.FieldTypeNullableString,
				"int64":   data.FieldTypeNullableInt64,
				"float64": data.FieldTypeNullableFloat64,
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

	frame := newFrameWithCapacity("test", len(values))

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
		"int_val":     data.FieldTypeNullableInt64,
		"int8_val":    data.FieldTypeNullableInt64,
		"int16_val":   data.FieldTypeNullableInt64,
		"int32_val":   data.FieldTypeNullableInt64,
		"int64_val":   data.FieldTypeNullableInt64,
		"uint_val":    data.FieldTypeNullableUint64,
		"uint8_val":   data.FieldTypeNullableUint64,
		"uint16_val":  data.FieldTypeNullableUint64,
		"uint32_val":  data.FieldTypeNullableUint64,
		"uint64_val":  data.FieldTypeNullableUint64,
		"float32_val": data.FieldTypeNullableFloat64,
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
		t.Fatal("NewFieldBuilder should return a non-nil FieldBuilder")
	}
	if fb.typeRegistry == nil {
		t.Error("NewFieldBuilder should initialize typeRegistry")
	}
}

func TestFieldBuilder_NullHandling(t *testing.T) {
	tests := []struct {
		name         string
		sequence     []interface{} // Values to add in sequence
		key          string
		expectedType data.FieldType
		expectNilAt  []int // Indices where value should be nil
	}{
		{
			name:         "int64 -> nil -> int64 maintains type",
			sequence:     []interface{}{int64(122), nil, int64(126)},
			key:          "sensor",
			expectedType: data.FieldTypeNullableInt64,
			expectNilAt:  []int{1},
		},
		{
			name:         "float64 -> nil -> float64 maintains type",
			sequence:     []interface{}{float64(23.5), nil, float64(24.0)},
			key:          "temperature",
			expectedType: data.FieldTypeNullableFloat64,
			expectNilAt:  []int{1},
		},
		{
			name:         "string -> nil -> string maintains type",
			sequence:     []interface{}{"hello", nil, "world"},
			key:          "message",
			expectedType: data.FieldTypeNullableString,
			expectNilAt:  []int{1},
		},
		{
			name:         "bool -> nil -> bool maintains type",
			sequence:     []interface{}{true, nil, false},
			key:          "active",
			expectedType: data.FieldTypeNullableBool,
			expectNilAt:  []int{1},
		},
		{
			name:         "multiple nulls in sequence",
			sequence:     []interface{}{int64(1), nil, nil, int64(2), nil},
			key:          "count",
			expectedType: data.FieldTypeNullableInt64,
			expectNilAt:  []int{1, 2, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb := NewFieldBuilder()

			for i, val := range tt.sequence {
				frame := newFrameWithCapacity("test", 1)
				fb.AddValueToFrame(frame, tt.key, val, 0)

				if len(frame.Fields) != 1 {
					t.Fatalf("Step %d: Expected 1 field, got %d", i, len(frame.Fields))
				}

				field := frame.Fields[0]

				// Check field type consistency
				if field.Type() != tt.expectedType {
					t.Errorf("Step %d: Expected field type %v, got %v", i, tt.expectedType, field.Type())
				}

				// Check nil values
				isNil := false
				for _, nilIdx := range tt.expectNilAt {
					if nilIdx == i {
						isNil = true
						break
					}
				}

				if isNil {
					if !field.Nullable() {
						t.Errorf("Step %d: Field should be nullable", i)
					}
					if !field.NilAt(0) {
						t.Errorf("Step %d: Expected nil value at index 0", i)
					}
				} else {
					if field.NilAt(0) {
						t.Errorf("Step %d: Expected non-nil value at index 0", i)
					}
				}
			}

			// Verify type registry was updated
			storedType, exists := fb.typeRegistry[tt.key]
			if !exists {
				t.Errorf("Type registry should have entry for key %q", tt.key)
			}
			if storedType != tt.expectedType {
				t.Errorf("Registry type %v doesn't match expected %v", storedType, tt.expectedType)
			}
		})
	}
}

func TestFieldBuilder_NullableBoolHandling(t *testing.T) {
	fb := NewFieldBuilder()

	// Test bool -> nil -> bool
	sequence := []interface{}{true, nil, false}
	for i, val := range sequence {
		frame := newFrameWithCapacity("test", 1)
		fb.AddValueToFrame(frame, "flag", val, 0)

		if len(frame.Fields) != 1 {
			t.Fatalf("Step %d: Expected 1 field, got %d", i, len(frame.Fields))
		}

		field := frame.Fields[0]
		if field.Type() != data.FieldTypeNullableBool {
			t.Errorf("Step %d: Expected NullableBool, got %v", i, field.Type())
		}

		if i == 1 { // nil case
			if !field.NilAt(0) {
				t.Errorf("Step %d: Expected nil value", i)
			}
		} else {
			if field.NilAt(0) {
				t.Errorf("Step %d: Expected non-nil value", i)
			}
		}
	}
}

func TestFieldBuilder_NilFirstBehavior(t *testing.T) {
	fb := NewFieldBuilder()

	// Step 0: nil comes first - should default to float64
	frame1 := newFrameWithCapacity("test", 1)
	fb.AddValueToFrame(frame1, "sensor", nil, 0)

	if len(frame1.Fields) != 1 {
		t.Fatalf("Step 0: Expected 1 field, got %d", len(frame1.Fields))
	}

	field1 := frame1.Fields[0]
	if field1.Type() != data.FieldTypeNullableFloat64 {
		t.Errorf("Step 0: Expected NullableFloat64 (default for unknown nil), got %v", field1.Type())
	}
	if !field1.NilAt(0) {
		t.Errorf("Step 0: Expected nil value")
	}

	// Step 1: int64 comes next - should register as int64 in registry for future messages
	frame2 := newFrameWithCapacity("test", 1)
	fb.AddValueToFrame(frame2, "sensor", int64(100), 0)

	if len(frame2.Fields) != 1 {
		t.Fatalf("Step 1: Expected 1 field, got %d", len(frame2.Fields))
	}

	field2 := frame2.Fields[0]
	if field2.Type() != data.FieldTypeNullableInt64 {
		t.Errorf("Step 1: Expected NullableInt64, got %v", field2.Type())
	}

	// Verify registry was updated
	if storedType, exists := fb.typeRegistry["sensor"]; !exists {
		t.Errorf("Type registry should have entry for 'sensor'")
	} else if storedType != data.FieldTypeNullableInt64 {
		t.Errorf("Registry type should be NullableInt64, got %v", storedType)
	}

	// Step 2: nil comes again - should now use int64 from registry
	frame3 := newFrameWithCapacity("test", 1)
	fb.AddValueToFrame(frame3, "sensor", nil, 0)

	if len(frame3.Fields) != 1 {
		t.Fatalf("Step 2: Expected 1 field, got %d", len(frame3.Fields))
	}

	field3 := frame3.Fields[0]
	if field3.Type() != data.FieldTypeNullableInt64 {
		t.Errorf("Step 2: Expected NullableInt64 (from registry), got %v", field3.Type())
	}
	if !field3.NilAt(0) {
		t.Errorf("Step 2: Expected nil value")
	}
}
