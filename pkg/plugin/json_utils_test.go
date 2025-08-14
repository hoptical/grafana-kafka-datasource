package plugin

import (
	"encoding/json"
	"testing"
)

func TestFlattenJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected map[string]interface{}
		maxDepth int
		maxCap   int
	}{
		{
			name: "simple flat object",
			input: map[string]interface{}{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			},
			expected: map[string]interface{}{
				"key1": "value1",
				"key2": 42,
				"key3": true,
			},
			maxDepth: 5,
			maxCap:   100,
		},
		{
			name: "nested object",
			input: map[string]interface{}{
				"host": map[string]interface{}{
					"name": "server1",
					"ip":   "127.0.0.1",
				},
				"metrics": map[string]interface{}{
					"cpu": map[string]interface{}{
						"load": 0.8,
						"temp": 75.5,
					},
					"memory": map[string]interface{}{
						"used": 1024,
						"free": 2048,
					},
				},
			},
			expected: map[string]interface{}{
				"host.name":           "server1",
				"host.ip":             "127.0.0.1",
				"metrics.cpu.load":    0.8,
				"metrics.cpu.temp":    75.5,
				"metrics.memory.used": 1024,
				"metrics.memory.free": 2048,
			},
			maxDepth: 5,
			maxCap:   100,
		},
		{
			name: "array handling",
			input: map[string]interface{}{
				"tags": []interface{}{"prod", "web", "us-east"},
				"data": map[string]interface{}{
					"values": []interface{}{1, 2, 3},
				},
			},
			expected: map[string]interface{}{
				"tags":        `["prod","web","us-east"]`,
				"data.values": `[1,2,3]`,
			},
			maxDepth: 5,
			maxCap:   100,
		},
		{
			name: "depth limit",
			input: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": map[string]interface{}{
							"level4": "should be included",
						},
					},
				},
			},
			expected: map[string]interface{}{
				"level1.level2.level3.level4": "should be included",
			},
			maxDepth: 5,
			maxCap:   100,
		},
		{
			name: "depth limit exceeded",
			input: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": "should be excluded",
					},
				},
			},
			expected: map[string]interface{}{},
			maxDepth: 1,
			maxCap:   100,
		},
		{
			name: "field cap limit",
			input: map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
			expected: map[string]interface{}{
				// Map iteration order is not guaranteed, so we just check count
			},
			maxDepth: 5,
			maxCap:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := make(map[string]interface{})
			FlattenJSON("", tt.input, result, 0, tt.maxDepth, tt.maxCap)

			// Special handling for field cap limit test
			if tt.name == "field cap limit" {
				if len(result) != 2 {
					t.Errorf("Expected exactly 2 fields due to cap limit, got %d", len(result))
				}
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d fields, got %d", len(tt.expected), len(result))
			}

			for key, expectedValue := range tt.expected {
				if actualValue, exists := result[key]; !exists {
					t.Errorf("Missing key: %s", key)
				} else if actualValue != expectedValue {
					t.Errorf("Key %s: expected %v, got %v", key, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestCoerceJSONNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected interface{}
	}{
		{
			name:     "integer",
			input:    "42",
			expected: int64(42),
		},
		{
			name:     "negative integer",
			input:    "-123",
			expected: int64(-123),
		},
		{
			name:     "float",
			input:    "3.14",
			expected: float64(3.14),
		},
		{
			name:     "scientific notation",
			input:    "1.23e4",
			expected: float64(12300),
		},
		{
			name:     "invalid number",
			input:    "not-a-number",
			expected: "not-a-number",
		},
		{
			name:     "zero",
			input:    "0",
			expected: int64(0),
		},
		{
			name:     "large number",
			input:    "9223372036854775807", // max int64
			expected: int64(9223372036854775807),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			num := json.Number(tt.input)
			result := CoerceJSONNumber(num)

			if result != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)", tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected int64
	}{
		{"int", int(42), int64(42)},
		{"int8", int8(42), int64(42)},
		{"int16", int16(42), int64(42)},
		{"int32", int32(42), int64(42)},
		{"int64", int64(42), int64(42)},
		{"negative", int(-42), int64(-42)},
		{"zero", int(0), int64(0)},
		{"unsupported type", "string", int64(0)},
		{"nil", nil, int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt64(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestToUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected uint64
	}{
		{"uint", uint(42), uint64(42)},
		{"uint8", uint8(42), uint64(42)},
		{"uint16", uint16(42), uint64(42)},
		{"uint32", uint32(42), uint64(42)},
		{"uint64", uint64(42), uint64(42)},
		{"zero", uint(0), uint64(0)},
		{"unsupported type", "string", uint64(0)},
		{"nil", nil, uint64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toUint64(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}
