package plugin

import (
	"encoding/base64"
	"testing"
)

func TestDecodeMessageKeyString(t *testing.T) {
	tests := []struct {
		name      string
		rawKey    []byte
		format    string
		expected  interface{}
		shouldAdd bool
		wantErr   bool
	}{
		{
			name:      "valid string key",
			rawKey:    []byte("user-123"),
			format:    "string",
			expected:  "user-123",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "empty key - string format",
			rawKey:    []byte{},
			format:    "string",
			expected:  "",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "string key with special chars",
			rawKey:    []byte("order:2024-01-15:region-us-east"),
			format:    "string",
			expected:  "order:2024-01-15:region-us-east",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "string key with UTF-8 characters",
			rawKey:    []byte("user-ñoño"),
			format:    "string",
			expected:  "user-ñoño",
			shouldAdd: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldAdd, err := decodeMessageKey(tt.rawKey, tt.format)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeMessageKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if shouldAdd != tt.shouldAdd {
				t.Errorf("decodeMessageKey() shouldAdd = %v, want %v", shouldAdd, tt.shouldAdd)
			}
			if result != tt.expected {
				t.Errorf("decodeMessageKey() result = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDecodeMessageKeyJSON(t *testing.T) {
	tests := []struct {
		name      string
		rawKey    []byte
		format    string
		shouldAdd bool
		wantErr   bool
	}{
		{
			name:      "valid JSON object",
			rawKey:    []byte(`{"userId": "123", "region": "us-east"}`),
			format:    "json",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "nested JSON object",
			rawKey:    []byte(`{"user": {"id": 123, "name": "Alice"}, "timestamp": 1234567890}`),
			format:    "json",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "JSON with numbers",
			rawKey:    []byte(`{"count": 42, "price": 19.99, "id": "abc"}`),
			format:    "json",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "invalid JSON",
			rawKey:    []byte(`{invalid json`),
			format:    "json",
			shouldAdd: false, // Should skip, not fail
			wantErr:   false,
		},
		{
			name:      "JSON array (not object)",
			rawKey:    []byte(`["item1", "item2"]`),
			format:    "json",
			shouldAdd: false, // Should skip arrays
			wantErr:   false,
		},
		{
			name:      "JSON primitive string",
			rawKey:    []byte(`"just-a-string"`),
			format:    "json",
			shouldAdd: false, // Should skip primitives
			wantErr:   false,
		},
		{
			name:      "JSON primitive number",
			rawKey:    []byte(`42`),
			format:    "json",
			shouldAdd: false,
			wantErr:   false,
		},
		{
			name:      "JSON primitive boolean",
			rawKey:    []byte(`true`),
			format:    "json",
			shouldAdd: false,
			wantErr:   false,
		},
		{
			name:      "JSON null",
			rawKey:    []byte(`null`),
			format:    "json",
			shouldAdd: false,
			wantErr:   false,
		},
		{
			name:      "empty key - JSON format",
			rawKey:    []byte{},
			format:    "json",
			shouldAdd: false,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldAdd, err := decodeMessageKey(tt.rawKey, tt.format)

			if (err != nil) != tt.wantErr {
				t.Errorf("decodeMessageKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if shouldAdd != tt.shouldAdd {
				t.Errorf("decodeMessageKey() shouldAdd = %v, want %v", shouldAdd, tt.shouldAdd)
			}

			if tt.shouldAdd {
				if result == nil {
					t.Error("decodeMessageKey() result is nil but shouldAdd is true")
				}
				// Verify result is a map[string]interface{} for valid JSON objects
				if _, ok := result.(map[string]interface{}); !ok {
					t.Errorf("decodeMessageKey() result type = %T, want map[string]interface{}", result)
				}
			}
		})
	}
}

func TestDecodeMessageKeyNone(t *testing.T) {
	tests := []struct {
		name   string
		rawKey []byte
		format string
	}{
		{
			name:   "none format - ignores key",
			rawKey: []byte("some-key"),
			format: "none",
		},
		{
			name:   "empty format defaults to none",
			rawKey: []byte("some-key"),
			format: "",
		},
		{
			name:   "unknown format defaults to none",
			rawKey: []byte("some-key"),
			format: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldAdd, err := decodeMessageKey(tt.rawKey, tt.format)
			if err != nil {
				t.Errorf("decodeMessageKey() unexpected error = %v", err)
			}
			if shouldAdd {
				t.Error("decodeMessageKey() shouldAdd = true, want false for 'none' format")
			}
			if result != nil {
				t.Error("decodeMessageKey() result is not nil for 'none' format")
			}
		})
	}
}

func TestDecodeMessageKeyEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		rawKey    []byte
		format    string
		shouldAdd bool
		wantErr   bool
	}{
		{
			name:      "very long string key",
			rawKey:    []byte(string(make([]byte, 10000)) + "key"),
			format:    "string",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "binary data as string",
			rawKey:    []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
			format:    "string",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "whitespace only JSON",
			rawKey:    []byte(`   `),
			format:    "json",
			shouldAdd: false,
			wantErr:   false,
		},
		{
			name:      "empty object JSON",
			rawKey:    []byte(`{}`),
			format:    "json",
			shouldAdd: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldAdd, err := decodeMessageKey(tt.rawKey, tt.format)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeMessageKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if shouldAdd != tt.shouldAdd {
				t.Errorf("decodeMessageKey() shouldAdd = %v, want %v", shouldAdd, tt.shouldAdd)
			}

			if tt.shouldAdd && result == nil {
				t.Error("decodeMessageKey() result is nil but shouldAdd is true")
			}
		})
	}
}

func TestDecodeMessageKeyBase64(t *testing.T) {
	tests := []struct {
		name      string
		rawKey    []byte
		expected  string
		shouldAdd bool
		wantErr   bool
	}{
		{
			name:      "binary bytes encode correctly",
			rawKey:    []byte{0x00, 0x01, 0x02},
			expected:  "AAEC",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "empty key returns empty string",
			rawKey:    []byte{},
			expected:  "",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "single byte",
			rawKey:    []byte{0xFF},
			expected:  "/w==",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "all-zero bytes",
			rawKey:    []byte{0x00, 0x00, 0x00, 0x00},
			expected:  "AAAAAA==",
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "utf8 string round-trips via base64",
			rawKey:    []byte("hello"),
			expected:  base64.StdEncoding.EncodeToString([]byte("hello")),
			shouldAdd: true,
			wantErr:   false,
		},
		{
			name:      "arbitrary binary data",
			rawKey:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			expected:  base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
			shouldAdd: true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldAdd, err := decodeMessageKey(tt.rawKey, "base64")
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeMessageKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if shouldAdd != tt.shouldAdd {
				t.Errorf("decodeMessageKey() shouldAdd = %v, want %v", shouldAdd, tt.shouldAdd)
			}
			got, ok := result.(string)
			if !ok && tt.shouldAdd {
				t.Errorf("decodeMessageKey() result type = %T, want string", result)
				return
			}
			if got != tt.expected {
				t.Errorf("decodeMessageKey() result = %q, want %q", got, tt.expected)
			}
			// Round-trip check: decode base64 back to original bytes
			if tt.shouldAdd && len(tt.rawKey) > 0 {
				decoded, err := base64.StdEncoding.DecodeString(got)
				if err != nil {
					t.Errorf("base64 result is not valid base64: %v", err)
					return
				}
				if string(decoded) != string(tt.rawKey) {
					t.Errorf("round-trip mismatch: decoded %v, want %v", decoded, tt.rawKey)
				}
			}
		})
	}
}
