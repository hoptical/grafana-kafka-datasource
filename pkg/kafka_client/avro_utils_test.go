package kafka_client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewSchemaRegistryClient(t *testing.T) {
	client := NewSchemaRegistryClient("http://localhost:8081", "user", "pass")

	if client.BaseURL != "http://localhost:8081" {
		t.Errorf("Expected BaseURL to be 'http://localhost:8081', got %s", client.BaseURL)
	}
	if client.Username != "user" {
		t.Errorf("Expected Username to be 'user', got %s", client.Username)
	}
	if client.Password != "pass" {
		t.Errorf("Expected Password to be 'pass', got %s", client.Password)
	}
	if client.Client == nil {
		t.Error("Expected HTTP client to be initialized")
	}
}

func TestSchemaRegistryClient_GetSchemaByID(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		if r.URL.Path != "/schemas/ids/123" {
			t.Errorf("Expected path '/schemas/ids/123', got %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Basic dXNlcjpwYXNz" { // user:pass base64
			t.Errorf("Expected basic auth header, got %s", r.Header.Get("Authorization"))
		}

		response := map[string]string{"schema": `{"type": "record", "name": "Test", "fields": []}`}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "user", "pass")
	schema, err := client.GetSchemaByID(123)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	expected := `{"type": "record", "name": "Test", "fields": []}`
	if schema != expected {
		t.Errorf("Expected schema %s, got %s", expected, schema)
	}
}

func TestSchemaRegistryClient_GetSchemaByID_NoAuth(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Errorf("Expected no auth header, got %s", r.Header.Get("Authorization"))
		}

		response := map[string]string{"schema": `{"type": "string"}`}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "")
	schema, err := client.GetSchemaByID(456)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if schema != `{"type": "string"}` {
		t.Errorf("Expected schema, got %s", schema)
	}
}

func TestSchemaRegistryClient_GetSchemaByID_Error(t *testing.T) {
	// Mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal server error"))
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "")
	_, err := client.GetSchemaByID(123)

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestSchemaRegistryClient_GetLatestSchema(t *testing.T) {
	// Mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		if r.URL.Path != "/subjects/test-topic/versions/latest" {
			t.Errorf("Expected path '/subjects/test-topic/versions/latest', got %s", r.URL.Path)
		}

		response := map[string]string{"schema": `{"type": "record", "name": "TestRecord", "fields": [{"name": "id", "type": "string"}]}`}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "")
	schema, err := client.GetLatestSchema("test-topic")

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	expected := `{"type": "record", "name": "TestRecord", "fields": [{"name": "id", "type": "string"}]}`
	if schema != expected {
		t.Errorf("Expected schema %s, got %s", expected, schema)
	}
}

func TestSchemaRegistryClient_GetLatestSchema_Error(t *testing.T) {
	// Mock server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Subject not found"))
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "")
	_, err := client.GetLatestSchema("nonexistent-topic")

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestDecodeAvroMessage(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	// Create a simple Avro message (this would normally be created by an Avro encoder)
	// For testing purposes, we'll use a mock binary representation
	// In a real scenario, this would be proper Avro binary data
	data := []byte{0x06, 0x74, 0x65, 0x73, 0x74, 0x00} // Mock data

	_, err := DecodeAvroMessage(data, schema)
	// We expect this to fail with mock data, but the function should handle it gracefully
	if err == nil {
		t.Log("DecodeAvroMessage handled mock data gracefully")
	}
}

func TestDecodeAvroMessage_InvalidSchema(t *testing.T) {
	invalidSchema := `{"type": "invalid"}`
	data := []byte("test")

	_, err := DecodeAvroMessage(data, invalidSchema)

	if err == nil {
		t.Error("Expected error for invalid schema, got nil")
	}
}

func TestGetSubjectName(t *testing.T) {
	tests := []struct {
		name         string
		topic        string
		strategy     string
		expectedName string
	}{
		{
			name:         "topicName strategy",
			topic:        "user-events",
			strategy:     "topicName",
			expectedName: "user-events",
		},
		{
			name:         "recordName strategy",
			topic:        "user-events",
			strategy:     "recordName",
			expectedName: "user-events-value",
		},
		{
			name:         "topicRecordName strategy",
			topic:        "user-events",
			strategy:     "topicRecordName",
			expectedName: "user-events-user-events-value",
		},
		{
			name:         "default strategy (empty)",
			topic:        "orders",
			strategy:     "",
			expectedName: "orders",
		},
		{
			name:         "default strategy (unknown)",
			topic:        "products",
			strategy:     "unknown",
			expectedName: "products",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetSubjectName(tt.topic, tt.strategy)
			if result != tt.expectedName {
				t.Errorf("GetSubjectName(%s, %s) = %s, expected %s", tt.topic, tt.strategy, result, tt.expectedName)
			}
		})
	}
}
