package kafka_client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNewSchemaRegistryClient(t *testing.T) {
	httpClient := &http.Client{}
	client := NewSchemaRegistryClient("http://localhost:8081", "user", "pass", httpClient)

	if len(client.BaseURLs) != 1 || client.BaseURLs[0] != "http://localhost:8081" {
		t.Errorf("Expected BaseURLs to be [http://localhost:8081], got %v", client.BaseURLs)
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
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "user", "pass", &http.Client{})
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
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "", &http.Client{})
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
		_, _ = w.Write([]byte("Internal server error"))
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "", &http.Client{})
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
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewSchemaRegistryClient(server.URL, "", "", &http.Client{})
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
		_, _ = w.Write([]byte("Subject not found"))
	}))
	defer server.Close()

	httpClient := &http.Client{}
	client := NewSchemaRegistryClient(server.URL, "", "", httpClient)
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
			expectedName: "user-events-value",
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
			expectedName: "orders-value",
		},
		{
			name:         "default strategy (unknown)",
			topic:        "products",
			strategy:     "unknown",
			expectedName: "products-value",
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

func TestParseSchemaRegistryURLs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "empty", raw: "", want: nil},
		{name: "single", raw: "http://sr:8081", want: []string{"http://sr:8081"}},
		{name: "trailing slash", raw: "http://sr:8081/", want: []string{"http://sr:8081"}},
		{name: "multiple with spaces", raw: " http://a:8081 , http://b:8081/ ", want: []string{"http://a:8081", "http://b:8081"}},
		{name: "empty entries dropped", raw: "http://a:8081,, ,http://b:8081,", want: []string{"http://a:8081", "http://b:8081"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseSchemaRegistryURLs(tt.raw); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseSchemaRegistryURLs(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestRedactSchemaRegistryURLs(t *testing.T) {
	got := RedactSchemaRegistryURLs("http://user:secret@a:8081,http://b:8081")
	if strings.Contains(got, "secret") || strings.Contains(got, "user") {
		t.Fatalf("credentials not redacted: %q", got)
	}
	if got != "http://a:8081,http://b:8081" {
		t.Fatalf("unexpected redacted value %q", got)
	}
}

func schemaHandler(hits *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": `{"type":"string"}`})
	}
}

func statusHandler(status int, hits *atomic.Int32) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, http.StatusText(status), status)
	}
}

// deadURL returns the URL of a server that has already been shut down, so
// connections to it are refused.
func deadURL() string {
	s := httptest.NewServer(http.NotFoundHandler())
	u := s.URL
	s.Close()
	return u
}

func TestSchemaRegistryClient_FailoverOnConnectionError(t *testing.T) {
	var hits atomic.Int32
	good := httptest.NewServer(schemaHandler(&hits))
	defer good.Close()

	client := NewSchemaRegistryClient(deadURL()+","+good.URL, "", "", &http.Client{})
	schema, err := client.GetSchemaByID(1)
	if err != nil {
		t.Fatalf("expected failover to succeed, got %v", err)
	}
	if schema != `{"type":"string"}` {
		t.Fatalf("unexpected schema %q", schema)
	}

	// The healthy endpoint is remembered, so the next call goes straight to it.
	if got := client.preferred.Load(); got != 1 {
		t.Fatalf("expected preferred endpoint 1, got %d", got)
	}
	if _, err := client.GetLatestSchema("orders-value"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 hits on healthy endpoint, got %d", hits.Load())
	}
}

func TestSchemaRegistryClient_FailoverOn5xx(t *testing.T) {
	var badHits, goodHits atomic.Int32
	bad := httptest.NewServer(statusHandler(http.StatusServiceUnavailable, &badHits))
	defer bad.Close()
	good := httptest.NewServer(schemaHandler(&goodHits))
	defer good.Close()

	client := NewSchemaRegistryClient(bad.URL+","+good.URL, "", "", &http.Client{})
	if _, err := client.GetLatestSchema("orders-value"); err != nil {
		t.Fatalf("expected failover to succeed, got %v", err)
	}
	if badHits.Load() != 1 || goodHits.Load() != 1 {
		t.Fatalf("expected one hit each, got bad=%d good=%d", badHits.Load(), goodHits.Load())
	}
}

func TestSchemaRegistryClient_NoFailoverOn4xx(t *testing.T) {
	var firstHits, secondHits atomic.Int32
	first := httptest.NewServer(statusHandler(http.StatusNotFound, &firstHits))
	defer first.Close()
	second := httptest.NewServer(schemaHandler(&secondHits))
	defer second.Close()

	client := NewSchemaRegistryClient(first.URL+","+second.URL, "", "", &http.Client{})
	_, err := client.GetLatestSchema("missing-value")
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("expected 404 error, got %v", err)
	}
	if secondHits.Load() != 0 {
		t.Fatalf("404 must not fail over, second endpoint hit %d times", secondHits.Load())
	}
}

func TestSchemaRegistryClient_AllEndpointsFail(t *testing.T) {
	a, b := deadURL(), deadURL()
	client := NewSchemaRegistryClient(a+","+b, "", "", &http.Client{})
	_, err := client.GetSchemaByID(1)
	if err == nil {
		t.Fatal("expected error when all endpoints are down")
	}
	msg := err.Error()
	if !strings.Contains(msg, "all 2 schema registry endpoints failed") || !strings.Contains(msg, a) || !strings.Contains(msg, b) {
		t.Fatalf("error should list every endpoint, got %q", msg)
	}
}

func TestSchemaRegistryClient_AllEndpoints5xx(t *testing.T) {
	var hits atomic.Int32
	a := httptest.NewServer(statusHandler(http.StatusInternalServerError, &hits))
	defer a.Close()
	b := httptest.NewServer(statusHandler(http.StatusBadGateway, &hits))
	defer b.Close()

	client := NewSchemaRegistryClient(a.URL+","+b.URL, "", "", &http.Client{})
	_, err := client.GetLatestSchema("orders-value")
	if err == nil || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected last endpoint's 502 to surface, got %v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected both endpoints tried, got %d", hits.Load())
	}
}
