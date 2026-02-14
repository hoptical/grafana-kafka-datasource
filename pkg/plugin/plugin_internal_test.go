package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/segmentio/kafka-go"
)

type internalMockClient struct {
	schemaRegistryURL      string
	schemaRegistryUsername string
	schemaRegistryPassword string
	httpClient             *http.Client
}

func (m *internalMockClient) NewConnection() error { return nil }

func (m *internalMockClient) GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error) {
	return []int32{0}, nil
}

func (m *internalMockClient) GetTopics(ctx context.Context, prefix string, limit int) ([]string, error) {
	return []string{"topic-a"}, nil
}

func (m *internalMockClient) HealthCheck() error { return nil }

func (m *internalMockClient) NewStreamReader(ctx context.Context, topic string, partition int32, autoOffsetReset string, lastN int32) (*kafka.Reader, error) {
	return nil, nil
}

func (m *internalMockClient) ConsumerPull(ctx context.Context, reader *kafka.Reader, messageFormat string) (kafka_client.KafkaMessage, error) {
	return kafka_client.KafkaMessage{}, errors.New("not implemented")
}

func (m *internalMockClient) Dispose()                          {}
func (m *internalMockClient) GetMessageFormat() string          { return "json" }
func (m *internalMockClient) GetSchemaRegistryUrl() string      { return m.schemaRegistryURL }
func (m *internalMockClient) GetSchemaRegistryUsername() string { return m.schemaRegistryUsername }
func (m *internalMockClient) GetSchemaRegistryPassword() string { return m.schemaRegistryPassword }
func (m *internalMockClient) GetSubjectNamingStrategy() string  { return "recordName" }
func (m *internalMockClient) GetHTTPClient() *http.Client       { return m.httpClient }

func TestGetDatasourceSettings_ParsesSecureJSONAndBounds(t *testing.T) {
	settings, err := getDatasourceSettings(backend.DataSourceInstanceSettings{
		JSONData: []byte(`{
			"bootstrapServers": "localhost:9092",
			"tlsSkipVerify": true,
			"tlsAuthWithCACert": true,
			"tlsAuth": true,
			"flattenMaxDepth": -2,
			"flattenFieldCap": 0
		}`),
		DecryptedSecureJSONData: map[string]string{
			"saslPassword":           "secret",
			"tlsCACert":              "ca-cert",
			"tlsClientCert":          "client-cert",
			"tlsClientKey":           "client-key",
			"schemaRegistryUsername": "sr-user",
			"schemaRegistryPassword": "sr-pass",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !settings.TLSSkipVerify || !settings.TLSAuthWithCACert || !settings.TLSAuth {
		t.Fatalf("expected TLS boolean fields to be true")
	}
	if settings.FlattenMaxDepth != 1 {
		t.Fatalf("expected flattenMaxDepth to be clamped to 1, got %d", settings.FlattenMaxDepth)
	}
	if settings.FlattenFieldCap != defaultFlattenFieldCap {
		t.Fatalf("expected default flattenFieldCap %d, got %d", defaultFlattenFieldCap, settings.FlattenFieldCap)
	}
	if settings.SaslPassword != "secret" || settings.TLSCACert != "ca-cert" || settings.TLSClientCert != "client-cert" || settings.TLSClientKey != "client-key" {
		t.Fatalf("expected secure TLS/SASL fields to be loaded")
	}
	if settings.SchemaRegistryUsername != "sr-user" || settings.SchemaRegistryPassword != "sr-pass" {
		t.Fatalf("expected schema registry credentials to be loaded")
	}
}

func TestGetDatasourceSettings_InvalidJSON(t *testing.T) {
	_, err := getDatasourceSettings(backend.DataSourceInstanceSettings{JSONData: []byte("{invalid")})
	if err == nil {
		t.Fatalf("expected JSON parse error")
	}
}

func TestSendJSON_SuccessAndMarshalFailure(t *testing.T) {
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error {
		sent = *r
		return nil
	})

	if err := sendJSON(sender, 201, map[string]string{"ok": "true"}); err != nil {
		t.Fatalf("unexpected sendJSON error: %v", err)
	}
	if sent.Status != 201 {
		t.Fatalf("expected status 201, got %d", sent.Status)
	}
	if contentType := sent.Headers["Content-Type"]; len(contentType) == 0 || contentType[0] != "application/json" {
		t.Fatalf("expected application/json content type")
	}

	if err := sendJSON(sender, 200, map[string]interface{}{"bad": make(chan int)}); err != nil {
		t.Fatalf("unexpected sendJSON marshal-fallback error: %v", err)
	}
	if sent.Status != 500 {
		t.Fatalf("expected marshal fallback status 500, got %d", sent.Status)
	}
	if !strings.Contains(string(sent.Body), "Failed to marshal response") {
		t.Fatalf("expected marshal fallback message, got %s", string(sent.Body))
	}
}

func TestIsSubjectNotFoundError(t *testing.T) {
	if isSubjectNotFoundError(nil) {
		t.Fatalf("expected nil error to be false")
	}
	if !isSubjectNotFoundError(errors.New("status 404 from registry")) {
		t.Fatalf("expected 404 error to match")
	}
	if !isSubjectNotFoundError(errors.New("Subject not found in schema registry")) {
		t.Fatalf("expected subject-not-found message to match")
	}
	if isSubjectNotFoundError(errors.New("connection reset")) {
		t.Fatalf("expected unrelated error to be false")
	}
}

func TestValidateProtobufSchema(t *testing.T) {
	ds := NewWithClient(&internalMockClient{})

	emptyResult, err := ds.ValidateProtobufSchema("  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if emptyResult.Status != backend.HealthStatusError {
		t.Fatalf("expected error status for empty schema")
	}

	invalidResult, err := ds.ValidateProtobufSchema("message broken {")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if invalidResult.Status != backend.HealthStatusError {
		t.Fatalf("expected error status for invalid schema")
	}

	validResult, err := ds.ValidateProtobufSchema(`syntax = "proto3"; message T { string name = 1; }`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if validResult.Status != backend.HealthStatusOk {
		t.Fatalf("expected ok status for valid schema")
	}
}

func TestValidateSchemaRegistry_Branches(t *testing.T) {
	t.Run("invalid scheme", func(t *testing.T) {
		ds := NewWithClient(&internalMockClient{schemaRegistryURL: "ftp://registry.example"})
		result, err := ds.ValidateSchemaRegistry(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Status != backend.HealthStatusError {
			t.Fatalf("expected error status")
		}
		if !strings.Contains(result.Message, "HTTP or HTTPS") {
			t.Fatalf("expected protocol validation message, got %q", result.Message)
		}
	})

	t.Run("nil http client", func(t *testing.T) {
		ds := NewWithClient(&internalMockClient{schemaRegistryURL: "http://registry.example"})
		result, err := ds.ValidateSchemaRegistry(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Status != backend.HealthStatusError {
			t.Fatalf("expected error status")
		}
		if !strings.Contains(result.Message, "HTTP client not available") {
			t.Fatalf("unexpected message: %q", result.Message)
		}
	})

	t.Run("connectivity success with subject not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Subject not found", http.StatusNotFound)
		}))
		defer server.Close()

		ds := NewWithClient(&internalMockClient{
			schemaRegistryURL: server.URL,
			httpClient:        server.Client(),
		})
		result, err := ds.ValidateSchemaRegistry(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Status != backend.HealthStatusOk {
			t.Fatalf("expected ok status, got %v", result.Status)
		}
	})
}

func TestCallResource_ValidateSchemaEndpoints(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Subject not found", http.StatusNotFound)
	}))
	defer server.Close()

	ds := NewWithClient(&internalMockClient{
		schemaRegistryURL: server.URL,
		httpClient:        server.Client(),
	})

	t.Run("validate schema registry", func(t *testing.T) {
		var sent backend.CallResourceResponse
		sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "validate-schema-registry", Method: "GET"}, sender)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent.Status != 200 {
			t.Fatalf("expected 200, got %d", sent.Status)
		}
	})

	t.Run("validate avro schema invalid json", func(t *testing.T) {
		var sent backend.CallResourceResponse
		sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "validate-avro-schema", Method: "POST", Body: []byte("{")}, sender)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent.Status != 400 {
			t.Fatalf("expected 400, got %d", sent.Status)
		}
	})

	t.Run("validate avro schema missing type", func(t *testing.T) {
		var sent backend.CallResourceResponse
		sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
		body, _ := json.Marshal(map[string]string{"schema": `{"name":"X","fields":[]}`})
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "validate-avro-schema", Method: "POST", Body: body}, sender)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent.Status != 400 {
			t.Fatalf("expected 400, got %d", sent.Status)
		}
	})

	t.Run("validate protobuf schema invalid json", func(t *testing.T) {
		var sent backend.CallResourceResponse
		sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "validate-protobuf-schema", Method: "POST", Body: []byte("{")}, sender)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent.Status != 400 {
			t.Fatalf("expected 400, got %d", sent.Status)
		}
	})

	t.Run("validate protobuf schema valid", func(t *testing.T) {
		var sent backend.CallResourceResponse
		sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
		body, _ := json.Marshal(map[string]string{"schema": `syntax = "proto3"; message T { string name = 1; }`})
		err := ds.CallResource(context.Background(), &backend.CallResourceRequest{Path: "validate-protobuf-schema", Method: "POST", Body: body}, sender)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sent.Status != 200 {
			t.Fatalf("expected 200, got %d", sent.Status)
		}
	})
}

func TestProcessMessageToFrame_Branches(t *testing.T) {
	client := &internalMockClient{}
	fieldBuilder := NewFieldBuilder()

	t.Run("message error creates error frame", func(t *testing.T) {
		msg := kafka_client.KafkaMessage{
			Timestamp: time.Now(),
			Offset:    10,
			Error:     errors.New("decode failed"),
		}
		frame, err := ProcessMessageToFrame(client, msg, 1, []int32{0, 1}, &StreamConfig{MessageFormat: "json"}, "topic-a", 5, 100, fieldBuilder)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if frame == nil || len(frame.Fields) == 0 {
			t.Fatalf("expected non-empty frame")
		}
		if !frameHasField(frame, "error") {
			t.Fatalf("expected error field in frame")
		}
	})

	t.Run("json decode failure returns error frame", func(t *testing.T) {
		msg := kafka_client.KafkaMessage{
			RawValue:  []byte("{bad-json"),
			Timestamp: time.Now(),
			Offset:    11,
			Value:     nil,
		}
		frame, err := ProcessMessageToFrame(client, msg, 0, []int32{0}, &StreamConfig{MessageFormat: "json"}, "topic-a", 5, 100, fieldBuilder)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !frameHasField(frame, "error") {
			t.Fatalf("expected error field in frame")
		}
	})

	t.Run("top-level array flattening and timestamp now", func(t *testing.T) {
		before := time.Now().Add(-1 * time.Second)
		msg := kafka_client.KafkaMessage{
			Value: []interface{}{
				map[string]interface{}{"id": 1.0, "name": "first"},
				map[string]interface{}{"id": 2.0},
			},
			Timestamp: time.Now().Add(-1 * time.Hour),
			Offset:    12,
		}
		frame, err := ProcessMessageToFrame(client, msg, 0, []int32{0}, &StreamConfig{MessageFormat: "json", TimestampMode: "now"}, "topic-a", 5, 100, fieldBuilder)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !frameHasField(frame, "item_0.id") || !frameHasField(frame, "item_0.name") || !frameHasField(frame, "item_1.id") {
			t.Fatalf("expected flattened item_* fields in frame")
		}
		timeField, ok := frame.Fields[0].At(0).(time.Time)
		if !ok {
			t.Fatalf("expected time field to be time.Time")
		}
		if timeField.Before(before) {
			t.Fatalf("expected timestampMode=now to use current time")
		}
	})
}

func frameHasField(frame *data.Frame, name string) bool {
	for _, field := range frame.Fields {
		if field.Name == name {
			return true
		}
	}
	return false
}
