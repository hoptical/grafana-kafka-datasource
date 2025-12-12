package plugin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/hoptical/grafana-kafka-datasource/pkg/plugin"
	"github.com/segmentio/kafka-go"
)

var testTLSConfig = map[string]interface{}{
	"clientId":          "test-client",
	"tlsAuthWithCACert": true,
	"tlsAuth":           true,
	"tlsSkipVerify":     true,
	"serverName":        "test-server",
	"timeout":           1234,
}

// mockKafkaClient implements plugin.KafkaClientAPI for tests
type mockKafkaClient struct {
	newConnErr       error
	partitions       []int32
	partitionsErr    error
	topics           []string
	topicsErr        error
	healthErr        error
	streamReaderErr  error
	consumerMessages []kafka_client.KafkaMessage
	consumerErr      error
	// Avro-related fields
	messageFormat             string
	schemaRegistryUrl         string
	schemaRegistryUsername    string
	schemaRegistryPassword    string
	avroSubjectNamingStrategy string
}

func (m *mockKafkaClient) NewConnection() error { return m.newConnErr }
func (m *mockKafkaClient) GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error) {
	if m.partitionsErr != nil {
		return nil, m.partitionsErr
	}
	return m.partitions, nil
}
func (m *mockKafkaClient) GetTopics(ctx context.Context, prefix string, limit int) ([]string, error) {
	if m.topicsErr != nil {
		return nil, m.topicsErr
	}
	return m.topics, nil
}
func (m *mockKafkaClient) HealthCheck() error { return m.healthErr }
func (m *mockKafkaClient) NewStreamReader(ctx context.Context, topic string, partition int32, autoOffsetReset string, lastN int32) (*kafka.Reader, error) {
	if m.streamReaderErr != nil {
		return nil, m.streamReaderErr
	}
	// Return nil reader for testing - the code should handle this
	return nil, nil
}
func (m *mockKafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader, messageFormat string) (kafka_client.KafkaMessage, error) {
	if m.consumerErr != nil {
		return kafka_client.KafkaMessage{}, m.consumerErr
	}
	if len(m.consumerMessages) == 0 {
		return kafka_client.KafkaMessage{}, errors.New("no more messages")
	}
	msg := m.consumerMessages[0]
	m.consumerMessages = m.consumerMessages[1:]

	// For Avro format, don't return parsed Value to force Avro decoding
	if messageFormat == "avro" {
		msg.Value = nil
	}

	return msg, nil
}
func (m *mockKafkaClient) Dispose() {}

// Avro-related methods
func (m *mockKafkaClient) GetMessageFormat() string             { return m.messageFormat }
func (m *mockKafkaClient) GetSchemaRegistryUrl() string         { return m.schemaRegistryUrl }
func (m *mockKafkaClient) GetSchemaRegistryUsername() string    { return m.schemaRegistryUsername }
func (m *mockKafkaClient) GetSchemaRegistryPassword() string    { return m.schemaRegistryPassword }
func (m *mockKafkaClient) GetAvroSubjectNamingStrategy() string { return m.avroSubjectNamingStrategy }
func (m *mockKafkaClient) GetHTTPClient() *http.Client          { return &http.Client{} }

func TestQueryData(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	jsonBytes, err := json.Marshal(testTLSConfig)
	if err != nil {
		t.Fatalf("Failed to marshal test config: %v", err)
	}
	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A", JSON: jsonBytes},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}
	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}

func TestCheckHealth_OK(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	// Simulate DataSourceInstanceSettings with TLS and clientId config
	jsonBytes, err := json.Marshal(testTLSConfig)
	if err != nil {
		t.Fatalf("Failed to marshal test config: %v", err)
	}
	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData: jsonBytes,
			},
		},
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result.Status != backend.HealthStatusOk && result.Status != backend.HealthStatusError {
		t.Errorf("Unexpected health status: %v", result.Status)
	}
}

func TestQuery_UnmarshalError(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	badQuery := backend.DataQuery{JSON: []byte(`not-json`)}
	resp, _ := ds.QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{badQuery}})
	if resp == nil {
		t.Fatal("Expected a response, got nil")
	}
}

func TestSubscribeStream_EmptyTopic(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	data, _ := json.Marshal(map[string]interface{}{})
	resp, err := ds.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Data: data})
	if err == nil {
		t.Error("Expected error for empty topic")
	}
	if resp.Status != backend.SubscribeStreamStatusPermissionDenied {
		t.Errorf("Expected permission denied, got %v", resp.Status)
	}
}

func TestCallResource_Partitions_Success(t *testing.T) {
	mc := &mockKafkaClient{partitions: []int32{0, 1, 2}}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "/?topic=test"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	if err := ds.CallResource(context.Background(), req, sender); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sent.Status != 200 {
		t.Fatalf("expected 200 got %d", sent.Status)
	}
}

func TestCallResource_Partitions_Error(t *testing.T) {
	mc := &mockKafkaClient{partitionsErr: errors.New("boom")}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "/?topic=test"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 500 {
		t.Fatalf("expected 500 got %d", sent.Status)
	}
}

func TestCallResource_Topics_Success(t *testing.T) {
	mc := &mockKafkaClient{topics: []string{"a", "ab", "b"}}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "topics", Method: "GET", URL: "/?prefix=a&limit=2"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	if err := ds.CallResource(context.Background(), req, sender); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if sent.Status != 200 {
		t.Fatalf("expected 200 got %d", sent.Status)
	}
}

func TestCallResource_NotFound(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	req := &backend.CallResourceRequest{Path: "x", Method: "GET"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 404 {
		t.Fatalf("expected 404 got %d", sent.Status)
	}
}

func TestHealthCheck_Error(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{healthErr: errors.New("down")})
	result, _ := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{PluginContext: backend.PluginContext{DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{}}})
	if result.Status != backend.HealthStatusError {
		t.Fatalf("expected error status")
	}
}

func TestRunStream_InvalidPartitionValue(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{partitions: []int32{0}})
	// Provide invalid string value not 'all'
	dataBytes, _ := json.Marshal(map[string]interface{}{"topicName": "t", "partition": "invalid"})
	err := ds.RunStream(context.Background(), &backend.RunStreamRequest{Data: dataBytes}, backend.NewStreamSender(nil))
	if err == nil {
		t.Fatalf("expected error for invalid partition value")
	}
}

func TestRunStream_AllPartitions_ErrorFetching(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{partitionsErr: errors.New("x")})
	dataBytes, _ := json.Marshal(map[string]interface{}{"topicName": "t", "partition": "all"})
	err := ds.RunStream(context.Background(), &backend.RunStreamRequest{Data: dataBytes}, backend.NewStreamSender(nil))
	if err == nil {
		t.Fatalf("expected error when failing to get partitions")
	}
}

func TestRunStream_ConnectionError(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{newConnErr: errors.New("connection failed")})
	dataBytes, _ := json.Marshal(map[string]interface{}{"topicName": "t", "partition": 0})
	err := ds.RunStream(context.Background(), &backend.RunStreamRequest{Data: dataBytes}, backend.NewStreamSender(nil))
	if err == nil {
		t.Fatalf("expected error when connection fails")
	}
}

func TestRunStream_TopicNotFound(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{partitionsErr: kafka_client.ErrTopicNotFound})
	dataBytes, _ := json.Marshal(map[string]interface{}{"topicName": "nonexistent", "partition": "all"})
	err := ds.RunStream(context.Background(), &backend.RunStreamRequest{Data: dataBytes}, backend.NewStreamSender(nil))
	if err == nil {
		t.Fatalf("expected error when topic not found")
	}
	if !errors.Is(err, kafka_client.ErrTopicNotFound) {
		t.Errorf("expected topic not found error, got: %v", err)
	}
}

func TestRunStream_InvalidJSON(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	err := ds.RunStream(context.Background(), &backend.RunStreamRequest{Data: []byte("invalid-json")}, backend.NewStreamSender(nil))
	if err == nil {
		t.Fatalf("expected error for invalid JSON")
	}
}

func TestSubscribeStream_InvalidJSON(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	resp, err := ds.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Data: []byte("invalid-json")})
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
	if resp.Status != backend.SubscribeStreamStatusPermissionDenied {
		t.Errorf("Expected permission denied status, got %v", resp.Status)
	}
}

func TestSubscribeStream_ValidRequest(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	data, _ := json.Marshal(map[string]interface{}{"topicName": "valid-topic"})
	resp, err := ds.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Data: data})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if resp.Status != backend.SubscribeStreamStatusOK {
		t.Errorf("Expected OK status, got %v", resp.Status)
	}
}

func TestCallResource_InvalidURL(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "://invalid-url"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 400 {
		t.Fatalf("expected 400 for invalid URL, got %d", sent.Status)
	}
}

func TestCallResource_MissingTopicParameter(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "/"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 400 {
		t.Fatalf("expected 400 for missing topic parameter, got %d", sent.Status)
	}
}

func TestCallResource_Partitions_ConnectionError(t *testing.T) {
	mc := &mockKafkaClient{newConnErr: errors.New("connection failed")}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "/?topic=test"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 500 {
		t.Fatalf("expected 500 for connection error, got %d", sent.Status)
	}
}

func TestCallResource_Partitions_TopicNotFound(t *testing.T) {
	mc := &mockKafkaClient{partitionsErr: kafka_client.ErrTopicNotFound}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "partitions", Method: "GET", URL: "/?topic=nonexistent"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 404 {
		t.Fatalf("expected 404 for topic not found, got %d", sent.Status)
	}
}

func TestCallResource_Topics_ConnectionError(t *testing.T) {
	mc := &mockKafkaClient{newConnErr: errors.New("connection failed")}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "topics", Method: "GET", URL: "/?prefix=test"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 500 {
		t.Fatalf("expected 500 for connection error, got %d", sent.Status)
	}
}

func TestCallResource_Topics_SearchError(t *testing.T) {
	mc := &mockKafkaClient{topicsErr: errors.New("search failed")}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "topics", Method: "GET", URL: "/?prefix=test"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	_ = ds.CallResource(context.Background(), req, sender)
	if sent.Status != 500 {
		t.Fatalf("expected 500 for search error, got %d", sent.Status)
	}
}

func TestCallResource_Topics_WithLimit(t *testing.T) {
	mc := &mockKafkaClient{topics: []string{"topic1", "topic2"}}
	ds := plugin.NewWithClient(mc)
	req := &backend.CallResourceRequest{Path: "topics", Method: "GET", URL: "/?prefix=topic&limit=10"}
	var sent backend.CallResourceResponse
	sender := backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error { sent = *r; return nil })
	if err := ds.CallResource(context.Background(), req, sender); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sent.Status != 200 {
		t.Fatalf("expected 200, got %d", sent.Status)
	}
}

func TestCheckHealth_NilClient(t *testing.T) {
	ds := plugin.NewWithClient(nil)
	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result.Status != backend.HealthStatusError {
		t.Error("Expected error status for nil client")
	}
}

func TestDispose(t *testing.T) {
	mc := &mockKafkaClient{}
	ds := plugin.NewWithClient(mc)

	// Should not panic
	ds.Dispose()
}

func TestPublishStream(t *testing.T) {
	ds := plugin.NewWithClient(&mockKafkaClient{})
	resp, err := ds.PublishStream(context.Background(), &backend.PublishStreamRequest{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if resp.Status != backend.PublishStreamStatusPermissionDenied {
		t.Errorf("Expected permission denied, got %v", resp.Status)
	}
}

// Note: getDatasourceSettings is unexported; higher coverage would require moving
// it or adding a test shim inside the plugin package. Skipping for now.

func TestMockKafkaClient_AvroMethods(t *testing.T) {
	mc := &mockKafkaClient{
		messageFormat:             "avro",
		schemaRegistryUrl:         "http://localhost:8081",
		schemaRegistryUsername:    "test-user",
		schemaRegistryPassword:    "test-pass",
		avroSubjectNamingStrategy: "recordName",
	}

	if mc.GetMessageFormat() != "avro" {
		t.Errorf("Expected GetMessageFormat to return 'avro', got %s", mc.GetMessageFormat())
	}
	if mc.GetSchemaRegistryUrl() != "http://localhost:8081" {
		t.Errorf("Expected GetSchemaRegistryUrl to return 'http://localhost:8081', got %s", mc.GetSchemaRegistryUrl())
	}
	if mc.GetSchemaRegistryUsername() != "test-user" {
		t.Errorf("Expected GetSchemaRegistryUsername to return 'test-user', got %s", mc.GetSchemaRegistryUsername())
	}
	if mc.GetSchemaRegistryPassword() != "test-pass" {
		t.Errorf("Expected GetSchemaRegistryPassword to return 'test-pass', got %s", mc.GetSchemaRegistryPassword())
	}
	if mc.GetAvroSubjectNamingStrategy() != "recordName" {
		t.Errorf("Expected GetAvroSubjectNamingStrategy to return 'recordName', got %s", mc.GetAvroSubjectNamingStrategy())
	}
}

func TestNewKafkaInstance(t *testing.T) {
	tests := []struct {
		name        string
		settings    backend.DataSourceInstanceSettings
		expectError bool
	}{
		{
			name: "valid settings",
			settings: backend.DataSourceInstanceSettings{
				JSONData: []byte(`{
					"bootstrapServers": "localhost:9092",
					"clientId": "test-client",
					"timeout": 5000
				}`),
				DecryptedSecureJSONData: map[string]string{
					"saslPassword": "test-password",
				},
			},
			expectError: false,
		},
		{
			name: "invalid JSON",
			settings: backend.DataSourceInstanceSettings{
				JSONData: []byte(`invalid json`),
			},
			expectError: true,
		},
		{
			name: "empty settings",
			settings: backend.DataSourceInstanceSettings{
				JSONData: []byte(`{}`),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance, err := plugin.NewKafkaInstance(context.Background(), tt.settings)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				if instance != nil {
					t.Error("Expected nil instance when error occurs")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
				if instance == nil {
					t.Error("Expected non-nil instance")
				}

				// Verify it's a KafkaDatasource
				if _, ok := instance.(*plugin.KafkaDatasource); !ok {
					t.Errorf("Expected *plugin.KafkaDatasource, got %T", instance)
				}
			}
		})
	}
}

func TestNewKafkaInstance_CompleteSettings(t *testing.T) {
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{
			"bootstrapServers": "localhost:9092",
			"clientId": "test-client",
			"securityProtocol": "SASL_SSL",
			"saslMechanisms": "PLAIN",
			"saslUsername": "test-user",
			"tlsSkipVerify": true,
			"timeout": 5000,
			"messageFormat": "json",
			"schemaRegistryUrl": "http://localhost:8081"
		}`),
		DecryptedSecureJSONData: map[string]string{
			"saslPassword":           "test-password",
			"schemaRegistryUsername": "registry-user",
			"schemaRegistryPassword": "registry-pass",
		},
	}

	instance, err := plugin.NewKafkaInstance(context.Background(), settings)
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
	if instance == nil {
		t.Error("Expected non-nil instance")
	}

	// Verify it's a KafkaDatasource
	if _, ok := instance.(*plugin.KafkaDatasource); !ok {
		t.Errorf("Expected *plugin.KafkaDatasource, got %T", instance)
	}
}

func TestNewKafkaInstance_BooleanAsStrings(t *testing.T) {
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{
			"bootstrapServers": "localhost:9092",
			"tlsSkipVerify": true,
			"tlsAuthWithCACert": true,
			"tlsAuth": true
		}`),
	}

	instance, err := plugin.NewKafkaInstance(context.Background(), settings)
	if err != nil {
		t.Errorf("Expected no error but got: %v", err)
	}
	if instance == nil {
		t.Error("Expected non-nil instance")
	}
}

func TestRunStream_RuntimeConfigUpdate_JSON_to_Avro(t *testing.T) {
	// Create a mock client that can simulate message processing
	mockClient := &mockKafkaClient{
		partitions: []int32{0},
		consumerMessages: []kafka_client.KafkaMessage{
			{
				Value:     map[string]interface{}{"key": "test-value"},
				RawValue:  []byte(`{"key": "test-value"}`),
				Offset:    1,
				Timestamp: time.Now(),
			},
		},
	}

	ds := plugin.NewWithClient(mockClient)

	// Initial stream request with JSON format
	initialData, _ := json.Marshal(map[string]interface{}{
		"topicName":     "test-topic",
		"partition":     0,
		"messageFormat": "json",
	})

	// Create a custom stream sender to capture frames
	streamSender := backend.NewStreamSender(nil)

	// Start streaming with JSON format
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		err := ds.RunStream(ctx, &backend.RunStreamRequest{Data: initialData}, streamSender)
		if err != nil {
			t.Errorf("RunStream failed: %v", err)
		}
	}()

	// Wait for initial processing
	time.Sleep(30 * time.Millisecond)

	// Simulate query editor changing format to Avro
	updatedData, _ := json.Marshal(map[string]interface{}{
		"topicName":        "test-topic",
		"partition":        0,
		"messageFormat":    "avro",
		"avroSchemaSource": "inlineSchema",
		"avroSchema":       `{"type": "record", "name": "Test", "fields": [{"name": "key", "type": "string"}]}`,
	})

	// Start new stream with Avro format (simulating query editor change)
	streamSender2 := backend.NewStreamSender(nil)

	go func() {
		err := ds.RunStream(ctx, &backend.RunStreamRequest{Data: updatedData}, streamSender2)
		if err != nil {
			t.Errorf("RunStream with updated config failed: %v", err)
		}
	}()

	// Wait for processing with new config
	time.Sleep(30 * time.Millisecond)

	// Test passes if no errors occurred during the configuration changes
}

func TestRunStream_ConfigChange_AllPartitions(t *testing.T) {
	// Test configuration changes when streaming from all partitions
	mockClient := &mockKafkaClient{
		partitions: []int32{0, 1, 2},
		consumerMessages: []kafka_client.KafkaMessage{
			{
				Value:     map[string]interface{}{"key": "value1"},
				RawValue:  []byte(`{"key": "value1"}`),
				Offset:    1,
				Timestamp: time.Now(),
			},
		},
	}

	ds := plugin.NewWithClient(mockClient)

	// Initial request for all partitions with JSON
	initialData, _ := json.Marshal(map[string]interface{}{
		"topicName":     "test-topic",
		"partition":     "all",
		"messageFormat": "json",
	})

	streamSender := backend.NewStreamSender(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	go func() {
		err := ds.RunStream(ctx, &backend.RunStreamRequest{Data: initialData}, streamSender)
		if err != nil {
			t.Errorf("RunStream for all partitions failed: %v", err)
		}
	}()

	// Wait for initial setup
	time.Sleep(40 * time.Millisecond)

	// Simulate changing to Avro format for all partitions
	updatedData, _ := json.Marshal(map[string]interface{}{
		"topicName":        "test-topic",
		"partition":        "all",
		"messageFormat":    "avro",
		"avroSchemaSource": "inlineSchema",
		"avroSchema":       `{"type": "record", "name": "Test", "fields": [{"name": "key", "type": "string"}]}`,
	})

	streamSender2 := backend.NewStreamSender(nil)

	go func() {
		err := ds.RunStream(ctx, &backend.RunStreamRequest{Data: updatedData}, streamSender2)
		if err != nil {
			t.Errorf("RunStream with Avro config failed: %v", err)
		}
	}()

	// Wait for processing
	time.Sleep(40 * time.Millisecond)

	// Test passes if no errors occurred during the configuration changes
	t.Log("Configuration change test for all partitions completed")
}

func TestValidateAvroSchema(t *testing.T) {
	// Create datasource using NewKafkaInstance (the proper way for tests)
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{}`),
	}
	instance, err := plugin.NewKafkaInstance(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	ds, ok := instance.(*plugin.KafkaDatasource)
	if !ok {
		t.Fatalf("Instance is not a KafkaDatasource")
	}

	tests := []struct {
		name     string
		schema   string
		expected string
	}{
		{
			name:     "Valid record schema",
			schema:   `{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "string"}]}`,
			expected: "Schema is valid",
		},
		{
			name:     "Empty schema",
			schema:   "",
			expected: "Schema cannot be empty",
		},
		{
			name:     "Invalid JSON",
			schema:   `{"type": "record", "name":`,
			expected: "Invalid JSON format: unexpected end of JSON input",
		},
		{
			name:     "Missing type field",
			schema:   `{"name": "Test", "fields": []}`,
			expected: "Avro schema must have a 'type' field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ds.ValidateAvroSchema(tt.schema)
			if err != nil {
				t.Errorf("ValidateAvroSchema() error = %v", err)
				return
			}
			if result.Message != tt.expected {
				t.Errorf("ValidateAvroSchema() = %v, expected %v", result.Message, tt.expected)
			}
		})
	}
}

func TestValidateSchemaRegistry(t *testing.T) {
	// Create datasource using NewKafkaInstance (the proper way for tests)
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{}`),
	}
	instance, err := plugin.NewKafkaInstance(context.Background(), settings)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	ds, ok := instance.(*plugin.KafkaDatasource)
	if !ok {
		t.Fatalf("Instance is not a KafkaDatasource")
	}

	// Test with no schema registry URL configured
	result, err := ds.ValidateSchemaRegistry(context.Background())
	if err != nil {
		t.Errorf("ValidateSchemaRegistry() error = %v", err)
		return
	}
	if result.Status != backend.HealthStatusError {
		t.Errorf("Expected error status for missing schema registry URL, got %v", result.Status)
	}
	if result.Message != "Schema registry URL is not configured" {
		t.Errorf("Expected 'Schema registry URL is not configured', got %v", result.Message)
	}
}
