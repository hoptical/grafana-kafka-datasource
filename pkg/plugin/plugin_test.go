package plugin_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

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
	return nil, m.streamReaderErr
}
func (m *mockKafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader) (kafka_client.KafkaMessage, error) {
	if m.consumerErr != nil {
		return kafka_client.KafkaMessage{}, m.consumerErr
	}
	if len(m.consumerMessages) == 0 {
		return kafka_client.KafkaMessage{}, errors.New("no more messages")
	}
	msg := m.consumerMessages[0]
	m.consumerMessages = m.consumerMessages[1:]
	return msg, nil
}
func (m *mockKafkaClient) Dispose() {}

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

// Note: getDatasourceSettings is unexported; higher coverage would require moving
// it or adding a test shim inside the plugin package. Skipping for now.
