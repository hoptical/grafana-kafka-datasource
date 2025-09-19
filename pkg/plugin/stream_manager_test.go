package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/segmentio/kafka-go"
)

// mockStreamClient implements KafkaClientAPI for stream manager tests
type mockStreamClient struct {
	partitions    []int32
	partitionsErr error
	readerErr     error
	pullMessages  []kafka_client.KafkaMessage
	pullErr       error
	pullIndex     int
}

func (m *mockStreamClient) NewConnection() error { return nil }
func (m *mockStreamClient) GetTopics(ctx context.Context, prefix string, limit int) ([]string, error) {
	return nil, nil
}
func (m *mockStreamClient) HealthCheck() error { return nil }
func (m *mockStreamClient) Dispose()           {}

func (m *mockStreamClient) GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error) {
	if m.partitionsErr != nil {
		return nil, m.partitionsErr
	}
	return m.partitions, nil
}

func (m *mockStreamClient) NewStreamReader(ctx context.Context, topic string, partition int32, autoOffsetReset string, lastN int32) (*kafka.Reader, error) {
	return nil, m.readerErr
}

func (m *mockStreamClient) ConsumerPull(ctx context.Context, reader *kafka.Reader) (kafka_client.KafkaMessage, error) {
	if m.pullErr != nil {
		return kafka_client.KafkaMessage{}, m.pullErr
	}
	if m.pullIndex >= len(m.pullMessages) {
		return kafka_client.KafkaMessage{}, errors.New("no more messages")
	}
	msg := m.pullMessages[m.pullIndex]
	m.pullIndex++
	return msg, nil
}

func TestStreamManager_ValidateAndGetPartitions(t *testing.T) {
	tests := []struct {
		name          string
		partition     interface{}
		partitions    []int32
		partitionsErr error
		expectedParts []int32
		expectError   bool
	}{
		{
			name:          "single partition number",
			partition:     float64(0),
			partitions:    []int32{0, 1, 2},
			expectedParts: []int32{0},
			expectError:   false,
		},
		{
			name:        "partition out of range",
			partition:   float64(5),
			partitions:  []int32{0, 1, 2},
			expectError: true,
		},
		{
			name:          "all partitions",
			partition:     "all",
			partitions:    []int32{0, 1, 2},
			expectedParts: []int32{0, 1, 2},
			expectError:   false,
		},
		{
			name:        "invalid string partition",
			partition:   "invalid",
			partitions:  []int32{0, 1, 2},
			expectError: true,
		},
		{
			name:        "invalid partition type",
			partition:   []string{"array"},
			partitions:  []int32{0, 1, 2},
			expectError: true,
		},
		{
			name:          "topic not found error",
			partition:     "all",
			partitionsErr: kafka_client.ErrTopicNotFound,
			expectError:   true,
		},
		{
			name:          "general partition fetch error",
			partition:     "all",
			partitionsErr: errors.New("connection failed"),
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockStreamClient{
				partitions:    tt.partitions,
				partitionsErr: tt.partitionsErr,
			}
			sm := NewStreamManager(client, 5, 1000)

			qm := queryModel{
				Topic:     "test-topic",
				Partition: tt.partition,
			}

			result, err := sm.ValidateAndGetPartitions(context.Background(), qm)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result) != len(tt.expectedParts) {
				t.Errorf("Expected %d partitions, got %d", len(tt.expectedParts), len(result))
				return
			}

			for i, expected := range tt.expectedParts {
				if result[i] != expected {
					t.Errorf("Expected partition %d at index %d, got %d", expected, i, result[i])
				}
			}
		})
	}
}

func TestStreamManager_ProcessMessage(t *testing.T) {
	tests := []struct {
		name           string
		message        kafka_client.KafkaMessage
		partition      int32
		partitions     []int32
		timestampMode  string
		expectedFields int // minimum expected fields (time + potential partition + data fields)
	}{
		{
			name: "simple message with timestamp mode 'now'",
			message: kafka_client.KafkaMessage{
				Value: map[string]interface{}{
					"temperature": float64(23.5),
					"humidity":    float64(45.2),
				},
				Timestamp: time.Now().Add(-1 * time.Hour),
				Offset:    100,
			},
			partition:      0,
			partitions:     []int32{0},
			timestampMode:  "now",
			expectedFields: 4, // time + offset + temperature + humidity
		},
		{
			name: "nested message with multiple partitions",
			message: kafka_client.KafkaMessage{
				Value: map[string]interface{}{
					"host": map[string]interface{}{
						"name": "server1",
						"ip":   "127.0.0.1",
					},
					"metrics": map[string]interface{}{
						"cpu": 0.8,
					},
				},
				Timestamp: time.Now(),
				Offset:    200,
			},
			partition:      1,
			partitions:     []int32{0, 1, 2},
			timestampMode:  "message",
			expectedFields: 6, // time + partition + offset + host.name + host.ip + metrics.cpu
		},
		{
			name: "message with various data types",
			message: kafka_client.KafkaMessage{
				Value: map[string]interface{}{
					"string_val": "hello",
					"int_val":    int(42),
					"float_val":  float64(3.14),
					"bool_val":   true,
					"null_val":   nil,
				},
				Timestamp: time.Now(),
				Offset:    300,
			},
			partition:      0,
			partitions:     []int32{0},
			timestampMode:  "message",
			expectedFields: 7, // time + offset + 5 data fields
		},
		{
			name: "top-level array message",
			message: kafka_client.KafkaMessage{
				Value: []interface{}{
					map[string]interface{}{
						"id":   1,
						"name": "item1",
					},
					map[string]interface{}{
						"id":   2,
						"name": "item2",
					},
				},
				Timestamp: time.Now().Add(-30 * time.Minute),
				Offset:    300,
			},
			partition:      0,
			partitions:     []int32{0},
			timestampMode:  "message",
			expectedFields: 6, // time + offset + item_0.id + item_0.name + item_1.id + item_1.name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockStreamClient{}
			sm := NewStreamManager(client, 5, 1000)

			qm := queryModel{
				Topic:         "test-topic",
				TimestampMode: tt.timestampMode,
			}

			frame, err := sm.ProcessMessage(tt.message, tt.partition, tt.partitions, qm)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if frame == nil {
				t.Error("Expected frame but got nil")
				return
			}

			if len(frame.Fields) < tt.expectedFields {
				t.Errorf("Expected at least %d fields, got %d", tt.expectedFields, len(frame.Fields))
			}

			// Check time field is always first
			if len(frame.Fields) > 0 && frame.Fields[0].Name != "time" {
				t.Error("First field should be 'time'")
			}

			// Check partition field when multiple partitions
			if len(tt.partitions) > 1 {
				if len(frame.Fields) < 2 || frame.Fields[1].Name != "partition" {
					t.Error("Second field should be 'partition' when multiple partitions")
				} else {
					partitionValue := frame.Fields[1].At(0)
					if partitionValue != tt.partition {
						t.Errorf("Expected partition value %d, got %v", tt.partition, partitionValue)
					}
				}
			}

			// Verify timestamp handling
			timeField := frame.Fields[0]
			timeValue := timeField.At(0).(time.Time)

			if tt.timestampMode == "now" {
				// Should be recent (within last few seconds)
				if time.Since(timeValue) > 5*time.Second {
					t.Error("Expected recent timestamp when using 'now' mode")
				}
			} else {
				// Should match message timestamp
				if !timeValue.Equal(tt.message.Timestamp) {
					t.Errorf("Expected message timestamp %v, got %v", tt.message.Timestamp, timeValue)
				}
			}
		})
	}
}

func TestNewStreamManager(t *testing.T) {
	client := &mockStreamClient{}
	sm := NewStreamManager(client, 5, 1000)

	if sm == nil {
		t.Error("NewStreamManager should return a non-nil StreamManager")
	} else {
		if sm.client != client {
			t.Error("StreamManager should store the provided client")
		}
	}
}

func TestStreamManager_HandleTopicError(t *testing.T) {
	client := &mockStreamClient{}
	sm := NewStreamManager(client, 5, 1000)

	// Test topic not found error
	err := sm.handleTopicError(kafka_client.ErrTopicNotFound, "test-topic")
	if err == nil {
		t.Error("Expected error but got nil")
	}
	expected := "topic test-topic topic not found"
	if err.Error() != expected {
		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
	}

	// Test general error
	generalErr := errors.New("connection failed")
	err = sm.handleTopicError(generalErr, "test-topic")
	if err == nil {
		t.Error("Expected error but got nil")
	}
	if !errors.Is(err, generalErr) {
		t.Error("Expected wrapped general error")
	}
}
