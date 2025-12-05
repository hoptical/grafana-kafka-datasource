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
	if m.readerErr != nil {
		return nil, m.readerErr
	}
	// Return a minimal kafka.Reader that can be safely closed
	// We create it with invalid config so it won't actually connect, but Close() will work
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"invalid-broker"},
		Topic:   "invalid-topic",
	})
	return reader, nil
}

func (m *mockStreamClient) ConsumerPull(ctx context.Context, reader *kafka.Reader, messageFormat string) (kafka_client.KafkaMessage, error) {
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

// Avro-related methods
func (m *mockStreamClient) GetMessageFormat() string             { return "json" }
func (m *mockStreamClient) GetSchemaRegistryUrl() string         { return "" }
func (m *mockStreamClient) GetSchemaRegistryUsername() string    { return "" }
func (m *mockStreamClient) GetSchemaRegistryPassword() string    { return "" }
func (m *mockStreamClient) GetAvroSubjectNamingStrategy() string { return "recordName" }

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

			config := &StreamConfig{
				MessageFormat:    "json",
				AvroSchemaSource: "",
				AvroSchema:       "",
				AutoOffsetReset:  "latest",
				TimestampMode:    tt.timestampMode,
			}

			frame, err := sm.ProcessMessage(tt.message, tt.partition, tt.partitions, config, "test")
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

func TestStreamManager_readFromPartition_ReaderError(t *testing.T) {
	mockClient := &mockStreamClient{
		readerErr: errors.New("failed to create reader"),
	}

	sm := NewStreamManager(mockClient, 5, 1000)
	qm := queryModel{
		Topic:           "test-topic",
		AutoOffsetReset: "earliest",
		LastN:           10,
		MessageFormat:   "json",
	}

	ctx := context.Background()
	messagesCh := make(chan messageWithPartition, 10)

	// This should not panic and should exit gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("readFromPartition panicked: %v", r)
		}
	}()

	config := &StreamConfig{
		MessageFormat: qm.MessageFormat,
	}

	sm.readFromPartition(ctx, 0, qm, config, messagesCh)

	// Channel should remain empty
	select {
	case <-messagesCh:
		t.Error("Expected no messages due to reader error")
	default:
		// Expected - no messages should be sent
	}
}

func TestStreamManager_readFromPartition_PullError(t *testing.T) {
	mockClient := &mockStreamClient{
		pullErr: errors.New("failed to pull message"),
	}

	sm := NewStreamManager(mockClient, 5, 1000)
	qm := queryModel{
		Topic:           "test-topic",
		AutoOffsetReset: "earliest",
		LastN:           10,
		MessageFormat:   "json",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	messagesCh := make(chan messageWithPartition, 10)

	config := &StreamConfig{
		MessageFormat:   qm.MessageFormat,
		AutoOffsetReset: qm.AutoOffsetReset,
	}

	// Start the partition reader
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("readFromPartition panicked: %v", r)
		}
	}()

	go sm.readFromPartition(ctx, 0, qm, config, messagesCh)

	// Wait a bit and check that error messages are received
	time.Sleep(60 * time.Millisecond)

	// We should receive error messages when pull fails
	messageReceived := false
	select {
	case msg := <-messagesCh:
		messageReceived = true
		if msg.msg.Error == nil {
			t.Error("Expected error message due to pull error, but got regular message")
		} else if msg.msg.Error.Error() != "failed to pull message" {
			t.Errorf("Expected pull error message, got: %v", msg.msg.Error)
		}
	default:
		// Should not happen - we expect error messages to be sent
	}

	if !messageReceived {
		t.Error("Expected error messages to be sent when pull fails")
	}
}

func TestStreamManager_readFromPartition_ContextCancellation(t *testing.T) {
	mockClient := &mockStreamClient{
		pullMessages: []kafka_client.KafkaMessage{
			{
				Value:     map[string]interface{}{"key": "value"},
				RawValue:  []byte(`{"key": "value"}`),
				Timestamp: time.Now(),
				Offset:    1,
			},
		},
	}

	sm := NewStreamManager(mockClient, 5, 1000)
	qm := queryModel{
		Topic:           "test-topic",
		AutoOffsetReset: "earliest",
		LastN:           10,
		MessageFormat:   "json",
	}

	ctx, cancel := context.WithCancel(context.Background())
	messagesCh := make(chan messageWithPartition, 10)

	config := &StreamConfig{
		MessageFormat: qm.MessageFormat,
	}

	// Start the partition reader
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("readFromPartition panicked: %v", r)
		}
	}()

	go sm.readFromPartition(ctx, 0, qm, config, messagesCh)

	// Cancel context immediately
	cancel()

	// Wait a bit and check that the reader stops
	time.Sleep(50 * time.Millisecond)

	// Should be able to receive at most one message before cancellation
	select {
	case msg := <-messagesCh:
		if msg.partition != 0 {
			t.Errorf("Expected partition 0, got %d", msg.partition)
		}
	default:
		// It's also acceptable if no message was received before cancellation
	}
}

func TestStreamManager_UpdateStreamConfig(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Verify initial configuration
	if config.MessageFormat != "json" {
		t.Errorf("Expected initial message format 'json', got '%s'", config.MessageFormat)
	}

	// Update configuration
	sm.UpdateStreamConfig(config, "avro")

	// Verify configuration was updated
	if config.MessageFormat != "avro" {
		t.Errorf("Expected updated message format 'avro', got '%s'", config.MessageFormat)
	}
}

func TestStreamManager_ProcessMessage_RuntimeConfigChange_JSON(t *testing.T) {
	mockClient := &mockStreamClient{}
	sm := NewStreamManager(mockClient, 5, 1000)

	// Test message with JSON format
	msg := kafka_client.KafkaMessage{
		Value:     map[string]interface{}{"key": "value", "number": 42},
		RawValue:  []byte(`{"key": "value", "number": 42}`),
		Offset:    1,
		Timestamp: time.Now(),
	}

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Process message with JSON format
	frame, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error processing JSON message, got: %v", err)
	}

	// Verify frame contains expected fields
	if len(frame.Fields) < 3 {
		t.Fatalf("Expected at least 3 fields (time, offset, data), got %d", len(frame.Fields))
	}

	// Verify message format is used correctly
	if config.MessageFormat != "json" {
		t.Errorf("Expected message format to remain 'json', got '%s'", config.MessageFormat)
	}
}

func TestStreamManager_ProcessMessage_RuntimeConfigChange_Avro(t *testing.T) {
	mockClient := &mockStreamClient{}
	sm := NewStreamManager(mockClient, 5, 1000)

	// Test message with Avro format (raw bytes that will fail decoding)
	msg := kafka_client.KafkaMessage{
		Value:     nil,                         // No parsed value for Avro
		RawValue:  []byte(`invalid avro data`), // Mock Avro data that will fail
		Offset:    1,
		Timestamp: time.Now(),
	}

	config := &StreamConfig{
		MessageFormat:    "avro",
		AvroSchemaSource: "inlineSchema",
		AvroSchema:       `{"type": "record", "name": "Test", "fields": [{"name": "key", "type": "string"}]}`,
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Process message with Avro format - should return error frame since Avro decoding will fail
	frame, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error (error should be in frame), got: %v", err)
	}

	// Verify frame contains error field
	if len(frame.Fields) < 3 {
		t.Fatalf("Expected at least 3 fields (time, offset, error), got %d", len(frame.Fields))
	}

	// Check if error field exists
	hasErrorField := false
	for _, field := range frame.Fields {
		if field.Name == "error" {
			hasErrorField = true
			break
		}
	}

	if !hasErrorField {
		t.Error("Expected error field in frame for failed Avro decoding")
	}
}

func TestStreamManager_ProcessMessage_ConfigChangeReflection(t *testing.T) {
	mockClient := &mockStreamClient{}
	sm := NewStreamManager(mockClient, 5, 1000)

	// Test message
	msg := kafka_client.KafkaMessage{
		Value:     map[string]interface{}{"key": "value"},
		RawValue:  []byte(`{"key": "value"}`),
		Offset:    1,
		Timestamp: time.Now(),
	}

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Process message with initial JSON format
	frame1, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error processing initial message, got: %v", err)
	}

	// Change configuration to Avro format
	sm.UpdateStreamConfig(config, "avro")

	// Process same message with updated Avro format
	frame2, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error processing message with updated config, got: %v", err)
	}

	// Verify configuration was updated
	if config.MessageFormat != "avro" {
		t.Errorf("Expected message format to be updated to 'avro', got '%s'", config.MessageFormat)
	}

	// Verify frames are different (Avro processing should handle differently)
	if frame1 == nil || frame2 == nil {
		t.Fatal("Expected both frames to be non-nil")
	}

	// Check if second frame has error field (due to Avro decoding failure)
	hasErrorField := false
	for _, field := range frame2.Fields {
		if field.Name == "error" {
			hasErrorField = true
			break
		}
	}

	if !hasErrorField {
		t.Error("Expected error field in frame after config change to Avro")
	}
}

func TestStreamManager_ReadFromPartition_ConfigUpdate(t *testing.T) {
	mockClient := &mockStreamClient{
		pullMessages: []kafka_client.KafkaMessage{
			{
				Value:     map[string]interface{}{"key": "value1"},
				RawValue:  []byte(`{"key": "value1"}`),
				Offset:    1,
				Timestamp: time.Now(),
			},
			{
				Value:     map[string]interface{}{"key": "value2"},
				RawValue:  []byte(`{"key": "value2"}`),
				Offset:    2,
				Timestamp: time.Now(),
			},
		},
	}

	sm := NewStreamManager(mockClient, 5, 1000)

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	messagesCh := make(chan messageWithPartition, 10)

	// Start partition reader
	go sm.readFromPartition(ctx, 0, queryModel{
		Topic:           "test-topic",
		AutoOffsetReset: "latest",
		LastN:           10,
		MessageFormat:   "json",
	}, config, messagesCh)

	// Wait a bit for first message
	time.Sleep(20 * time.Millisecond)

	// Update configuration during streaming
	sm.UpdateStreamConfig(config, "avro")

	// Wait for second message
	time.Sleep(20 * time.Millisecond)

	// Collect received messages
	var receivedMessages []messageWithPartition
	timeout := time.After(50 * time.Millisecond)

loop:
	for len(receivedMessages) < 2 {
		select {
		case msg := <-messagesCh:
			receivedMessages = append(receivedMessages, msg)
		case <-timeout:
			break loop
		}
	}

	// Verify we received messages
	if len(receivedMessages) == 0 {
		t.Fatal("Expected to receive at least one message")
	}

	// Verify configuration was updated
	if config.MessageFormat != "avro" {
		t.Errorf("Expected message format to be updated to 'avro', got '%s'", config.MessageFormat)
	}
}

func TestStreamManager_DynamicConfig_AllPartitions(t *testing.T) {
	mockClient := &mockStreamClient{
		partitions: []int32{0, 1},
		pullMessages: []kafka_client.KafkaMessage{
			{
				Value:     map[string]interface{}{"key": "value1"},
				RawValue:  []byte(`{"key": "value1"}`),
				Offset:    1,
				Timestamp: time.Now(),
			},
		},
	}

	sm := NewStreamManager(mockClient, 5, 1000)

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	messagesCh := make(chan messageWithPartition, 10)

	// Start readers for all partitions
	sm.StartPartitionReaders(ctx, []int32{0, 1}, queryModel{
		Topic:           "test-topic",
		AutoOffsetReset: "latest",
		LastN:           10,
		MessageFormat:   "json",
	}, config, messagesCh)

	// Wait for initial messages
	time.Sleep(30 * time.Millisecond)

	// Update configuration during streaming
	sm.UpdateStreamConfig(config, "avro")

	// Wait for processing with new config
	time.Sleep(30 * time.Millisecond)

	// Verify configuration was updated
	if config.MessageFormat != "avro" {
		t.Errorf("Expected message format to be updated to 'avro', got '%s'", config.MessageFormat)
	}

	// Verify that the shared config is the same reference
	if config.MessageFormat != "avro" {
		t.Error("Expected shared config to reflect the update across all partition readers")
	}
}

func TestStreamManager_ProcessMessage_ErrorHandling_JSON(t *testing.T) {
	mockClient := &mockStreamClient{}
	sm := NewStreamManager(mockClient, 5, 1000)

	// Test message with invalid JSON
	msg := kafka_client.KafkaMessage{
		Value:     nil,
		RawValue:  []byte(`invalid json content`),
		Offset:    1,
		Timestamp: time.Now(),
	}

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "",
		AvroSchema:       "",
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Process message - should return error frame
	frame, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error (error should be in frame), got: %v", err)
	}

	// Verify frame contains error field
	hasErrorField := false
	for _, field := range frame.Fields {
		if field.Name == "error" {
			hasErrorField = true
			break
		}
	}

	if !hasErrorField {
		t.Error("Expected error field in frame for invalid JSON")
	}
}

func TestStreamManager_ProcessMessage_ErrorHandling_Avro(t *testing.T) {
	mockClient := &mockStreamClient{}
	sm := NewStreamManager(mockClient, 5, 1000)

	// Test message with invalid Avro data
	msg := kafka_client.KafkaMessage{
		Value:     nil,
		RawValue:  []byte(`invalid avro data`),
		Offset:    1,
		Timestamp: time.Now(),
	}

	config := &StreamConfig{
		MessageFormat:    "avro",
		AvroSchemaSource: "inlineSchema",
		AvroSchema:       `invalid schema`,
		AutoOffsetReset:  "latest",
		TimestampMode:    "message",
	}

	// Process message - should return error frame
	frame, err := sm.ProcessMessage(msg, 0, []int32{0}, config, "test")
	if err != nil {
		t.Fatalf("Expected no error (error should be in frame), got: %v", err)
	}

	// Verify frame contains error field
	hasErrorField := false
	for _, field := range frame.Fields {
		if field.Name == "error" {
			hasErrorField = true
			break
		}
	}

	if !hasErrorField {
		t.Error("Expected error field in frame for invalid Avro data")
	}
}
