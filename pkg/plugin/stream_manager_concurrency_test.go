package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// TestStreamManager_SchemaCacheConcurrency tests concurrent access to schema cache
func TestStreamManager_SchemaCacheConcurrency(t *testing.T) {
	// Create a mock Schema Registry server
	requestCount := 0
	mu := sync.Mutex{}

	schemaPayload := map[string]interface{}{
		"subject": "test-topic-value",
		"version": 1,
		"id":      1,
		"schema":  `{"type":"record","name":"TestRecord","fields":[{"name":"field1","type":"string"}]}`,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_ = json.NewEncoder(w).Encode(schemaPayload)
	}))
	defer server.Close()

	// Create a mock client that properly implements KafkaClientAPI
	mockClient := &mockStreamClient{}

	sm := NewStreamManager(mockClient, 5, 100)
	subject := "test-topic-value"

	// Test concurrent access to schema cache
	concurrency := 50
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()

			// Each goroutine tries to fetch the schema multiple times
			for j := 0; j < 3; j++ {
				schema, err := sm.getSchemaFromRegistryWithCache(server.URL, "", "", subject)
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d iteration %d: %w", idx, j, err)
					return
				}
				if schema == "" {
					errCh <- fmt.Errorf("goroutine %d iteration %d: empty schema", idx, j)
					return
				}
				// Small delay to increase chance of races
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent schema fetch error: %v", err)
	}

	// Verify that caching is working - should not have 150 requests (50 goroutines * 3 iterations)
	mu.Lock()
	count := requestCount
	mu.Unlock()

	if count > 10 {
		t.Logf("Warning: Schema cache may not be working efficiently. Request count: %d (expected ~1-5)", count)
	}

	t.Logf("Schema Registry requests made: %d (with %d concurrent goroutines * 3 iterations each)", count, concurrency)
}

// TestStreamManager_SchemaClientConcurrency tests concurrent Schema Registry client creation
func TestStreamManager_SchemaClientConcurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schemaPayload := map[string]interface{}{
			"subject": "test-subject",
			"version": 1,
			"id":      1,
			"schema":  `{"type":"record","name":"Test","fields":[]}`,
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_ = json.NewEncoder(w).Encode(schemaPayload)
	}))
	defer server.Close()

	// Create a mock client that properly implements KafkaClientAPI
	mockClient := &mockStreamClient{}

	sm := NewStreamManager(mockClient, 5, 100)

	// Multiple goroutines creating clients with different credentials
	concurrency := 30
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()

			// Alternate between different credentials to test client replacement
			username := fmt.Sprintf("user%d", idx%3)
			password := fmt.Sprintf("pass%d", idx%3)
			subject := fmt.Sprintf("subject-%d", idx%5)

			_, err := sm.getSchemaFromRegistryWithCache(server.URL, username, password, subject)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent schema client error: %v", err)
	}
}

// TestStreamConfig_ConcurrentAccess tests concurrent reads and writes to StreamConfig
func TestStreamConfig_ConcurrentAccess(t *testing.T) {
	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "inline",
		AvroSchema:       `{"type":"string"}`,
		AutoOffsetReset:  "latest",
		TimestampMode:    "kafka",
		LastN:            100,
	}

	// Create a mock client that properly implements KafkaClientAPI
	mockClient := &mockStreamClient{}

	sm := NewStreamManager(mockClient, 5, 100)

	concurrency := 50
	iterations := 100
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency*2)

	// Writers - update message format
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				format := "json"
				if j%2 == 0 {
					format = "avro"
				}
				sm.UpdateStreamConfig(config, format)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Readers - read config fields
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				config.mu.RLock()
				format := config.MessageFormat
				config.mu.RUnlock()

				if format != "json" && format != "avro" {
					errCh <- fmt.Errorf("reader %d: invalid format: %s", idx, format)
					return
				}
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent config access error: %v", err)
	}
}

// TestProcessMessageToFrame_Concurrency tests concurrent message processing
func TestProcessMessageToFrame_Concurrency(t *testing.T) {
	mockClient := &mockStreamClient{
		partitions: []int32{0, 1, 2},
	}

	sm := NewStreamManager(mockClient, 5, 100)

	config := &StreamConfig{
		MessageFormat:    "json",
		AutoOffsetReset:  "latest",
		TimestampMode:    "kafka",
		AvroSchemaSource: "inline",
	}

	// Create test messages
	messages := []kafka_client.KafkaMessage{
		{
			Offset:    100,
			Timestamp: time.Now(),
			Value: map[string]interface{}{
				"temperature": 25.5,
				"humidity":    60.0,
				"status":      "active",
			},
		},
		{
			Offset:    200,
			Timestamp: time.Now(),
			Value: map[string]interface{}{
				"user": map[string]interface{}{
					"name": "John",
					"age":  30,
				},
			},
		},
	}

	partitions := []int32{0, 1, 2}
	concurrency := 30
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()

			msg := messages[idx%len(messages)]
			partition := int32(idx % len(partitions))
			_, err := sm.ProcessMessage(msg, partition, partitions, config, "test-topic")
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent message processing error: %v", err)
	}
}

// TestStreamManager_MixedOperations tests various operations happening concurrently
func TestStreamManager_MixedOperations(t *testing.T) {
	// Create mock Schema Registry
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schemaPayload := map[string]interface{}{
			"subject": "test-topic-value",
			"version": 1,
			"id":      1,
			"schema":  `{"type":"record","name":"Mixed","fields":[{"name":"data","type":"string"}]}`,
		}
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		_ = json.NewEncoder(w).Encode(schemaPayload)
	}))
	defer server.Close()

	// Create a mock client that properly implements KafkaClientAPI
	mockClient := &mockStreamClient{}

	sm := NewStreamManager(mockClient, 5, 100)

	config := &StreamConfig{
		MessageFormat:    "json",
		AvroSchemaSource: "registry",
		AutoOffsetReset:  "latest",
		TimestampMode:    "kafka",
	}

	msg := kafka_client.KafkaMessage{
		Offset:    100,
		Timestamp: time.Now(),
		Value: map[string]interface{}{
			"field1": "value1",
			"field2": 42,
		},
	}

	partitions := []int32{0, 1, 2}
	concurrency := 20
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency*3)

	// Schema fetch operations
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := sm.getSchemaFromRegistryWithCache(server.URL, "", "", "test-topic-value")
			if err != nil {
				errCh <- fmt.Errorf("schema fetch %d: %w", idx, err)
			}
		}(i)
	}

	// Config update operations
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			format := "json"
			if idx%2 == 0 {
				format = "avro"
			}
			sm.UpdateStreamConfig(config, format)
		}(i)
	}

	// Message processing operations
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			partition := int32(idx % len(partitions))
			_, err := sm.ProcessMessage(msg, partition, partitions, config, "test-topic")
			if err != nil {
				errCh <- fmt.Errorf("process message %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Mixed operations error: %v", err)
	}
}

// TestValidateAndGetPartitions_Concurrency tests concurrent partition validation
func TestValidateAndGetPartitions_Concurrency(t *testing.T) {
	mockClient := &mockStreamClient{
		partitions: []int32{0, 1, 2, 3, 4},
	}

	sm := NewStreamManager(mockClient, 5, 100)
	ctx := context.Background()

	queries := []queryModel{
		{Topic: "test-topic", Partition: "all"},
		{Topic: "test-topic", Partition: float64(0)},
		{Topic: "test-topic", Partition: float64(1)},
		{Topic: "test-topic", Partition: float64(2)},
	}

	concurrency := 25
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()

			qm := queries[idx%len(queries)]
			partitions, err := sm.ValidateAndGetPartitions(ctx, qm)
			if err != nil {
				errCh <- fmt.Errorf("goroutine %d: %w", idx, err)
				return
			}

			if len(partitions) == 0 {
				errCh <- fmt.Errorf("goroutine %d: no partitions returned", idx)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	for err := range errCh {
		t.Errorf("Concurrent partition validation error: %v", err)
	}
}
