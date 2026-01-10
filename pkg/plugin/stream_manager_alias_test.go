package plugin

import (
	"testing"
	"time"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

func TestFormatAlias(t *testing.T) {
	config := &StreamConfig{
		RefID: "A",
	}
	topic := "test-topic"
	partition := int32(0)

	tests := []struct {
		name      string
		alias     string
		fieldName string
		expected  string
	}{
		{
			name:      "Simple alias",
			alias:     "My Alias",
			fieldName: "value",
			expected:  "My Alias",
		},
		{
			name:      "Topic template",
			alias:     "{{topic}} - Data",
			fieldName: "value",
			expected:  "test-topic - Data",
		},
		{
			name:      "Partition template",
			alias:     "Partition {{partition}}",
			fieldName: "value",
			expected:  "Partition 0",
		},
		{
			name:      "RefID template",
			alias:     "Query {{refid}}",
			fieldName: "value",
			expected:  "Query A",
		},
		{
			name:      "Field template",
			alias:     "{{field}} metric",
			fieldName: "temperature",
			expected:  "temperature metric",
		},
		{
			name:      "Combined template",
			alias:     "{{refid}} - {{topic}}:{{partition}} - {{field}}",
			fieldName: "voltage",
			expected:  "A - test-topic:0 - voltage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatAlias(tt.alias, config, topic, partition, tt.fieldName)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestProcessMessage_AliasApplication(t *testing.T) {
	// Setup
	sm := NewStreamManager(&mockStreamClient{}, 10, 100)

	msg := kafka_client.KafkaMessage{
		Value: map[string]interface{}{
			"temperature": 25.5,
			"humidity":    60,
		},
		Timestamp: time.Now(),
		Offset:    100,
	}

	partition := int32(0)
	partitions := []int32{0}
	topic := "sensor-data"

	t.Run("Should apply alias to frame name", func(t *testing.T) {
		config := &StreamConfig{
			MessageFormat: "json",
			RefID:         "A",
			Alias:         "{{topic}} Stream",
		}

		frame, err := sm.ProcessMessage(msg, partition, partitions, config, topic)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedName := "sensor-data Stream"
		if frame.Name != expectedName {
			t.Errorf("expected frame name %q, got %q", expectedName, frame.Name)
		}
	})

	t.Run("Should apply alias to field display names", func(t *testing.T) {
		config := &StreamConfig{
			MessageFormat: "json",
			RefID:         "A",
			Alias:         "{{field}} ({{topic}})",
		}

		frame, err := sm.ProcessMessage(msg, partition, partitions, config, topic)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check fields
		for _, field := range frame.Fields {
			if field.Name == "time" {
				continue
			}

			expectedName := field.Name + " (sensor-data)"
			if field.Config.DisplayNameFromDS != expectedName {
				t.Errorf("field %s: expected DisplayNameFromDS %q, got %q", field.Name, expectedName, field.Config.DisplayNameFromDS)
			}
		}
	})

	t.Run("Should apply alias even without {field} placeholder", func(t *testing.T) {
		config := &StreamConfig{
			MessageFormat: "json",
			RefID:         "A",
			Alias:         "Fixed Name",
		}

		frame, err := sm.ProcessMessage(msg, partition, partitions, config, topic)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedName := "Fixed Name"
		if frame.Name != expectedName {
			t.Errorf("expected frame name %q, got %q", expectedName, frame.Name)
		}

		for _, field := range frame.Fields {
			if field.Name == "time" {
				continue
			}
			// When {field} is missing, all fields get the same DisplayNameFromDS
			// This is expected behavior for single-stat panels or when user wants to override everything
			if field.Config.DisplayNameFromDS != expectedName {
				t.Errorf("field %s: expected DisplayNameFromDS %q, got %q", field.Name, expectedName, field.Config.DisplayNameFromDS)
			}
		}
	})
}
