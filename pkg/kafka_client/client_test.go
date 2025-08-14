package kafka_client

import (
	"context"
	"errors"
	"testing"
)

func TestNewKafkaClient_Defaults(t *testing.T) {
	options := Options{
		BootstrapServers:  "localhost:9092",
		ClientId:          "test-client",
		TLSAuthWithCACert: true,
		TLSAuth:           true,
		TLSSkipVerify:     true,
		ServerName:        "test-server",
		TLSCACert:         "test-ca-cert",
		TLSClientCert:     "test-client-cert",
		TLSClientKey:      "test-client-key",
		Timeout:           1234,
	}
	client := NewKafkaClient(options)
	if client.BootstrapServers != "localhost:9092" {
		t.Errorf("Expected BootstrapServers to be 'localhost:9092', got %s", client.BootstrapServers)
	}
	if client.ClientId != "test-client" {
		t.Errorf("Expected ClientId to be 'test-client', got %s", client.ClientId)
	}
	if !client.TLSAuthWithCACert {
		t.Error("Expected TLSAuthWithCACert to be true")
	}
	if !client.TLSAuth {
		t.Error("Expected TLSAuth to be true")
	}
	if !client.TLSSkipVerify {
		t.Error("Expected TLSSkipVerify to be true")
	}
	if client.ServerName != "test-server" {
		t.Errorf("Expected ServerName to be 'test-server', got %s", client.ServerName)
	}
	if client.TLSCACert != "test-ca-cert" {
		t.Errorf("Expected TLSCACert to be 'test-ca-cert', got %s", client.TLSCACert)
	}
	if client.TLSClientCert != "test-client-cert" {
		t.Errorf("Expected TLSClientCert to be 'test-client-cert', got %s", client.TLSClientCert)
	}
	if client.TLSClientKey != "test-client-key" {
		t.Errorf("Expected TLSClientKey to be 'test-client-key', got %s", client.TLSClientKey)
	}
	if client.Timeout != 1234 {
		t.Errorf("Expected Timeout to be 1234, got %d", client.Timeout)
	}
}

func TestNewKafkaClient_NegativeTimeout(t *testing.T) {
	client := NewKafkaClient(Options{BootstrapServers: "localhost:9092", Timeout: -5})
	if client.Timeout != 0 {
		t.Fatalf("expected sanitized timeout 0 got %d", client.Timeout)
	}
}

func TestKafkaClient_NewConnection_NoSASL(t *testing.T) {
	client := NewKafkaClient(Options{BootstrapServers: "localhost:9092"})
	err := client.NewConnection()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if client.Dialer == nil {
		t.Error("Expected Dialer to be initialized")
	}
	if client.Conn == nil {
		t.Error("Expected Conn to be initialized")
	}
}

func TestKafkaClient_Dispose(t *testing.T) {
	client := NewKafkaClient(Options{BootstrapServers: "localhost:9092"})
	client.Dispose() // Should not panic
}

func TestGetSASLMechanism_Unsupported(t *testing.T) {
	client := NewKafkaClient(Options{SaslMechanisms: "UNSUPPORTED"})
	_, err := getSASLMechanism(&client)
	if err == nil {
		t.Error("Expected error for unsupported SASL mechanism")
	}
}

func TestGetSASLMechanism_Supported(t *testing.T) {
	cases := []struct{ mech string }{{"PLAIN"}, {"SCRAM-SHA-256"}, {"SCRAM-SHA-512"}, {""}}
	for _, c := range cases {
		cl := NewKafkaClient(Options{SaslMechanisms: c.mech})
		if _, err := getSASLMechanism(&cl); err != nil {
			t.Fatalf("expected support for %s got %v", c.mech, err)
		}
	}
}

func TestNewStreamReader_EarliestAndLastN(t *testing.T) {
	cl := NewKafkaClient(Options{BootstrapServers: "localhost:9092"})
	ctx := context.Background()
	// Initialize connection; this config will allow creating a reader object
	if err := cl.NewConnection(); err != nil {
		t.Fatalf("NewConnection() error = %v", err)
	}

	// Earliest should set offset without error and return a reader
	reader, err := cl.NewStreamReader(ctx, "topic", 0, "earliest", 0)
	if err != nil {
		t.Fatalf("earliest: expected no error, got %v", err)
	}
	if reader == nil {
		t.Fatalf("earliest: expected non-nil reader")
	}
	if reader != nil {
		_ = reader.Close()
	}

	// lastN requires leader offset lookups; with no real broker, it should error
	reader2, err2 := cl.NewStreamReader(ctx, "topic", 0, "lastN", 100)
	if err2 == nil {
		t.Fatalf("lastN: expected an error due to no broker, got nil")
	}
	if reader2 != nil {
		t.Fatalf("lastN: expected nil reader on error, got non-nil")
	}
}

func TestGetKafkaLogger(t *testing.T) {
	logger, errorLogger := getKafkaLogger("debug")
	logger("test debug")
	errorLogger("test error")
	logger, errorLogger = getKafkaLogger("error")
	logger("should not print")
	errorLogger("should print error")
}

func TestNewKafkaClient_BrokerParsing(t *testing.T) {
	tests := []struct {
		name             string
		bootstrapServers string
		expectedBrokers  []string
	}{
		{
			name:             "single broker",
			bootstrapServers: "localhost:9092",
			expectedBrokers:  []string{"localhost:9092"},
		},
		{
			name:             "multiple brokers",
			bootstrapServers: "broker1:9092,broker2:9092,broker3:9092",
			expectedBrokers:  []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:             "brokers with spaces",
			bootstrapServers: "broker1:9092 , broker2:9092 , broker3:9092",
			expectedBrokers:  []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:             "empty broker filtered",
			bootstrapServers: "broker1:9092,,broker2:9092",
			expectedBrokers:  []string{"broker1:9092", "broker2:9092"},
		},
		{
			name:             "only spaces and commas",
			bootstrapServers: " , , ",
			expectedBrokers:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewKafkaClient(Options{BootstrapServers: tt.bootstrapServers})

			if len(client.Brokers) != len(tt.expectedBrokers) {
				t.Errorf("Expected %d brokers, got %d", len(tt.expectedBrokers), len(client.Brokers))
			}

			for i, expected := range tt.expectedBrokers {
				if i >= len(client.Brokers) || client.Brokers[i] != expected {
					t.Errorf("Expected broker %s at index %d, got %s", expected, i, client.Brokers[i])
				}
			}
		})
	}
}

func TestNewKafkaClient_TimeoutHandling(t *testing.T) {
	tests := []struct {
		name            string
		timeout         int32
		expectedTimeout int32
	}{
		{"positive timeout", 5000, 5000},
		{"zero timeout", 0, 0},
		{"negative timeout sanitized", -1000, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewKafkaClient(Options{Timeout: tt.timeout})
			if client.Timeout != tt.expectedTimeout {
				t.Errorf("Expected timeout %d, got %d", tt.expectedTimeout, client.Timeout)
			}
		})
	}
}

func TestNewConnection_SecurityProtocols(t *testing.T) {
	tests := []struct {
		name             string
		securityProtocol string
		saslMechanisms   string
		saslUsername     string
		saslPassword     string
		expectError      bool
	}{
		{"PLAINTEXT", "PLAINTEXT", "", "", "", false},
		{"SASL_PLAINTEXT with PLAIN", "SASL_PLAINTEXT", "PLAIN", "user", "pass", false},
		{"SASL_PLAINTEXT with SCRAM-SHA-256", "SASL_PLAINTEXT", "SCRAM-SHA-256", "user", "pass", false},
		{"SASL_PLAINTEXT with SCRAM-SHA-512", "SASL_PLAINTEXT", "SCRAM-SHA-512", "user", "pass", false},
		{"SASL_PLAINTEXT with unsupported mechanism", "SASL_PLAINTEXT", "UNSUPPORTED", "user", "pass", true},
		{"SSL", "SSL", "", "", "", false},
		{"SASL_SSL", "SASL_SSL", "PLAIN", "user", "pass", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewKafkaClient(Options{
				BootstrapServers: "localhost:9092",
				SecurityProtocol: tt.securityProtocol,
				SaslMechanisms:   tt.saslMechanisms,
				SaslUsername:     tt.saslUsername,
				SaslPassword:     tt.saslPassword,
			})

			err := client.NewConnection()

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			} else if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tt.expectError {
				if client.Dialer == nil {
					t.Error("Expected Dialer to be initialized")
				}
				if client.Conn == nil {
					t.Error("Expected Conn to be initialized")
				}
			}
		})
	}
}

func TestIsTopicNotFound(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "unknown topic error",
			err:      errors.New("unknown topic or partition"),
			expected: true,
		},
		{
			name:     "not found error",
			err:      errors.New("topic not found"),
			expected: true,
		},
		{
			name:     "does not exist error",
			err:      errors.New("topic does not exist"),
			expected: true,
		},
		{
			name:     "case insensitive unknown",
			err:      errors.New("UNKNOWN TOPIC"),
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("connection failed"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTopicNotFound(tt.err)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v for error: %v", tt.expected, result, tt.err)
			}
		})
	}
}
