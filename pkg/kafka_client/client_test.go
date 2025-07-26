package kafka_client

import (
	"testing"
)

func TestNewKafkaClient_Defaults(t *testing.T) {
	options := Options{BootstrapServers: "localhost:9092"}
	client := NewKafkaClient(options)
	if client.BootstrapServers != "localhost:9092" {
		t.Errorf("Expected BootstrapServers to be 'localhost:9092', got %s", client.BootstrapServers)
	}
	if client.HealthcheckTimeout <= 0 {
		t.Error("Expected HealthcheckTimeout to be set to default if not provided")
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

func TestGetKafkaLogger(t *testing.T) {
	logger, errorLogger := getKafkaLogger("debug")
	logger("test debug")
	errorLogger("test error")
	logger, errorLogger = getKafkaLogger("error")
	logger("should not print")
	errorLogger("should print error")
}
