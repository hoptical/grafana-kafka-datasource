package kafka_client

import (
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
