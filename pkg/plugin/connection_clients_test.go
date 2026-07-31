package plugin

import (
	"context"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestNewConnectionClients_PDCDisabled(t *testing.T) {
	clients, err := newConnectionClients(context.Background(), backend.DataSourceInstanceSettings{}, false)
	if err != nil {
		t.Fatalf("newConnectionClients() error = %v", err)
	}
	if clients.schemaRegistryHTTPClient == nil {
		t.Fatal("expected Schema Registry HTTP client")
	}
	if clients.kafkaDialFunc != nil {
		t.Fatal("expected no Kafka dial function when PDC is disabled")
	}
}

func TestNewConnectionClients_PDCUnavailable(t *testing.T) {
	_, err := newConnectionClients(context.Background(), backend.DataSourceInstanceSettings{}, true)
	if err == nil {
		t.Fatal("expected an error when PDC is enabled but unavailable")
	}
	if err.Error() != "PDC is enabled for the datasource but unavailable in Grafana" {
		t.Fatalf("unexpected error: %v", err)
	}
}
