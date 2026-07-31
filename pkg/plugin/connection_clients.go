package plugin

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
)

// connectionClients contains the protocol-specific clients created from one
// datasource connection configuration.
type connectionClients struct {
	schemaRegistryHTTPClient *http.Client
	kafkaDialFunc            func(context.Context, string, string) (net.Conn, error)
}

func newConnectionClients(ctx context.Context, settings backend.DataSourceInstanceSettings, enablePDC bool) (connectionClients, error) {
	opts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to get HTTP client options: %w", err)
	}

	schemaRegistryHTTPClient, err := httpclient.New(opts)
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to create Schema Registry HTTP client: %w", err)
	}

	clients := connectionClients{schemaRegistryHTTPClient: schemaRegistryHTTPClient}
	if !enablePDC {
		return clients, nil
	}

	proxyClient, err := settings.ProxyClient(ctx)
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to create PDC client: %w", err)
	}
	if !proxyClient.SecureSocksProxyEnabled() {
		return connectionClients{}, fmt.Errorf("PDC is enabled for the datasource but unavailable in Grafana")
	}

	proxyDialer, err := proxyClient.NewSecureSocksProxyContextDialer()
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to create PDC dialer: %w", err)
	}
	contextDialer, ok := proxyDialer.(interface {
		DialContext(context.Context, string, string) (net.Conn, error)
	})
	if !ok {
		return connectionClients{}, fmt.Errorf("PDC dialer does not support context dialing")
	}
	clients.kafkaDialFunc = contextDialer.DialContext

	return clients, nil
}
