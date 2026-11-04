package plugin

import (
	"context"
	"encoding/json"
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
	oauthHTTPClient          *http.Client
	kafkaDialFunc            func(context.Context, string, string) (net.Conn, error)
}

func newConnectionClients(ctx context.Context, settings backend.DataSourceInstanceSettings, enablePDC bool) (connectionClients, error) {
	opts, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to get HTTP client options: %w", err)
	}

	// Build the OAuth token client from the base options, before the Schema
	// Registry TLS override is applied.
	oauthHTTPClient, err := httpclient.New(oauthTokenHTTPClientOptions(opts))
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to create OAuth token HTTP client: %w", err)
	}

	if err := applySchemaRegistryTLS(&opts, settings); err != nil {
		return connectionClients{}, err
	}

	schemaRegistryHTTPClient, err := httpclient.New(opts)
	if err != nil {
		return connectionClients{}, fmt.Errorf("failed to create Schema Registry HTTP client: %w", err)
	}

	clients := connectionClients{schemaRegistryHTTPClient: schemaRegistryHTTPClient, oauthHTTPClient: oauthHTTPClient}
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

// oauthTokenHTTPClientOptions derives options for OAuth token requests from the
// datasource HTTP options. The token endpoint is a different server than Kafka,
// so the Kafka TLS settings (custom CA, skip verify, client certs) and any
// datasource basic auth are dropped; only the TLS version bounds are kept.
// Proxy (PDC) options and middlewares are preserved.
func oauthTokenHTTPClientOptions(opts httpclient.Options) httpclient.Options {
	if opts.TLS != nil {
		opts.TLS = &httpclient.TLSOptions{MinVersion: opts.TLS.MinVersion, MaxVersion: opts.TLS.MaxVersion}
	}
	opts.BasicAuth = nil
	return opts
}

// Schema Registry TLS modes. "kafka" (the default, and the behaviour before the
// option existed) lets the SDK reuse the Kafka TLS fields for the registry
// HTTP client; "custom" uses registry-specific certificates instead.
const (
	schemaRegistryTLSModeKafka  = "kafka"
	schemaRegistryTLSModeCustom = "custom"
)

type schemaRegistryTLSSettings struct {
	Mode       string `json:"schemaRegistryTlsMode"`
	SkipVerify bool   `json:"schemaRegistryTlsSkipVerify"`
	ServerName string `json:"schemaRegistryTlsServerName"`
}

// applySchemaRegistryTLS replaces the TLS options inherited from the Kafka TLS
// settings with registry-specific ones when the custom mode is selected.
// Proxy (PDC) options and any SDK-enforced TLS version bounds are kept.
func applySchemaRegistryTLS(opts *httpclient.Options, settings backend.DataSourceInstanceSettings) error {
	var cfg schemaRegistryTLSSettings
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &cfg); err != nil {
			return fmt.Errorf("failed to parse Schema Registry TLS settings: %w", err)
		}
	}
	if cfg.Mode != schemaRegistryTLSModeCustom {
		return nil
	}

	secure := settings.DecryptedSecureJSONData
	tlsOpts := &httpclient.TLSOptions{
		CACertificate:      secure["schemaRegistryTlsCACert"],
		ClientCertificate:  secure["schemaRegistryTlsClientCert"],
		ClientKey:          secure["schemaRegistryTlsClientKey"],
		InsecureSkipVerify: cfg.SkipVerify,
		ServerName:         cfg.ServerName,
	}
	if opts.TLS != nil {
		tlsOpts.MinVersion = opts.TLS.MinVersion
		tlsOpts.MaxVersion = opts.TLS.MaxVersion
	}
	if (tlsOpts.ClientCertificate == "") != (tlsOpts.ClientKey == "") {
		return fmt.Errorf("schema Registry TLS: client certificate and client key must be provided together")
	}

	opts.TLS = tlsOpts
	if _, err := httpclient.GetTLSConfig(*opts); err != nil {
		return fmt.Errorf("invalid Schema Registry TLS configuration: %w", err)
	}
	return nil
}
