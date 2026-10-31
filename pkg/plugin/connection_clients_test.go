package plugin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
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

// selfSignedClientCert returns a PEM-encoded self-signed client certificate and key.
func selfSignedClientCert(t *testing.T) (certPEM, keyPEM string, cert *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "grafana-kafka-datasource-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM, cert
}

// newMTLSRegistry starts a TLS server that requires a client certificate signed
// by clientCA, and returns it with its CA certificate in PEM form.
func newMTLSRegistry(t *testing.T, clientCA *x509.Certificate) (*httptest.Server, string) {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Subject not found", http.StatusNotFound)
	}))
	pool := x509.NewCertPool()
	pool.AddCert(clientCA)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	return server, caPEM
}

func TestNewConnectionClients_SchemaRegistryCustomMTLS(t *testing.T) {
	clientCert, clientKey, clientCA := selfSignedClientCert(t)
	server, serverCA := newMTLSRegistry(t, clientCA)

	// Kafka TLS points at an unrelated CA; custom mode must ignore it.
	otherCA, _, _ := selfSignedClientCert(t)
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{"tlsAuthWithCACert": true, "schemaRegistryTlsMode": "custom"}`),
		DecryptedSecureJSONData: map[string]string{
			"tlsCACert":                   otherCA,
			"schemaRegistryTlsCACert":     serverCA,
			"schemaRegistryTlsClientCert": clientCert,
			"schemaRegistryTlsClientKey":  clientKey,
		},
	}
	clients, err := newConnectionClients(context.Background(), settings, false)
	if err != nil {
		t.Fatalf("newConnectionClients() error = %v", err)
	}

	ds := NewWithClient(&internalMockClient{schemaRegistryURL: server.URL}, clients.schemaRegistryHTTPClient)
	result, err := ds.ValidateSchemaRegistry(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != backend.HealthStatusOk {
		t.Fatalf("expected mTLS handshake to succeed, got %q", result.Message)
	}
}

func TestNewConnectionClients_SchemaRegistryInheritsKafkaTLS(t *testing.T) {
	clientCert, clientKey, clientCA := selfSignedClientCert(t)
	server, serverCA := newMTLSRegistry(t, clientCA)

	// Default mode: the Kafka TLS fields are reused for the registry, which is
	// the behaviour existing datasources rely on.
	settings := backend.DataSourceInstanceSettings{
		JSONData: []byte(`{"tlsAuth": true, "tlsAuthWithCACert": true}`),
		DecryptedSecureJSONData: map[string]string{
			"tlsCACert":     serverCA,
			"tlsClientCert": clientCert,
			"tlsClientKey":  clientKey,
		},
	}
	clients, err := newConnectionClients(context.Background(), settings, false)
	if err != nil {
		t.Fatalf("newConnectionClients() error = %v", err)
	}
	ds := NewWithClient(&internalMockClient{schemaRegistryURL: server.URL}, clients.schemaRegistryHTTPClient)
	result, err := ds.ValidateSchemaRegistry(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != backend.HealthStatusOk {
		t.Fatalf("expected inherited Kafka TLS to work, got %q", result.Message)
	}

	// Without a client certificate the server rejects the handshake.
	settings.DecryptedSecureJSONData = map[string]string{"tlsCACert": serverCA}
	settings.JSONData = []byte(`{"tlsAuthWithCACert": true}`)
	clients, err = newConnectionClients(context.Background(), settings, false)
	if err != nil {
		t.Fatalf("newConnectionClients() error = %v", err)
	}
	ds = NewWithClient(&internalMockClient{schemaRegistryURL: server.URL}, clients.schemaRegistryHTTPClient)
	result, _ = ds.ValidateSchemaRegistry(context.Background())
	if result.Status != backend.HealthStatusError {
		t.Fatal("expected handshake without client certificate to fail")
	}
}

func TestApplySchemaRegistryTLS(t *testing.T) {
	certPEM, keyPEM, _ := selfSignedClientCert(t)

	t.Run("kafka mode leaves options untouched", func(t *testing.T) {
		inherited := &httpclient.TLSOptions{CACertificate: "kafka-ca"}
		opts := httpclient.Options{TLS: inherited}
		err := applySchemaRegistryTLS(&opts, backend.DataSourceInstanceSettings{JSONData: []byte(`{"schemaRegistryTlsMode": "kafka"}`)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.TLS != inherited {
			t.Fatal("expected inherited TLS options to be kept")
		}
	})

	t.Run("custom mode keeps SDK TLS version bounds", func(t *testing.T) {
		opts := httpclient.Options{TLS: &httpclient.TLSOptions{CACertificate: "kafka-ca", MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13}}
		err := applySchemaRegistryTLS(&opts, backend.DataSourceInstanceSettings{
			JSONData: []byte(`{"schemaRegistryTlsMode": "custom", "schemaRegistryTlsSkipVerify": true, "schemaRegistryTlsServerName": "sr.internal"}`),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.TLS.CACertificate != "" || !opts.TLS.InsecureSkipVerify || opts.TLS.ServerName != "sr.internal" {
			t.Fatalf("unexpected TLS options: %+v", opts.TLS)
		}
		if opts.TLS.MinVersion != tls.VersionTLS13 || opts.TLS.MaxVersion != tls.VersionTLS13 {
			t.Fatalf("expected TLS 1.3 bounds to be kept, got %+v", opts.TLS)
		}
	})

	errorCases := []struct {
		name   string
		secure map[string]string
		want   string
	}{
		{name: "invalid CA", secure: map[string]string{"schemaRegistryTlsCACert": "not a pem"}, want: "invalid Schema Registry TLS configuration"},
		{name: "cert without key", secure: map[string]string{"schemaRegistryTlsClientCert": certPEM}, want: "must be provided together"},
		{name: "mismatched key", secure: map[string]string{"schemaRegistryTlsClientCert": certPEM, "schemaRegistryTlsClientKey": strings.Replace(keyPEM, "A", "B", 5)}, want: "invalid Schema Registry TLS configuration"},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := httpclient.Options{}
			err := applySchemaRegistryTLS(&opts, backend.DataSourceInstanceSettings{
				JSONData:                []byte(`{"schemaRegistryTlsMode": "custom"}`),
				DecryptedSecureJSONData: tc.secure,
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}
