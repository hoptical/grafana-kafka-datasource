package kafka_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/segmentio/kafka-go/sasl"
)

// maxRefreshSkew caps how early a token is proactively refreshed before its
// reported expiry, so short-lived tokens (e.g. 600s per KIP-255 IdPs) don't
// refresh needlessly early.
const maxRefreshSkew = 30 * time.Second

// refreshSkewFraction is the fraction of a token's TTL reserved as refresh
// skew when the TTL is shorter than 2*maxRefreshSkew.
const refreshSkewFraction = 0.2

// maxTokenResponseBytes bounds how much of the token endpoint's response body
// is read, so a misbehaving or malicious endpoint can't exhaust memory with an
// oversized response.
const maxTokenResponseBytes = 1 << 20 // 1MB

// maxLoggedBodyBytes caps how much of a non-200 token response body is logged.
const maxLoggedBodyBytes = 1024

// truncateForLog caps s at n bytes (without splitting a UTF-8 character),
// appending a marker when it was shortened.
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "...(truncated)"
}

// redactEndpoint returns the endpoint with userinfo, query and fragment removed
// so it is safe to log. It falls back to a placeholder if the URL can't be parsed.
func redactEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparseable token endpoint>"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

var errRedirectNotAllowed = errors.New("redirects are not followed for OAuth token requests")

// oauthBearerMechanism implements the SASL/OAUTHBEARER (KIP-255, RFC 7628)
// mechanism using an OAuth2 client_credentials grant. kafka-go v0.4.47 does
// not ship this mechanism (only PLAIN and SCRAM-SHA-256/512 exist), so it is
// implemented here directly against kafka-go's sasl.Mechanism interface.
//
// A single instance is constructed per KafkaClient connection and reused
// across every dial (kafka-go calls Start once per new TCP connection), so
// the fetched token is cached and proactively refreshed rather than fetched
// on every connection attempt. Per sasl.Mechanism's contract, instances must
// be safe for concurrent use by multiple goroutines.
type oauthBearerMechanism struct {
	tokenEndpoint string
	clientID      string
	clientSecret  string
	scope         string
	httpClient    *http.Client
	nowFunc       func() time.Time

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// newOAuthBearerMechanism builds the mechanism from the client's OAuth settings,
// using the client timeout (or dialerTimeout) for token requests. Token requests
// go through the Grafana SDK HTTP client (client.OAuthHTTPClient) so the SDK
// middlewares (response limit, PDC secure socks proxy, ...) apply. A shallow copy
// is made so the per-mechanism timeout and redirect rejection don't alter the
// shared client.
func newOAuthBearerMechanism(client *KafkaClient) (*oauthBearerMechanism, error) {
	timeout := dialerTimeout
	if client.Timeout > 0 {
		timeout = time.Duration(client.Timeout) * time.Millisecond
	}
	base := client.OAuthHTTPClient
	if base == nil {
		var err error
		base, err = httpclient.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create OAuth token HTTP client: %w", err)
		}
	}
	httpClient := *base
	httpClient.Timeout = timeout
	httpClient.CheckRedirect = rejectRedirects
	return &oauthBearerMechanism{
		tokenEndpoint: client.SaslOauthTokenEndpoint,
		clientID:      client.SaslOauthClientId,
		clientSecret:  client.SaslOauthClientSecret,
		scope:         client.SaslOauthScope,
		httpClient:    &httpClient,
		nowFunc:       time.Now,
	}, nil
}

// rejectRedirects is an http.Client CheckRedirect that refuses to follow any
// redirect, so the client secret is never forwarded to another host.
func rejectRedirects(req *http.Request, via []*http.Request) error {
	return errRedirectNotAllowed
}

// validateTokenEndpoint requires HTTPS for the token endpoint, since the
// request carries the client secret in its body. Plaintext HTTP is allowed
// only against loopback addresses, so local/test token servers (e.g.
// httptest.NewServer) keep working without weakening the real-world
// requirement.
func validateTokenEndpoint(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid token endpoint URL: %w", err)
	}
	if u.Scheme == "https" {
		return nil
	}
	host := u.Hostname()
	if host == "localhost" || net.ParseIP(host).IsLoopback() {
		return nil
	}
	return fmt.Errorf("token endpoint must use https (got %q); plaintext http is only allowed for localhost", rawURL)
}

// Name returns the SASL mechanism name.
func (m *oauthBearerMechanism) Name() string {
	return "OAUTHBEARER"
}

// Start fetches (or reuses a cached) bearer token and builds the initial
// GS2 response per RFC 7628: "n,,\x01auth=Bearer <token>\x01\x01".
func (m *oauthBearerMechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	token, err := m.getToken(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch OAuth token: %w", err)
	}
	msg := []byte(fmt.Sprintf("n,,\x01auth=Bearer %s\x01\x01", token))
	return m, msg, nil
}

// Next is a formality: kafka-go's SaslAuthenticate round trip already turns a
// non-zero broker error code into an error before Next is ever invoked, so a
// successful OAUTHBEARER exchange always calls Next exactly once with an
// empty challenge (mirroring sasl/plain.Mechanism.Next in this same
// kafka-go version).
func (m *oauthBearerMechanism) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	return true, nil, nil
}

type oauthTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

// getToken returns the cached token, or fetches a new one from the token
// endpoint when none is cached or the cached one is about to expire.
func (m *oauthBearerMechanism) getToken(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.token != "" && m.nowFunc().Before(m.expiresAt) {
		return m.token, nil
	}

	if err := validateTokenEndpoint(m.tokenEndpoint); err != nil {
		return "", err
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", m.clientID)
	form.Set("client_secret", m.clientSecret)
	if m.scope != "" {
		form.Set("scope", m.scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token endpoint request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.DefaultLogger.Error("failed to close OAuth token response body", "error", closeErr)
		}
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseBytes+1))
	if err != nil {
		return "", fmt.Errorf("failed to read token endpoint response: %w", err)
	}
	if len(body) > maxTokenResponseBytes {
		return "", fmt.Errorf("token endpoint response exceeds maximum size of %d bytes", maxTokenResponseBytes)
	}

	if resp.StatusCode != http.StatusOK {
		// The response body may contain provider-specific details, so keep it out
		// of the returned error (which is logged at Error level and returned to
		// the caller) and only log it at Debug level for troubleshooting.
		log.DefaultLogger.Debug("OAuth token endpoint returned a non-200 status",
			"endpoint", redactEndpoint(m.tokenEndpoint), "status", resp.StatusCode, "body", truncateForLog(strings.TrimSpace(string(body)), maxLoggedBodyBytes))
		return "", fmt.Errorf("token endpoint returned status %d", resp.StatusCode)
	}

	var tokenResp oauthTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token endpoint response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("token endpoint response missing access_token")
	}
	if tokenResp.ExpiresIn <= 0 {
		return "", fmt.Errorf("token endpoint response missing or invalid expires_in")
	}

	ttl := time.Duration(tokenResp.ExpiresIn) * time.Second
	skew := time.Duration(float64(ttl) * refreshSkewFraction)
	if skew > maxRefreshSkew {
		skew = maxRefreshSkew
	}
	if skew < time.Second {
		skew = time.Second
	}

	m.token = tokenResp.AccessToken
	m.expiresAt = m.nowFunc().Add(ttl - skew)

	return m.token, nil
}
