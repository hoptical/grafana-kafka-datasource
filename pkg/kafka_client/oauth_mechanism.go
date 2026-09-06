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

func newOAuthBearerMechanism(client *KafkaClient) *oauthBearerMechanism {
	timeout := dialerTimeout
	if client.Timeout > 0 {
		timeout = time.Duration(client.Timeout) * time.Millisecond
	}
	return &oauthBearerMechanism{
		tokenEndpoint: client.SaslOauthTokenEndpoint,
		clientID:      client.SaslOauthClientId,
		clientSecret:  client.SaslOauthClientSecret,
		scope:         client.SaslOauthScope,
		httpClient:    &http.Client{Timeout: timeout, CheckRedirect: rejectRedirects},
		nowFunc:       time.Now,
	}
}

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
		return "", fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
