package kafka_client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestOAuthMechanism(tokenEndpoint string) *oauthBearerMechanism {
	return &oauthBearerMechanism{
		tokenEndpoint: tokenEndpoint,
		clientID:      "client-id",
		clientSecret:  "client-secret",
		scope:         "kafka",
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		nowFunc:       time.Now,
	}
}

func tokenHandler(accessToken string, expiresIn int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.PostForm.Get("grant_type") != "client_credentials" {
			http.Error(w, "expected client_credentials grant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":"%s","expires_in":%d}`, accessToken, expiresIn)
	}
}

func TestOAuthBearerMechanism_Name(t *testing.T) {
	m := newTestOAuthMechanism("http://example.com/token")
	if m.Name() != "OAUTHBEARER" {
		t.Fatalf("expected Name() to be OAUTHBEARER, got %s", m.Name())
	}
}

func TestOAuthBearerMechanism_Start_WireFormat(t *testing.T) {
	server := httptest.NewServer(tokenHandler("test-token", 600))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	sess, ir, err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if sess != m {
		t.Fatal("expected Start() to return itself as the StateMachine")
	}
	expected := "n,,\x01auth=Bearer test-token\x01\x01"
	if string(ir) != expected {
		t.Fatalf("expected initial response %q, got %q", expected, string(ir))
	}
}

func TestOAuthBearerMechanism_Next_CompletesImmediately(t *testing.T) {
	m := newTestOAuthMechanism("http://example.com/token")
	done, resp, err := m.Next(context.Background(), nil)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if !done {
		t.Fatal("expected Next() to report done=true")
	}
	if resp != nil {
		t.Fatalf("expected nil response, got %v", resp)
	}
}

func TestOAuthBearerMechanism_CachesTokenWithinTTL(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		tokenHandler("cached-token", 600)(w, r)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)

	for i := 0; i < 3; i++ {
		if _, _, err := m.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	}

	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Fatalf("expected exactly 1 token request while cache is valid, got %d", got)
	}
}

func TestOAuthBearerMechanism_RefreshesAfterExpiry(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		tokenHandler(fmt.Sprintf("token-%d", n), 600)(w, r)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	baseTime := time.Now()
	m.nowFunc = func() time.Time { return baseTime }

	if _, ir, err := m.Start(context.Background()); err != nil || string(ir) != "n,,\x01auth=Bearer token-1\x01\x01" {
		t.Fatalf("expected first token, got ir=%q err=%v", ir, err)
	}

	// Advance time past the cached expiry (600s TTL - 30s skew = 570s window).
	m.nowFunc = func() time.Time { return baseTime.Add(600 * time.Second) }

	if _, ir, err := m.Start(context.Background()); err != nil || string(ir) != "n,,\x01auth=Bearer token-2\x01\x01" {
		t.Fatalf("expected refreshed token, got ir=%q err=%v", ir, err)
	}

	if got := atomic.LoadInt32(&requestCount); got != 2 {
		t.Fatalf("expected exactly 2 token requests after expiry, got %d", got)
	}
}

func TestOAuthBearerMechanism_ConcurrentStartSingleFetch(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		time.Sleep(10 * time.Millisecond) // widen the race window
		tokenHandler("concurrent-token", 600)(w, r)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := m.Start(context.Background()); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected Start() error: %v", err)
	}

	if got := atomic.LoadInt32(&requestCount); got != 1 {
		t.Fatalf("expected exactly 1 token request under concurrent access, got %d", got)
	}
}

func TestOAuthBearerMechanism_TokenEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for non-200 token endpoint response")
	}
}

func TestOAuthBearerMechanism_MalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `not-json`)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected error for malformed token endpoint response")
	}
}

func TestOAuthBearerMechanism_MissingAccessToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"expires_in":600}`)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected error for missing access_token")
	}
}

func TestOAuthBearerMechanism_MissingExpiresIn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"access_token":"token"}`)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected error for missing expires_in")
	}
}

func TestNewOAuthBearerMechanism_UsesClientTimeout(t *testing.T) {
	client := NewKafkaClient(Options{
		BootstrapServers:       "localhost:9092",
		SaslOauthTokenEndpoint: "https://idp.example.com/token",
		SaslOauthClientId:      "id",
		SaslOauthClientSecret:  "secret",
		SaslOauthScope:         "kafka",
		Timeout:                5000,
	})
	m := newOAuthBearerMechanism(&client)
	if m.httpClient.Timeout != 5*time.Second {
		t.Fatalf("expected http client timeout to derive from client.Timeout, got %v", m.httpClient.Timeout)
	}
	if m.tokenEndpoint != "https://idp.example.com/token" || m.clientID != "id" || m.clientSecret != "secret" || m.scope != "kafka" {
		t.Fatal("expected OAuth fields to be copied from the client")
	}
}
