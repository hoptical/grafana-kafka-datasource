package kafka_client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func newTestOAuthMechanism(tokenEndpoint string) *oauthBearerMechanism {
	return &oauthBearerMechanism{
		tokenEndpoint: tokenEndpoint,
		clientID:      "client-id",
		clientSecret:  "client-secret",
		scope:         "kafka",
		httpClient:    &http.Client{Timeout: 5 * time.Second, CheckRedirect: rejectRedirects},
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
		http.Error(w, "sensitive-provider-detail", http.StatusUnauthorized)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for non-200 token endpoint response")
	}
	if !strings.Contains(err.Error(), "status 401") {
		t.Fatalf("expected status code in error, got %v", err)
	}
	if strings.Contains(err.Error(), "sensitive-provider-detail") {
		t.Fatalf("response body must not be included in the error, got %v", err)
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

func TestOAuthBearerMechanism_RejectsNonLoopbackHTTP(t *testing.T) {
	m := newTestOAuthMechanism("http://idp.example.com/token")
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for non-HTTPS, non-loopback token endpoint")
	}
	if !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("expected https requirement error, got %v", err)
	}
}

// TestOAuthBearerMechanism_EndpointErrorsAreRedacted ensures credentials in the
// token endpoint URL (userinfo, query, or fragments of a malformed URL) never
// appear in returned errors, which are logged at Error level.
func TestOAuthBearerMechanism_EndpointErrorsAreRedacted(t *testing.T) {
	// Grab a free loopback port and close it so the request fails to connect.
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	cases := map[string]struct {
		endpoint string
		wantMsg  string
	}{
		"non-https endpoint": {
			endpoint: "http://user:pwsecret@idp.example.com/token?client_secret=qsecret",
			wantMsg:  "must use https",
		},
		"unparseable endpoint": {
			endpoint: "http://user:pwsecret@idp.example.com:qsecret/token",
			wantMsg:  "invalid token endpoint URL",
		},
		"request failure": {
			endpoint: closedURL + "/token?client_secret=qsecret",
			wantMsg:  "token endpoint request failed",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			m := newTestOAuthMechanism(tc.endpoint)
			_, _, err := m.Start(context.Background())
			if err == nil {
				t.Fatal("expected error")
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("expected error containing %q, got %v", tc.wantMsg, err)
			}
			for _, secret := range []string{"pwsecret", "qsecret", "user:"} {
				if strings.Contains(msg, secret) {
					t.Fatalf("error leaks %q: %v", secret, err)
				}
			}
		})
	}
}

func TestOAuthBearerMechanism_AllowsLoopbackHTTP(t *testing.T) {
	server := httptest.NewServer(tokenHandler("loopback-token", 600))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	if _, _, err := m.Start(context.Background()); err != nil {
		t.Fatalf("expected loopback http token endpoint to be allowed, got error: %v", err)
	}
}

func TestOAuthBearerMechanism_RejectsRedirect(t *testing.T) {
	target := httptest.NewServer(tokenHandler("redirected-token", 600))
	defer target.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when token endpoint issues a redirect")
	}
}

func TestOAuthBearerMechanism_RejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		oversized := strings.Repeat("a", maxTokenResponseBytes+1)
		_, _ = fmt.Fprintf(w, `{"access_token":"%s","expires_in":600}`, oversized)
	}))
	defer server.Close()

	m := newTestOAuthMechanism(server.URL)
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for oversized token endpoint response")
	}
	if !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("expected size-limit error, got %v", err)
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
	m, err := newOAuthBearerMechanism(&client)
	if err != nil {
		t.Fatalf("newOAuthBearerMechanism() error = %v", err)
	}
	if m.httpClient.CheckRedirect == nil {
		t.Fatal("expected redirects to be rejected")
	}
	if m.httpClient.Timeout != 5*time.Second {
		t.Fatalf("expected http client timeout to derive from client.Timeout, got %v", m.httpClient.Timeout)
	}
	if m.tokenEndpoint != "https://idp.example.com/token" || m.clientID != "id" || m.clientSecret != "secret" || m.scope != "kafka" {
		t.Fatal("expected OAuth fields to be copied from the client")
	}
}

type countingTransport struct{ calls int32 }

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	atomic.AddInt32(&c.calls, 1)
	return http.DefaultTransport.RoundTrip(r)
}

func TestNewOAuthBearerMechanism_UsesInjectedSDKClient(t *testing.T) {
	server := httptest.NewServer(tokenHandler("tok", 600))
	defer server.Close()

	transport := &countingTransport{}
	base := &http.Client{Transport: transport}
	client := NewKafkaClient(Options{
		BootstrapServers:       "localhost:9092",
		SaslOauthTokenEndpoint: server.URL,
		Timeout:                3000,
	})
	client.OAuthHTTPClient = base

	m, err := newOAuthBearerMechanism(&client)
	if err != nil {
		t.Fatal(err)
	}
	if m.httpClient == base {
		t.Fatal("expected a copy, not the shared client")
	}
	if m.httpClient.Timeout != 3*time.Second || m.httpClient.CheckRedirect == nil {
		t.Fatal("expected per-mechanism timeout and redirect rejection")
	}
	if base.CheckRedirect != nil || base.Timeout != 0 {
		t.Fatal("shared client must not be modified")
	}
	if _, _, err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&transport.calls) != 1 {
		t.Fatal("expected token request to go through the injected client's transport")
	}
}

// captureLogger records Debug calls; other levels are ignored.
type captureLogger struct {
	log.Logger
	mu    sync.Mutex
	debug [][]any
}

// Debug records the call for later inspection.
func (c *captureLogger) Debug(msg string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.debug = append(c.debug, append([]any{msg}, args...))
}

// field returns the value of the first logged key/value pair named name.
func (c *captureLogger) field(name string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, call := range c.debug {
		for i := 1; i+1 < len(call); i += 2 {
			if call[i] == name {
				return call[i+1], true
			}
		}
	}
	return nil, false
}

// TestTruncateForLog covers bodies below, at, and above the cap.
func TestTruncateForLog(t *testing.T) {
	short := strings.Repeat("a", 10)
	exact := strings.Repeat("a", maxLoggedBodyBytes)
	long := strings.Repeat("a", maxLoggedBodyBytes+50)

	if got := truncateForLog(short, maxLoggedBodyBytes); got != short {
		t.Fatalf("short body should be unchanged, got %q", got)
	}
	if got := truncateForLog(exact, maxLoggedBodyBytes); got != exact {
		t.Fatal("body of exactly the limit should be unchanged")
	}
	want := exact + "...(truncated)"
	if got := truncateForLog(long, maxLoggedBodyBytes); got != want {
		t.Fatalf("long body should be capped, got len %d", len(got))
	}
	// A cut in the middle of a multi-byte character must not yield invalid UTF-8.
	if got := truncateForLog("ééé", 3); !utf8.ValidString(got) {
		t.Fatalf("truncated output is not valid UTF-8: %q", got)
	}
}

// TestRedactEndpoint verifies userinfo, query and fragment are stripped.
func TestRedactEndpoint(t *testing.T) {
	cases := map[string]string{
		"https://user:pw@idp.example.com/oauth/token?secret=x#frag": "https://idp.example.com/oauth/token",
		"https://idp.example.com/token":                             "https://idp.example.com/token",
		"://bad":                                                    "<unparseable token endpoint>",
		"not a url":                                                 "<unparseable token endpoint>",
	}
	for in, want := range cases {
		if got := redactEndpoint(in); got != want {
			t.Errorf("redactEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestOAuthBearerMechanism_NonOKDebugLogIsTruncatedAndRedacted checks the Debug event contents.
func TestOAuthBearerMechanism_NonOKDebugLogIsTruncatedAndRedacted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(strings.Repeat("x", maxLoggedBodyBytes+500)))
	}))
	defer server.Close()

	orig := log.DefaultLogger
	capture := &captureLogger{Logger: log.NewNullLogger()}
	log.DefaultLogger = capture
	defer func() { log.DefaultLogger = orig }()

	m := newTestOAuthMechanism(server.URL + "/token?client_secret=leak")
	_, _, err := m.Start(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	body, ok := capture.field("body")
	if !ok {
		t.Fatal("expected debug log with body field")
	}
	if want := strings.Repeat("x", maxLoggedBodyBytes) + "...(truncated)"; body != want {
		t.Fatalf("expected truncated body in debug log, got len %d", len(body.(string)))
	}
	endpoint, ok := capture.field("endpoint")
	if !ok {
		t.Fatal("expected endpoint field in debug log")
	}
	if strings.Contains(endpoint.(string), "leak") || strings.Contains(endpoint.(string), "?") {
		t.Fatalf("endpoint must be redacted, got %v", endpoint)
	}
}
