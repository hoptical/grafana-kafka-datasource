package kafka_client

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/segmentio/kafka-go/sasl"
)

const testKrb5Conf = `[libdefaults]
default_realm = EXAMPLE.COM

[realms]
EXAMPLE.COM = {
  kdc = kdc.example.com
}
`

func newTestKerberosOptions(overrides func(*KafkaClient)) *KafkaClient {
	c := &KafkaClient{
		SaslGssapiServiceName: "kafka",
		SaslGssapiRealm:       "EXAMPLE.COM",
		SaslGssapiUsername:    "grafana",
		SaslGssapiAuthType:    "password",
		SaslGssapiKrb5Config:  testKrb5Conf,
		SaslGssapiPassword:    "grafana-password",
	}
	if overrides != nil {
		overrides(c)
	}
	return c
}

func TestNewGSSAPIMechanism_Valid(t *testing.T) {
	m, err := newGSSAPIMechanism(newTestKerberosOptions(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.Name() != "GSSAPI" {
		t.Errorf("Name() = %q, want GSSAPI", m.Name())
	}
	if m.serviceName != "kafka" {
		t.Errorf("serviceName = %q, want kafka", m.serviceName)
	}
}

func TestNewGSSAPIMechanism_DefaultsServiceName(t *testing.T) {
	m, err := newGSSAPIMechanism(newTestKerberosOptions(func(c *KafkaClient) {
		c.SaslGssapiServiceName = ""
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.serviceName != defaultKerberosServiceName {
		t.Errorf("serviceName = %q, want default %q", m.serviceName, defaultKerberosServiceName)
	}
}

func TestNewGSSAPIMechanism_Errors(t *testing.T) {
	tests := []struct {
		name      string
		overrides func(*KafkaClient)
		wantErr   string
	}{
		{
			name: "missing krb5 config",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiKrb5Config = ""
			},
			wantErr: "krb5.conf",
		},
		{
			// gokrb5's krb5.conf parser is deliberately lenient: it does not
			// return an error for unrecognized syntax, it simply produces a
			// config with no realms. Our realmConfigured check is therefore
			// the actual safety net for a garbled or empty krb5.conf, not
			// parseKrb5Config's own error path.
			name: "malformed krb5 config yields no realms",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiKrb5Config = "not a valid krb5.conf {{{"
			},
			wantErr: "not defined",
		},
		{
			name: "realm not in krb5 config",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiRealm = "OTHER.COM"
			},
			wantErr: "not defined",
		},
		{
			name: "realm case mismatch is not silently accepted",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiRealm = "example.com"
			},
			wantErr: "not defined",
		},
		{
			name: "principal includes realm suffix",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiUsername = "grafana@EXAMPLE.COM"
			},
			wantErr: "must not include the realm",
		},
		{
			name: "keytab auth without keytab content or path",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiAuthType = gssapiAuthTypeKeytab
			},
			wantErr: "keytab content or a keytab file path",
		},
		{
			name: "unsupported auth type is rejected rather than silently treated as password",
			overrides: func(c *KafkaClient) {
				c.SaslGssapiAuthType = "keytabb" // typo of "keytab"
			},
			wantErr: "unsupported GSSAPI authentication type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newGSSAPIMechanism(newTestKerberosOptions(tt.overrides))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestNewGSSAPIMechanism_KeytabAuth(t *testing.T) {
	kt := keytab.New()
	if err := kt.AddEntry("grafana", "EXAMPLE.COM", "grafana-password", time.Now(), 1, etypeID.AES256_CTS_HMAC_SHA1_96); err != nil {
		t.Fatalf("failed to build test keytab: %v", err)
	}
	raw, err := kt.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal test keytab: %v", err)
	}

	m, err := newGSSAPIMechanism(newTestKerberosOptions(func(c *KafkaClient) {
		c.SaslGssapiAuthType = gssapiAuthTypeKeytab
		c.SaslGssapiPassword = ""
		c.SaslGssapiKeytab = base64.StdEncoding.EncodeToString(raw)
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.keytab == nil {
		t.Fatal("expected keytab to be parsed and stored")
	}
}

func TestParseKrb5Config(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		cfg, err := parseKrb5Config(testKrb5Conf, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Realms) != 1 || cfg.Realms[0].Realm != "EXAMPLE.COM" {
			t.Errorf("unexpected realms: %+v", cfg.Realms)
		}
	})

	t.Run("path", func(t *testing.T) {
		path := writeTempFile(t, testKrb5Conf)
		cfg, err := parseKrb5Config("", path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Realms) != 1 || cfg.Realms[0].Realm != "EXAMPLE.COM" {
			t.Errorf("unexpected realms: %+v", cfg.Realms)
		}
	})

	t.Run("missing path", func(t *testing.T) {
		_, err := parseKrb5Config("", "/nonexistent/krb5.conf")
		if err == nil {
			t.Fatal("expected error for missing file")
		}
	})

	t.Run("both empty", func(t *testing.T) {
		_, err := parseKrb5Config("", "")
		if err == nil {
			t.Fatal("expected error when neither inline content nor a path is set")
		}
	})

	t.Run("inline takes precedence over path", func(t *testing.T) {
		path := writeTempFile(t, "[libdefaults]\ndefault_realm = FROM_PATH.COM\n[realms]\nFROM_PATH.COM = {\nkdc = kdc\n}\n")
		cfg, err := parseKrb5Config(testKrb5Conf, path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Realms[0].Realm != "EXAMPLE.COM" {
			t.Errorf("expected inline config to take precedence, got realm %q", cfg.Realms[0].Realm)
		}
	})
}

func TestParseKeytab(t *testing.T) {
	kt := keytab.New()
	if err := kt.AddEntry("kafka/broker.example.com", "EXAMPLE.COM", "svc-password", time.Now(), 1, etypeID.AES256_CTS_HMAC_SHA1_96); err != nil {
		t.Fatalf("failed to build test keytab: %v", err)
	}
	raw, err := kt.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal test keytab: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(raw)

	t.Run("inline base64", func(t *testing.T) {
		got, err := parseKeytab(b64, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Entries) != 1 {
			t.Errorf("expected 1 keytab entry, got %d", len(got.Entries))
		}
	})

	t.Run("path", func(t *testing.T) {
		path := writeTempBinaryFile(t, raw)
		got, err := parseKeytab("", path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got.Entries) != 1 {
			t.Errorf("expected 1 keytab entry, got %d", len(got.Entries))
		}
	})

	t.Run("bad base64", func(t *testing.T) {
		_, err := parseKeytab("not-valid-base64!!!", "")
		if err == nil || !strings.Contains(err.Error(), "base64") {
			t.Fatalf("expected a base64 error, got %v", err)
		}
	})

	t.Run("malformed keytab bytes", func(t *testing.T) {
		_, err := parseKeytab(base64.StdEncoding.EncodeToString([]byte("not a keytab")), "")
		if err == nil {
			t.Fatal("expected an error for malformed keytab bytes")
		}
		if strings.Contains(err.Error(), "base64") {
			t.Errorf("error should be distinguishable from a base64 error, got %v", err)
		}
	})

	t.Run("both empty", func(t *testing.T) {
		_, err := parseKeytab("", "")
		if err == nil {
			t.Fatal("expected error when neither keytab content nor a path is set")
		}
	})
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	return writeTempBinaryFile(t, []byte(content))
}

func writeTempBinaryFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kerberos-test-file")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func TestSpnForHost(t *testing.T) {
	tests := []struct {
		host string
		want string
	}{
		{"broker.example.com", "kafka/broker.example.com"},
		{"BROKER.EXAMPLE.COM", "kafka/broker.example.com"},
		{"broker.example.com.", "kafka/broker.example.com"},
	}
	for _, tt := range tests {
		if got := spnForHost("kafka", tt.host); got != tt.want {
			t.Errorf("spnForHost(%q) = %q, want %q", tt.host, got, tt.want)
		}
	}
}

func TestGssapiAuthenticatorChecksum_MatchesGoldenValue(t *testing.T) {
	// Golden value taken from gokrb5's own spnego/krb5Token_test.go
	// (AuthChksum), which computes the identical RFC 4121 §4.1.1 checksum
	// for GSSAPI's ContextFlagInteg|ContextFlagConf flag set.
	const golden = "100000000000000000000000000000000000000030000000"
	got := hex.EncodeToString(gssapiAuthenticatorChecksum())
	if got != golden {
		t.Errorf("checksum = %s, want %s", got, golden)
	}
}

func TestIsCredentialError(t *testing.T) {
	tests := []struct {
		err  string
		want bool
	}{
		{"login error: KDC_ERR_PREAUTH_FAILED occurred", true},
		{"KDC_ERR_C_PRINCIPAL_UNKNOWN: client not found", true},
		{"KDC_ERR_S_PRINCIPAL_UNKNOWN: service not found", true},
		{"KDC_ERR_CLIENT_REVOKED", true},
		{"dial tcp: connection refused", false},
		{"KRB_AP_ERR_SKEW: clocks too far apart", false},
	}
	for _, tt := range tests {
		if got := isCredentialError(errString(tt.err)); got != tt.want {
			t.Errorf("isCredentialError(%q) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// errString is a trivial error type for table-driven message matching.
type errString string

func (e errString) Error() string { return string(e) }

func TestGssapiMechanism_NegativeCache(t *testing.T) {
	var calls int32
	fakeErr := errString("KDC_ERR_PREAUTH_FAILED: bad password")
	now := time.Now()

	m := &gssapiMechanism{nowFunc: func() time.Time { return now }}
	m.newClient = func() (kerberosClient, error) {
		atomic.AddInt32(&calls, 1)
		return nil, fakeErr
	}

	if _, _, _, err := m.serviceTicket("kafka/broker.example.com"); err == nil {
		t.Fatal("expected an error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 call to newClient, got %d", got)
	}

	// Still within the credential-failure TTL: cached error is returned
	// without calling newClient again.
	if _, _, _, err := m.serviceTicket("kafka/broker.example.com"); err == nil {
		t.Fatal("expected the cached error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected newClient to still have been called once (cache hit), got %d calls", got)
	}

	// Advance past the TTL: the mechanism should retry.
	now = now.Add(gssapiCredentialFailureTTL + time.Second)
	if _, _, _, err := m.serviceTicket("kafka/broker.example.com"); err == nil {
		t.Fatal("expected an error")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected newClient to be called again after TTL expiry, got %d calls", got)
	}
}

// TestGssapiMechanism_ServiceTicketFailuresAreScopedPerSPN is a regression
// test for a bug where a service-ticket failure for one broker's SPN (e.g.
// an unknown-principal error because that broker's keytab entry is missing)
// was cached mechanism-wide, causing every *other* broker's otherwise-
// healthy ticket requests to fail too until the cache entry expired. Broker
// A failing must not block broker B from succeeding within the same TTL
// window.
func TestGssapiMechanism_ServiceTicketFailuresAreScopedPerSPN(t *testing.T) {
	const spnA = "kafka/broker-a.example.com"
	const spnB = "kafka/broker-b.example.com"

	fake := &fakeKerberosClient{
		failSPNs: map[string]error{
			spnA: errString("KDC_ERR_S_PRINCIPAL_UNKNOWN: server not found"),
		},
	}
	now := time.Now()
	m := &gssapiMechanism{nowFunc: func() time.Time { return now }}
	m.newClient = func() (kerberosClient, error) { return fake, nil }

	if _, _, _, err := m.serviceTicket(spnA); err == nil {
		t.Fatal("expected broker A's request to fail")
	}

	// Well within broker A's cached-failure TTL, broker B must still
	// succeed: its SPN is healthy and was never asked to fail.
	if _, _, _, err := m.serviceTicket(spnB); err != nil {
		t.Fatalf("expected broker B to succeed despite broker A's cached failure, got: %v", err)
	}

	// Broker A's failure is still cached (still within its TTL).
	if _, _, _, err := m.serviceTicket(spnA); err == nil {
		t.Fatal("expected broker A's cached failure to still apply")
	}
}

func TestGssapiMechanism_ConcurrentStart(t *testing.T) {
	kt, cname, sname, realm := testKeytabAndPrincipals()
	tkt, sessionKey, err := messages.NewTicket(
		cname, realm, sname, realm, types.NewKrbFlags(), kt,
		etypeID.AES256_CTS_HMAC_SHA1_96, 1,
		time.Now().UTC(), time.Now().UTC(), time.Now().UTC().Add(time.Hour), time.Now().UTC().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("failed to mint test ticket: %v", err)
	}

	var newClientCalls int32
	fake := &fakeKerberosClient{realm: realm, cname: cname, ticket: tkt, sessionKey: sessionKey}
	m := &gssapiMechanism{nowFunc: time.Now}
	m.newClient = func() (kerberosClient, error) {
		atomic.AddInt32(&newClientCalls, 1)
		return fake, nil
	}

	const n = 20
	sessions := make([]sasl.StateMachine, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ctx := sasl.WithMetadata(context.Background(), &sasl.Metadata{Host: "broker.example.com"})
			sess, _, err := m.Start(ctx)
			if err != nil {
				t.Errorf("Start() error: %v", err)
				return
			}
			sessions[i] = sess
		}(i)
	}
	wg.Wait()

	seen := make(map[sasl.StateMachine]bool)
	for _, s := range sessions {
		if s == nil {
			continue
		}
		if seen[s] {
			t.Error("Start() returned the same StateMachine instance to two callers")
		}
		seen[s] = true
	}
	if got := atomic.LoadInt32(&newClientCalls); got != 1 {
		t.Errorf("expected the Kerberos client to be constructed exactly once, got %d times", got)
	}
}

// fakeKerberosClient implements kerberosClient over an offline-minted
// ticket, so gssapiMechanism.Start/gssapiSession.Next can be exercised
// without a live KDC.
type fakeKerberosClient struct {
	realm      string
	cname      types.PrincipalName
	ticket     messages.Ticket
	sessionKey types.EncryptionKey
	destroyed  bool

	// failSPNs, when set, makes GetServiceTicket fail for the listed SPNs
	// with the given error instead of returning ticket/sessionKey.
	failSPNs map[string]error
}

func (f *fakeKerberosClient) GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error) {
	if err, ok := f.failSPNs[spn]; ok {
		return messages.Ticket{}, types.EncryptionKey{}, err
	}
	return f.ticket, f.sessionKey, nil
}
func (f *fakeKerberosClient) Realm() string              { return f.realm }
func (f *fakeKerberosClient) CName() types.PrincipalName { return f.cname }
func (f *fakeKerberosClient) Destroy()                   { f.destroyed = true }

// testKeytabAndPrincipals builds a service keytab plus matching client/
// service principal names and realm, shared by the offline handshake tests.
func testKeytabAndPrincipals() (*keytab.Keytab, types.PrincipalName, types.PrincipalName, string) {
	const realm = "EXAMPLE.COM"
	const spn = "kafka/broker.example.com"
	kt := keytab.New()
	_ = kt.AddEntry(spn, realm, "svc-password", time.Now(), 1, etypeID.AES256_CTS_HMAC_SHA1_96)
	sname, _ := types.ParseSPNString(spn)
	cname := types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, "grafana")
	return kt, cname, sname, realm
}

// TestGSSAPIHandshake_Offline exercises the full client side of the GSSAPI
// exchange (Start's AP_REQ plus both steps of Next's security-layer
// negotiation) against a ticket minted offline with messages.NewTicket, with
// no KDC or broker involved. It verifies the AP_REQ for real using
// messages.APReq.Verify (not service.VerifyAPREQ, which consults a
// package-level singleton replay cache that would make a second such test
// in this binary fail), and it locks in the ordering kafka-go's SASL driver
// depends on: a response returned together with done=true would never be
// sent, because kafka-go checks `done` only after already sending the
// state from the previous Next call.
func TestGSSAPIHandshake_Offline(t *testing.T) {
	kt, cname, sname, realm := testKeytabAndPrincipals()
	now := time.Now().UTC()
	tkt, sessionKey, err := messages.NewTicket(
		cname, realm, sname, realm, types.NewKrbFlags(), kt,
		etypeID.AES256_CTS_HMAC_SHA1_96, 1,
		now, now, now.Add(time.Hour), now.Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("failed to mint test ticket: %v", err)
	}

	fake := &fakeKerberosClient{realm: realm, cname: cname, ticket: tkt, sessionKey: sessionKey}
	m := &gssapiMechanism{nowFunc: time.Now}
	m.newClient = func() (kerberosClient, error) { return fake, nil }

	ctx := sasl.WithMetadata(context.Background(), &sasl.Metadata{Host: "broker.example.com", Port: 9093})
	sess, initial, err := m.Start(ctx)
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// --- Verify the AP_REQ framing and content ---
	if len(initial) == 0 || initial[0] != 0x60 {
		t.Fatalf("expected GSS-API APPLICATION-0 framing (0x60 first byte), got first byte 0x%02x", initial[0])
	}

	var mt gssapiKRB5TokenForTest
	if err := mt.unmarshal(initial); err != nil {
		t.Fatalf("failed to unmarshal GSSAPI token framing: %v", err)
	}
	if mt.tokID != [2]byte{0x01, 0x00} {
		t.Fatalf("tokID = %x, want 0100 (KRB_AP_REQ)", mt.tokID)
	}

	var apReq messages.APReq
	if err := apReq.Unmarshal(mt.apReqBytes); err != nil {
		t.Fatalf("failed to unmarshal AP_REQ: %v", err)
	}
	if ok, err := apReq.Verify(kt, 5*time.Minute, types.HostAddress{}, nil); !ok || err != nil {
		t.Fatalf("AP_REQ failed real verification against the service keytab: ok=%v err=%v", ok, err)
	}
	if apReq.Authenticator.Cksum.CksumType != 32771 { // chksumtype.GSSAPI
		t.Errorf("authenticator checksum type = %d, want 32771 (GSSAPI)", apReq.Authenticator.Cksum.CksumType)
	}
	if hex.EncodeToString(apReq.Authenticator.Cksum.Checksum) != "100000000000000000000000000000000000000030000000" {
		t.Errorf("authenticator checksum = %x, want the mutual-auth-clear golden value", apReq.Authenticator.Cksum.Checksum)
	}

	// --- Step 1: acceptor's security-layer negotiation challenge ---
	// Deliberately offer more than "no security layer" (bits for integrity
	// and confidentiality too) with a non-zero buffer size, to prove the
	// response is a fixed "no security layer" selection rather than an
	// echo of whatever the acceptor happened to send.
	acceptorPayload := []byte{0x07, 0x01, 0x02, 0x03}
	challenge := buildAcceptorWrapTokenForTest(t, sessionKey, acceptorPayload, 1)

	done, resp, err := sess.Next(ctx, challenge)
	if err != nil {
		t.Fatalf("Next() (step 1) error: %v", err)
	}
	if done {
		t.Fatal("Next() (step 1) returned done=true; kafka-go would never send its response, " +
			"since it checks done only after already sending the previous state")
	}
	if len(resp) == 0 {
		t.Fatal("Next() (step 1) returned an empty response")
	}

	var respToken gssapi.WrapToken
	if err := respToken.Unmarshal(resp, false); err != nil {
		t.Fatalf("failed to unmarshal initiator WrapToken response: %v", err)
	}
	if ok, err := respToken.Verify(sessionKey, keyusage.GSSAPI_INITIATOR_SEAL); !ok || err != nil {
		t.Fatalf("initiator WrapToken failed verification: ok=%v err=%v", ok, err)
	}
	wantSelection := []byte{0x01, 0x00, 0x00, 0x00} // fixed "no security layer" selection
	if string(respToken.Payload) != string(wantSelection) {
		t.Errorf("response payload = %x, want the fixed no-security-layer selection %x (not an echo of the acceptor's offer %x)",
			respToken.Payload, wantSelection, acceptorPayload)
	}

	// --- Step 2: broker's empty final challenge ---
	done, resp, err = sess.Next(ctx, nil)
	if err != nil {
		t.Fatalf("Next() (step 2) error: %v", err)
	}
	if !done {
		t.Fatal("Next() (step 2) expected done=true")
	}
	if resp != nil {
		t.Errorf("Next() (step 2) expected a nil response, got %x", resp)
	}

	// --- Any further call is a protocol error ---
	if _, _, err := sess.Next(ctx, nil); err == nil {
		t.Error("expected an error on a Next() call beyond the handshake's two steps")
	}
}

func TestBuildWrapTokenResponse(t *testing.T) {
	et, err := crypto.GetEtype(etypeID.AES256_CTS_HMAC_SHA1_96)
	if err != nil {
		t.Fatalf("failed to resolve etype: %v", err)
	}
	key, err := types.GenerateEncryptionKey(et)
	if err != nil {
		t.Fatalf("failed to generate test key: %v", err)
	}

	t.Run("valid offer always yields the fixed no-security-layer selection", func(t *testing.T) {
		for _, offer := range [][]byte{
			{0x01, 0x00, 0x00, 0x00}, // exactly "no security layer", zero size
			{0x01, 0x01, 0x02, 0x03}, // "no security layer" plus a non-zero (and irrelevant) size
			{0x07, 0x00, 0x00, 0x00}, // all three layers offered; client still only wants "no security layer"
		} {
			challenge := buildAcceptorWrapTokenForTest(t, key, offer, 1)
			resp, err := buildWrapTokenResponse(challenge, key)
			if err != nil {
				t.Fatalf("offer %x: unexpected error: %v", offer, err)
			}
			var wt gssapi.WrapToken
			if err := wt.Unmarshal(resp, false); err != nil {
				t.Fatalf("offer %x: failed to unmarshal response: %v", offer, err)
			}
			if string(wt.Payload) != string(noSecurityLayerSelection) {
				t.Errorf("offer %x: response payload = %x, want %x", offer, wt.Payload, noSecurityLayerSelection)
			}
		}
	})

	t.Run("offer without the no-security-layer bit is rejected", func(t *testing.T) {
		challenge := buildAcceptorWrapTokenForTest(t, key, []byte{0x06, 0x00, 0x00, 0x00}, 1) // integrity+confidentiality only
		if _, err := buildWrapTokenResponse(challenge, key); err == nil {
			t.Fatal("expected an error for an offer that does not include \"no security layer\"")
		}
	})

	t.Run("offer with the wrong length is rejected", func(t *testing.T) {
		challenge := buildAcceptorWrapTokenForTest(t, key, []byte{0x01, 0x00, 0x00}, 1) // 3 bytes, not 4
		if _, err := buildWrapTokenResponse(challenge, key); err == nil {
			t.Fatal("expected an error for a non-4-byte offer")
		}
	})
}

func TestGSSAPIStart_MissingBrokerHost(t *testing.T) {
	m := &gssapiMechanism{nowFunc: time.Now}
	m.newClient = func() (kerberosClient, error) { t.Fatal("should not construct a client without a host"); return nil, nil }
	if _, _, err := m.Start(context.Background()); err == nil {
		t.Fatal("expected an error when sasl.Metadata is absent from the context")
	}
}

func TestGssapiMechanism_Close(t *testing.T) {
	fake := &fakeKerberosClient{}
	m := &gssapiMechanism{krb: fake}
	m.Close()
	if !fake.destroyed {
		t.Error("Close() did not call Destroy() on the underlying Kerberos client")
	}
	if m.krb != nil {
		t.Error("Close() did not clear the retained client")
	}
	// Calling Close() again (e.g. Dispose after NewConnection already closed
	// it) must not panic.
	m.Close()
}

// -- helpers below build/verify raw GSSAPI wrap/mech tokens for test
// assertions without depending on gokrb5's unexported spnego.KRB5Token. --

// buildAcceptorWrapTokenForTest builds a GSSAPI WrapToken as Kafka's broker
// (the GSS acceptor) would send it: the acceptor flag set, authenticated
// with the ticket's session key under the acceptor's key usage.
func buildAcceptorWrapTokenForTest(t *testing.T, key types.EncryptionKey, payload []byte, seq uint64) []byte {
	t.Helper()
	et, err := crypto.GetEtype(key.KeyType)
	if err != nil {
		t.Fatalf("failed to resolve etype: %v", err)
	}
	wt := gssapi.WrapToken{
		Flags:     0x01, // acceptor flag set
		EC:        uint16(et.GetHMACBitLength() / 8),
		RRC:       0,
		SndSeqNum: seq,
		Payload:   payload,
	}
	if err := wt.SetCheckSum(key, keyusage.GSSAPI_ACCEPTOR_SEAL); err != nil {
		t.Fatalf("failed to checksum acceptor WrapToken: %v", err)
	}
	b, err := wt.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal acceptor WrapToken: %v", err)
	}
	return b
}

// gssapiKRB5TokenForTest parses just enough of the GSS-API mechanism-
// independent token framing (RFC 2743 §3.1) to recover the mechanism OID
// tag, the KRB5 token ID, and the raw AP_REQ bytes, mirroring what a real
// GSS acceptor (and gokrb5's own unexported spnego.KRB5Token) would parse.
type gssapiKRB5TokenForTest struct {
	tokID      [2]byte
	apReqBytes []byte
}

func (m *gssapiKRB5TokenForTest) unmarshal(b []byte) error {
	// APPLICATION-0 DER wrapper: tag+length header, then OID, then tokID,
	// then the AP_REQ. We only need the tokID and the AP_REQ, so walk the
	// ASN.1 length headers by hand rather than pulling in a full decoder.
	if len(b) < 2 || b[0] != 0x60 {
		return errString("not a GSS-API APPLICATION-0 token")
	}
	i := 1
	i += lenHeaderSizeForTest(b[i:])
	// OID TLV: tag 0x06, then its own length header.
	if b[i] != 0x06 {
		return errString("expected an OID tag")
	}
	i++
	oidLen := int(b[i])
	i += 1 + oidLen
	if i+2 > len(b) {
		return errString("token too short for tokID")
	}
	m.tokID = [2]byte{b[i], b[i+1]}
	i += 2
	m.apReqBytes = b[i:]
	return nil
}

// lenHeaderSizeForTest returns the number of bytes the DER length header at
// the start of b occupies (short or long form), so the caller can skip past
// it without needing the decoded length itself.
func lenHeaderSizeForTest(b []byte) int {
	if b[0] <= 127 {
		return 1
	}
	return 1 + int(b[0]-128)
}

// Compile-time assertions that the production types satisfy the interfaces
// the mechanism depends on.
var (
	_ sasl.Mechanism    = (*gssapiMechanism)(nil)
	_ sasl.StateMachine = (*gssapiSession)(nil)
	_ kerberosClient    = gokrb5Client{}
	_ = config.Config{}
	_ = client.Settings{}
)
