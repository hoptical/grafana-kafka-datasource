package kafka_client

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/segmentio/kafka-go/sasl"
)

// gssapiMechanism implements SASL/GSSAPI (RFC 4752) Kerberos authentication
// using kafka-go's sasl.Mechanism interface. kafka-go v0.4.47 does not ship
// this mechanism (only PLAIN and SCRAM-SHA-256/512 exist), so it is
// implemented here directly on top of github.com/jcmturner/gokrb5/v8, the
// same way OAUTHBEARER is implemented in oauth_mechanism.go.
//
// # Wire exchange
//
// kafka-go's SASL driver (dialer.go/transport.go) runs a fixed loop: send
// the current state, receive a challenge, call Next(challenge) to get the
// next state and a "done" flag, repeat until done. GSSAPI's handshake, once
// mutual authentication is not requested, needs exactly two round trips
// after the initial response:
//
//  1. Start returns the AP_REQ (wrapped in the GSS-API mechanism-independent
//     token framing) as the initial response.
//  2. The broker's GSS acceptor verifies the AP_REQ and, having established
//     a security context, sends a security-layer negotiation WrapToken
//     (4-byte payload: supported layers + max buffer size) as the first
//     challenge. Next validates the offer and replies, in an initiator
//     WrapToken authenticated with the ticket's session key, with a fixed
//     selection of "no security layer" (the only one Kafka implements),
//     and returns done=false.
//  3. The broker sends an empty final challenge; Next returns done=true.
//
// Mutual authentication (an AP_REP from the broker before step 2) is
// deliberately never requested — it would add a round trip this state
// machine does not model. IMPORTANT: because kafka-go's loop sends the
// response returned from Next *before* checking the done flag, a response
// returned together with done=true would never reach the broker. Next must
// therefore return done=false on every step that carries a response, and
// done=true only once there is nothing left to send (see gssapi_session.Next
// and its test for this exact ordering).
//
// # Client lifecycle
//
// A single *client.Client (gokrb5's Kerberos client) is created lazily on
// first use and reused for the lifetime of the mechanism: gokrb5 caches the
// TGT and service tickets, and automatically renews the TGT in the
// background (or re-authenticates from the stored password/keytab once it
// can no longer be renewed), so there is no need for a user-configurable
// renewal interval. Close (called from KafkaClient.NewConnection when
// replacing the mechanism, and from Dispose) destroys the client and stops
// its renewal goroutine; it must never be called between connections that
// will keep using the mechanism, since gokrb5's Destroy permanently wipes
// the stored credentials.
type gssapiMechanism struct {
	serviceName     string
	krb5Config      *config.Config
	username        string
	realm           string
	authType        string
	password        string
	keytab          *keytab.Keytab
	disablePAFXFAST bool

	newClient func() (kerberosClient, error) // seam: production uses newGokrb5Client
	nowFunc   func() time.Time               // seam: negative-cache TTL in tests

	mu  sync.Mutex
	krb kerberosClient

	// loginErr caches a Kerberos client-construction (AS-exchange) failure.
	// This is scoped to the whole mechanism, not any one broker, because
	// login depends only on the configured principal/credentials - it is
	// the same regardless of which broker's SPN is being requested.
	loginErr    error
	loginErrAt  time.Time
	loginErrTTL time.Duration

	// ticketFailures caches GetServiceTicket failures per SPN (one entry
	// per broker). Unlike login, a service-ticket failure can be specific
	// to a single broker's principal (e.g. one broker's SPN doesn't match
	// any keytab entry), so it must not be applied to every other broker's
	// otherwise-healthy ticket requests.
	ticketFailures map[string]gssapiCachedFailure
}

// gssapiCachedFailure is one entry in gssapiMechanism.ticketFailures.
type gssapiCachedFailure struct {
	err error
	at  time.Time
	ttl time.Duration
}

const defaultKerberosServiceName = "kafka"
const gssapiAuthTypePassword = "password"
const gssapiAuthTypeKeytab = "keytab"

// Negative-cache TTLs for GetServiceTicket failures. HealthCheck retries on
// a 200ms ticker for up to HealthcheckTimeout (2s by default) — without this
// cache, a single bad password would trigger roughly ten Kerberos
// pre-authentication failures in two seconds, which is enough to lock an
// Active Directory account (default lockout threshold is 5). Credential
// errors get a long, sticky TTL because they cannot resolve themselves
// before the user fixes the configuration (which re-instantiates the
// datasource and its mechanism); transient errors (e.g. an unreachable KDC)
// get a short TTL so a genuinely temporary condition is retried promptly.
const gssapiCredentialFailureTTL = 30 * time.Second
const gssapiTransientFailureTTL = 1 * time.Second

// kerberosClient is the subset of gokrb5's *client.Client that gssapiMechanism
// depends on. It exists purely so unit tests can drive the SASL exchange
// against a ticket minted offline (see gssapi_mechanism_test.go) without a
// live KDC; production always uses gokrb5Client. This mirrors the
// httpClient/nowFunc injection in oauth_mechanism.go.
type kerberosClient interface {
	GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error)
	Realm() string
	CName() types.PrincipalName
	Destroy()
}

// gokrb5Client adapts *client.Client to kerberosClient. Realm and CName are
// thin wrappers because Credentials is a named field on client.Client, not
// an embedded one.
type gokrb5Client struct{ *client.Client }

func (c gokrb5Client) Realm() string              { return c.Credentials.Domain() }
func (c gokrb5Client) CName() types.PrincipalName { return c.Credentials.CName() }

// newGSSAPIMechanism builds a gssapiMechanism from the datasource's
// configured GSSAPI settings. It parses krb5.conf and (for keytab auth) the
// keytab synchronously, so configuration errors surface immediately from
// NewConnection rather than on the first dial.
func newGSSAPIMechanism(c *KafkaClient) (*gssapiMechanism, error) {
	krb5Config, err := parseKrb5Config(c.SaslGssapiKrb5Config, c.SaslGssapiKrb5ConfigPath)
	if err != nil {
		return nil, err
	}

	realm := c.SaslGssapiRealm
	if !realmConfigured(krb5Config, realm) {
		return nil, fmt.Errorf("kerberos realm %q is not defined in the [realms] section of krb5.conf", realm)
	}

	username := c.SaslGssapiUsername
	if strings.Contains(username, "@") {
		return nil, fmt.Errorf("GSSAPI principal must not include the realm; remove the \"@...\" suffix from %q", username)
	}

	serviceName := c.SaslGssapiServiceName
	if serviceName == "" {
		serviceName = defaultKerberosServiceName
	}

	switch c.SaslGssapiAuthType {
	case "", gssapiAuthTypePassword, gssapiAuthTypeKeytab:
		// valid
	default:
		return nil, fmt.Errorf("unsupported GSSAPI authentication type: %s", c.SaslGssapiAuthType)
	}

	m := &gssapiMechanism{
		serviceName:     serviceName,
		krb5Config:      krb5Config,
		username:        username,
		realm:           realm,
		authType:        c.SaslGssapiAuthType,
		password:        c.SaslGssapiPassword,
		disablePAFXFAST: c.SaslGssapiDisablePAFXFAST,
		nowFunc:         time.Now,
	}

	if m.authType == gssapiAuthTypeKeytab {
		kt, ktErr := parseKeytab(c.SaslGssapiKeytab, c.SaslGssapiKeytabPath)
		if ktErr != nil {
			return nil, ktErr
		}
		m.keytab = kt
	}

	m.newClient = m.newGokrb5Client
	return m, nil
}

// realmConfigured reports whether realm has a [realms] entry in cfg. gokrb5
// compares realm names byte-for-byte and never canonicalizes case, so a
// realm typed in a different case than krb5.conf would otherwise fail much
// later with an opaque "no defined KDCs" error; checking here gives a
// precise, actionable error instead.
func realmConfigured(cfg *config.Config, realm string) bool {
	for _, r := range cfg.Realms {
		if r.Realm == realm {
			return true
		}
	}
	return false
}

// parseKrb5Config parses krb5.conf from inline content or a file path.
// Inline content takes precedence when both are set. gokrb5 parses both
// forms entirely in memory (config.NewFromString / config.Load); no temp
// file is ever written.
func parseKrb5Config(inline, path string) (*config.Config, error) {
	switch {
	case inline != "":
		cfg, err := config.NewFromString(inline)
		if err != nil {
			return nil, fmt.Errorf("invalid Kerberos configuration: %w", err)
		}
		return cfg, nil
	case path != "":
		cfg, err := config.Load(path)
		if err != nil {
			return nil, fmt.Errorf("invalid Kerberos configuration: %w", err)
		}
		return cfg, nil
	default:
		return nil, fmt.Errorf("GSSAPI authentication requires krb5.conf content or a krb5.conf file path")
	}
}

// parseKeytab parses a keytab from base64-encoded inline content or a file
// path. Inline content takes precedence when both are set. gokrb5 has no
// keytab.Parse; the inline form is decoded and handed to keytab.Unmarshal.
func parseKeytab(base64Content, path string) (*keytab.Keytab, error) {
	switch {
	case base64Content != "":
		raw, err := base64.StdEncoding.DecodeString(base64Content)
		if err != nil {
			return nil, fmt.Errorf("invalid Kerberos keytab: content is not valid base64: %w", err)
		}
		kt := keytab.New()
		if err := kt.Unmarshal(raw); err != nil {
			return nil, fmt.Errorf("invalid Kerberos keytab: %w", err)
		}
		return kt, nil
	case path != "":
		kt, err := keytab.Load(path)
		if err != nil {
			return nil, fmt.Errorf("invalid Kerberos keytab: %w", err)
		}
		return kt, nil
	default:
		return nil, fmt.Errorf("GSSAPI keytab authentication requires keytab content or a keytab file path")
	}
}

func (m *gssapiMechanism) Name() string {
	return "GSSAPI"
}

// Close releases the underlying Kerberos client and stops its background
// TGT-renewal goroutine. It is safe to call on a mechanism that has not
// dialed yet. See the package (type) doc comment for why this must not be
// called between connections that will keep using the mechanism.
func (m *gssapiMechanism) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.krb != nil {
		m.krb.Destroy()
		m.krb = nil
	}
}

func (m *gssapiMechanism) newGokrb5Client() (kerberosClient, error) {
	opts := []func(*client.Settings){client.DisablePAFXFAST(m.disablePAFXFAST)}
	var cl *client.Client
	if m.authType == gssapiAuthTypeKeytab {
		cl = client.NewWithKeytab(m.username, m.realm, m.keytab, m.krb5Config, opts...)
	} else {
		cl = client.NewWithPassword(m.username, m.realm, m.password, m.krb5Config, opts...)
	}
	return gokrb5Client{cl}, nil
}

// serviceTicket returns a service ticket for spn, lazily creating the
// Kerberos client on first use. Login and ticket requests are serialized
// under m.mu — dial volume here is a handful of broker connections per
// datasource, and serializing avoids concurrent cold AS-exchanges hammering
// the KDC and keeps the failure caches coherent.
func (m *gssapiMechanism) serviceTicket(spn string) (messages.Ticket, types.EncryptionKey, kerberosClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.loginErr != nil && m.nowFunc().Before(m.loginErrAt.Add(m.loginErrTTL)) {
		return messages.Ticket{}, types.EncryptionKey{}, nil, m.loginErr
	}
	if failure, ok := m.ticketFailures[spn]; ok && m.nowFunc().Before(failure.at.Add(failure.ttl)) {
		return messages.Ticket{}, types.EncryptionKey{}, nil, failure.err
	}

	if m.krb == nil {
		krb, err := m.newClient()
		if err != nil {
			wrapped := fmt.Errorf("kerberos login failed for %s@%s: %w", m.username, m.realm, err)
			m.recordLoginFailure(wrapped)
			return messages.Ticket{}, types.EncryptionKey{}, nil, wrapped
		}
		m.krb = krb
	}

	tkt, key, err := m.krb.GetServiceTicket(spn)
	if err != nil {
		wrapped := fmt.Errorf("kerberos service ticket request failed for SPN %s: %w", spn, err)
		m.recordTicketFailure(spn, wrapped)
		return messages.Ticket{}, types.EncryptionKey{}, nil, wrapped
	}

	delete(m.ticketFailures, spn)
	m.loginErr = nil
	return tkt, key, m.krb, nil
}

func (m *gssapiMechanism) recordLoginFailure(err error) {
	m.loginErr = err
	m.loginErrAt = m.nowFunc()
	m.loginErrTTL = failureTTL(err)
}

func (m *gssapiMechanism) recordTicketFailure(spn string, err error) {
	if m.ticketFailures == nil {
		m.ticketFailures = make(map[string]gssapiCachedFailure)
	}
	m.ticketFailures[spn] = gssapiCachedFailure{err: err, at: m.nowFunc(), ttl: failureTTL(err)}
}

// failureTTL picks the negative-cache TTL for a Kerberos failure: a long,
// sticky TTL for credential errors (see the constants' doc comment above),
// and a short one for anything else (e.g. a transiently unreachable KDC).
func failureTTL(err error) time.Duration {
	if isCredentialError(err) {
		return gssapiCredentialFailureTTL
	}
	return gssapiTransientFailureTTL
}

// isCredentialError reports whether a Kerberos error indicates the
// configured credentials are wrong (bad password, unknown or revoked
// principal) as opposed to a transient condition (KDC unreachable, clock
// skew). gokrb5 wraps errors with krberror.Errorf, which flattens the cause
// into a plain string with no Unwrap, so classification has to match on the
// KDC error-code names it embeds (see
// github.com/jcmturner/gokrb5/v8/iana/errorcode) rather than errors.As.
func isCredentialError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, code := range []string{
		"KDC_ERR_PREAUTH_FAILED",
		"KDC_ERR_C_PRINCIPAL_UNKNOWN",
		"KDC_ERR_S_PRINCIPAL_UNKNOWN",
		"KDC_ERR_CLIENT_REVOKED",
	} {
		if strings.Contains(s, code) {
			return true
		}
	}
	return false
}

// Start begins the GSSAPI handshake for a new connection: it resolves the
// broker host kafka-go is dialing (via sasl.MetadataFromContext), builds the
// Kafka service principal name for it, obtains a service ticket, and
// returns the GSS-API-framed AP_REQ as the initial response. See the
// package (type) doc comment for the full wire sequence.
func (m *gssapiMechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	md := sasl.MetadataFromContext(ctx)
	if md == nil || md.Host == "" {
		return nil, nil, fmt.Errorf("GSSAPI authentication requires a broker host, but none was provided")
	}
	spn := spnForHost(m.serviceName, md.Host)

	tkt, key, krb, err := m.serviceTicket(spn)
	if err != nil {
		return nil, nil, err
	}

	apReq, err := buildAPReqToken(krb.Realm(), krb.CName(), tkt, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build Kerberos AP_REQ: %w", err)
	}
	framed, err := appendGSSAPIHeader(apReq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build Kerberos AP_REQ: %w", err)
	}

	return &gssapiSession{sessionKey: key}, framed, nil
}

// spnForHost builds the Kafka service principal name for a broker host, in
// the "<serviceName>/<host>" form Kafka's GSSAPI acceptor expects. The host
// is lowercased and any trailing dot stripped, since krb5.conf's
// [domain_realm] mapping is itself lowercase-keyed and gokrb5 does no host
// canonicalization of its own. Note that this must be the broker's
// *advertised* hostname (advertised.listeners), not necessarily what was
// typed into Bootstrap Servers — see docs/KERBEROS_AUTH.md.
func spnForHost(serviceName, host string) string {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	return serviceName + "/" + h
}

// tokIDKRBAPReq is the GSSAPI KRB5 mech-token ID for a KRB_AP_REQ token
// (RFC 4121 / gokrb5's spnego.TOK_ID_KRB_AP_REQ), big-endian 0x0100.
var tokIDKRBAPReq = [2]byte{0x01, 0x00}

// gssapiAuthenticatorChecksum builds the RFC 4121 §4.1.1 GSSAPI checksum
// carried in the AP_REQ's Authenticator: a 4-byte little-endian "bnd length"
// (always 16), 16 zero "channel bindings" bytes (channel binding is not
// used), and a 4-byte little-endian context-flags word.
//
// The flags request only integrity and confidentiality — the pair Kafka's
// GSS acceptor needs to negotiate the security-layer handshake carried out
// in gssapiSession.Next. Mutual authentication is deliberately not
// requested: setting that flag would make the acceptor emit an AP_REP that
// this two-round-trip state machine does not handle.
func gssapiAuthenticatorChecksum() []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint32(b[0:4], 16)
	flags := uint32(gssapi.ContextFlagInteg | gssapi.ContextFlagConf)
	binary.LittleEndian.PutUint32(b[20:24], flags)
	return b
}

// buildAPReqToken builds a raw (not yet GSS-API-framed) Kerberos AP_REQ
// message authenticating cname against the service ticket tkt, encrypted
// with the ticket's session key.
func buildAPReqToken(realm string, cname types.PrincipalName, tkt messages.Ticket, key types.EncryptionKey) ([]byte, error) {
	auth, err := types.NewAuthenticator(realm, cname)
	if err != nil {
		return nil, fmt.Errorf("failed to build Kerberos authenticator: %w", err)
	}
	auth.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  gssapiAuthenticatorChecksum(),
	}

	apReq, err := messages.NewAPReq(tkt, key, auth)
	if err != nil {
		return nil, fmt.Errorf("failed to build AP_REQ: %w", err)
	}

	b, err := apReq.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal AP_REQ: %w", err)
	}
	return b, nil
}

// appendGSSAPIHeader wraps a raw Kerberos AP_REQ in the GSS-API
// mechanism-independent token framing required by RFC 2743 §3.1: an
// APPLICATION-0 DER wrapper around the krb5 mechanism OID, the
// mechanism-specific KRB_AP_REQ token ID, and the AP_REQ bytes. Every GSS
// acceptor, Kafka's included, parses this wrapper before looking at the
// AP_REQ itself; an unwrapped AP_REQ is rejected.
func appendGSSAPIHeader(apReqBytes []byte) ([]byte, error) {
	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GSSAPI mechanism OID: %w", err)
	}
	b := make([]byte, 0, len(oidBytes)+len(tokIDKRBAPReq)+len(apReqBytes))
	b = append(b, oidBytes...)
	b = append(b, tokIDKRBAPReq[:]...)
	b = append(b, apReqBytes...)
	return asn1tools.AddASNAppTag(b, 0), nil
}

// noSecurityLayerBit is bit 0 of the security-layer offer/selection byte in
// RFC 4752 §3.1's 4-byte negotiation payload: "no security layer" (Kafka's
// "auth" QOP). This plugin never selects integrity (bit 1) or
// confidentiality (bit 2), since Kafka's SASL/GSSAPI acceptor only ever
// implements "no security layer".
const noSecurityLayerBit = 0x01

// noSecurityLayerSelection is the fixed response payload selecting "no
// security layer" with a client max-receive-size of 0. Per the JDK GSSAPI
// SASL client (which is what this response format matches, since Kafka's
// acceptor is built on it), bytes 2-4 of a "no security layer" selection
// are always zero — they are not a size the client should propose, unlike
// for the integrity/confidentiality layers this plugin doesn't use.
var noSecurityLayerSelection = []byte{noSecurityLayerBit, 0x00, 0x00, 0x00}

// buildWrapTokenResponse answers the acceptor's security-layer negotiation
// WrapToken (challenge), per RFC 4752 §3.1: Kafka's GSS acceptor advertises
// its supported security layers and its own max receive buffer size as a
// 4-byte payload once the security context is established, and expects the
// initiator to select exactly one of the offered layers.
//
// This plugin only ever needs "no security layer" (the only one Kafka
// implements), so the response is always the fixed noSecurityLayerSelection
// - not an echo of the acceptor's payload. Echoing would be wrong even
// though it happens to produce the same bytes against a default Kafka
// broker (whose offer already is exactly noSecurityLayerSelection): bytes
// 2-4 of the *offer* are the acceptor's own buffer size for the higher
// layers, not something to reflect back as the initiator's own selection,
// and a multi-bit offer echoed back would be an invalid multi-layer
// "selection" instead of a single one.
func buildWrapTokenResponse(challenge []byte, key types.EncryptionKey) ([]byte, error) {
	var wt gssapi.WrapToken
	if err := wt.Unmarshal(challenge, true); err != nil {
		return nil, fmt.Errorf("failed to parse security layer negotiation token: %w", err)
	}
	if _, err := wt.Verify(key, keyusage.GSSAPI_ACCEPTOR_SEAL); err != nil {
		return nil, fmt.Errorf("failed to verify security layer negotiation token: %w", err)
	}
	if len(wt.Payload) != 4 {
		return nil, fmt.Errorf("security layer negotiation offer has unexpected length %d (want 4)", len(wt.Payload))
	}
	if wt.Payload[0]&noSecurityLayerBit == 0 {
		return nil, fmt.Errorf("broker did not offer the \"no security layer\" option this plugin requires (offer byte: 0x%02x)", wt.Payload[0])
	}

	resp, err := gssapi.NewInitiatorWrapToken(noSecurityLayerSelection, key)
	if err != nil {
		return nil, fmt.Errorf("failed to build security layer negotiation response: %w", err)
	}
	b, err := resp.Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal security layer negotiation response: %w", err)
	}
	return b, nil
}

// gssapiSession implements sasl.StateMachine for a single connection. Unlike
// oauth_mechanism.go's mechanism (which is stateless per exchange and so
// returns itself from Start), GSSAPI carries a session key and a step
// counter that are specific to one connection's handshake, so Start returns
// a fresh gssapiSession each time. kafka-go's contract guarantees a
// StateMachine is only ever used by the connection that created it, so no
// mutex is needed here.
type gssapiSession struct {
	sessionKey types.EncryptionKey
	step       int
}

// Next drives the two-message security-layer negotiation that follows a
// successful AP_REQ. Per kafka-go's SASL driver loop (dialer.go /
// transport.go), the response returned here is sent to the broker *before*
// `done` is checked, so `done` must stay false on every step that returns a
// response — a done=true returned alongside a response would have that
// response silently dropped and the exchange would fail on the connection's
// first real request instead of during authentication.
func (s *gssapiSession) Next(ctx context.Context, challenge []byte) (bool, []byte, error) {
	switch s.step {
	case 0:
		resp, err := buildWrapTokenResponse(challenge, s.sessionKey)
		if err != nil {
			return false, nil, fmt.Errorf("GSSAPI security layer negotiation failed: %w", err)
		}
		s.step++
		return false, resp, nil
	case 1:
		s.step++
		return true, nil, nil
	default:
		return false, nil, fmt.Errorf("GSSAPI authentication protocol error: unexpected additional challenge")
	}
}
