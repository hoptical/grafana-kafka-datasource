# SASL/GSSAPI (Kerberos) Authentication

## Overview

This datasource supports SASL/GSSAPI, Kafka's Kerberos authentication mechanism, for
clusters where PLAIN, SCRAM, and OAUTHBEARER are not available (or not permitted) and
Kerberos is required instead.

The plugin authenticates as a Kerberos principal — either with a password or a keytab —
obtains a service ticket for the Kafka broker, and completes the GSSAPI handshake over
Kafka's standard `SaslAuthenticate` protocol. Ticket renewal is handled automatically: the
underlying Kerberos client renews the ticket-granting ticket (TGT) in the background, and
re-authenticates from the stored password/keytab if a realm does not support renewal, so
there is no renewal interval to configure.

`segmentio/kafka-go` (the Kafka client library this plugin uses) does not implement GSSAPI
itself — only PLAIN and SCRAM-SHA-256/512 — so this mechanism is implemented directly in
`pkg/kafka_client/gssapi_mechanism.go` against kafka-go's `sasl.Mechanism` interface, on top
of [`github.com/jcmturner/gokrb5`](https://github.com/jcmturner/gokrb5), a pure-Go Kerberos
client library.

## Configuration

### UI

In the datasource's **Authentication** section:

1. Set **Security Protocol** to `SASL_PLAINTEXT` or `SASL_SSL` (SASL_SSL is strongly
   recommended, since the GSSAPI handshake itself is not encrypted by SASL_PLAINTEXT).
2. Set **SASL Mechanism** to `GSSAPI`.
3. Fill in:
   - **Service Name**: the Kerberos service name Kafka registers as, defaulting to `kafka`.
     Combined with each broker's hostname, this forms the service principal name (SPN,
     `<service name>/<broker host>`) the plugin requests a ticket for.
   - **Realm**: the Kerberos realm, exactly as it appears (case-sensitive) in the
     `[realms]` section of krb5.conf.
   - **Principal**: the Kerberos username, without an `@REALM` suffix.
   - **krb5.conf Content**: paste the contents of krb5.conf. krb5.conf contains no key
     material, so it is stored as plain (not encrypted) configuration.
   - **Authentication Method**: `Password` or `Keytab`.
     - **Password**: the principal's Kerberos password (stored encrypted).
     - **Keytab**: paste the keytab's base64-encoded content (stored encrypted). Encode it
       with `base64 -w0 grafana.keytab` on Linux or `base64 -i grafana.keytab` on macOS.
   - **Disable PA-FX-FAST** (advanced): disables the `PA_REQ_ENC_PA_REP` pre-authentication
     data some Active Directory environments and older MIT KDCs reject. Leave unchecked
     unless authentication fails with a pre-authentication error.

Selecting `GSSAPI` replaces the SASL Username/Password fields used by PLAIN/SCRAM, and the
OAuth fields used by OAUTHBEARER, with these Kerberos fields; only one credential set
applies at a time.

### API (jsonData / secureJsonData)

```json
{
  "jsonData": {
    "securityProtocol": "SASL_SSL",
    "saslMechanisms": "GSSAPI",
    "saslGssapiServiceName": "kafka",
    "saslGssapiRealm": "EXAMPLE.COM",
    "saslGssapiUsername": "grafana",
    "saslGssapiAuthType": "password",
    "saslGssapiKrb5Config": "[libdefaults]\ndefault_realm = EXAMPLE.COM\n[realms]\nEXAMPLE.COM = {\n  kdc = kdc.example.com\n}\n"
  },
  "secureJsonData": {
    "saslGssapiPassword": "my-kerberos-password"
  }
}
```

For keytab authentication, set `saslGssapiAuthType` to `"keytab"` and provide
`secureJsonData.saslGssapiKeytab` (base64-encoded keytab content) instead of a password.
`saslGssapiDisablePAFXFAST` is an optional boolean.

krb5.conf and the keytab can only be supplied as content, not as file paths: the plugin never
reads files from the Grafana server's filesystem.

`saslGssapiPassword` and `saslGssapiKeytab` are the only GSSAPI fields treated as secrets;
they are stored the same way as `saslPassword` and other encrypted datasource fields.

## Behavior

### Handshake

On each new broker connection, the plugin obtains (or reuses a cached) service ticket for
`<service name>/<broker host>`, builds a Kerberos AP_REQ authenticator requesting integrity
and confidentiality (but not mutual authentication), and exchanges it with the broker over
Kafka's standard `SaslAuthenticate` protocol, followed by a short security-layer negotiation
in which the plugin always selects "no security layer" (the only one Kafka implements).

### Ticket lifecycle

- The underlying Kerberos client is created on first use and reused for the lifetime of the
  connection: the ticket-granting ticket (TGT) and service tickets are cached and reused
  across every broker connection rather than re-authenticated per connection.
- The TGT is renewed automatically in the background before it expires; if a realm does not
  support ticket renewal (or the renewal window has passed), the client transparently
  re-authenticates from the stored password or keytab instead. There is no user-configurable
  renewal interval to set.
- Repeated authentication failures (e.g. a wrong password) are cached for 30 seconds before
  being retried, specifically so that Grafana's health-check retry loop cannot trigger
  enough failed Kerberos pre-authentication attempts in a few seconds to lock an Active
  Directory account (AD's default lockout threshold is 5 attempts).

### Error surfacing

Configuration, login, and service-ticket errors are wrapped so each stage is
distinguishable in the **Save & Test** / health check result: `invalid Kerberos
configuration: ...`, `invalid Kerberos keytab: ...`, `Kerberos login failed for
<principal>@<realm>: ...`, and `Kerberos service ticket request failed for SPN <spn>: ...`
(the SPN is almost always the most useful detail when GSSAPI authentication fails against a
real cluster — see Limitations below).

## Limitations

- **The service principal name (SPN) uses the broker's _advertised_ hostname, not
  necessarily what was typed into Bootstrap Servers.** Kafka's GSSAPI acceptor expects the
  SPN to match the hostname the broker advertises in `advertised.listeners`. If that differs
  from the address used to connect (a short name vs. an FQDN, or an IP address), the
  handshake fails with an unknown-principal error naming an SPN that does not match any
  keytab entry. Ensure `advertised.listeners` uses the same hostname form as the broker's
  Kerberos service principal.
- **KDC traffic does not go through Grafana's Private Data Source Connect (PDC) / Secure
  Socks Proxy.** Only Kafka broker connections are routed through the configured PDC
  dialer; the Kerberos client dials the KDC directly. GSSAPI therefore requires the Grafana
  backend to reach the KDC directly, even when PDC is enabled for the Kafka connection
  itself.
- **The health check timeout may not be long enough for a slow or unreachable KDC.** KDC
  network calls are not bound by the configured Healthcheck Timeout; a slow or unreachable
  KDC can make **Save & Test** take noticeably longer than the configured timeout before
  failing. If this happens consistently, verify KDC reachability from the Grafana backend
  host before increasing the timeout.
- **No live end-to-end broker test in this repo's local dev/e2e stack.** The bundled Docker
  Compose Kafka broker has no KDC or GSSAPI listener configured, so Playwright e2e coverage
  is limited to verifying the configuration UI. The handshake, config/keytab parsing, and
  ticket-lifecycle logic are covered by Go unit tests instead (see below).

## Testing

Unit tests (no live KDC required):

- `pkg/kafka_client/gssapi_mechanism_test.go`: a full offline handshake test that mints a
  Kerberos ticket in-memory and drives the mechanism's `Start`/`Next` state machine against
  it exactly as kafka-go would, verifying the AP_REQ against the ticket's own keytab and
  the security-layer negotiation response; plus krb5.conf/keytab parsing (including empty and
  malformed input), realm and principal validation, SPN construction, the authentication-failure
  cache, and concurrent-connection behavior.
- `pkg/kafka_client/client_test.go`: `getSASLMechanism`/`NewConnection` GSSAPI selection and
  required-field validation, dial-function/TLS preservation, and that a previous GSSAPI
  mechanism's Kerberos client is released when the connection is re-established.

Run tests:

```bash
go test ./pkg/kafka_client/... -run GSSAPI -v
go test ./pkg/kafka_client/... -run Gssapi -v
```

Frontend unit tests (`src/__tests__/ConfigEditor.test.tsx`) cover conditional rendering of
the GSSAPI fields (and that no file-path inputs are rendered), the
password/keytab authentication-method toggle, and the password/keytab secret change and
reset flows.

Playwright e2e (`tests/configEditor.spec.ts`, "should allow configuring datasource with
SASL_SSL and GSSAPI") verifies the fields render, accept input, and persist across a save —
it does not exercise a live handshake, per the limitation above.

## Related Files

- `pkg/kafka_client/gssapi_mechanism.go` - GSSAPI mechanism implementation
- `pkg/kafka_client/gssapi_mechanism_test.go` - Unit tests, including the offline handshake test
- `pkg/kafka_client/client.go` - `Options`/`KafkaClient` fields, `getSASLMechanism`, `NewConnection`
- `pkg/plugin/plugin.go` - `getDatasourceSettings` secure field wiring
- `src/types.ts` - Frontend types
- `src/GssapiFields.tsx` - GSSAPI configuration UI fields
- `src/ConfigEditor.tsx` - Config editor mechanism selection
- `tests/configEditor.spec.ts` - Playwright e2e smoke test
