# SASL/OAUTHBEARER (OAuth 2.0) Authentication

## Overview

This datasource supports SASL/OAUTHBEARER (KIP-255), Kafka's standard mechanism for
authenticating with a short-lived bearer token instead of a static username/password.
It uses the OAuth 2.0 `client_credentials` grant: the plugin sends a `client_id` and
`client_secret` (plus an optional `scope`) to your identity provider's token endpoint,
receives a bearer token, and authenticates to Kafka with it. The token is cached and
proactively refreshed before it expires, so no user interaction is needed after initial
setup.

This is generic OIDC client-credentials support and works with Keycloak, Okta, Azure AD,
Ping, and any other OIDC-compliant identity provider that implements the
`client_credentials` grant using the `client_secret_post` client authentication method
(credentials sent in the POST body). Providers that require `client_secret_basic` (HTTP
Basic auth) or a client assertion (e.g. private_key_jwt) are not currently supported.

`segmentio/kafka-go` (the Kafka client library this plugin uses) does not implement
OAUTHBEARER itself — only PLAIN and SCRAM-SHA-256/512 — so this mechanism is implemented
directly in `pkg/kafka_client/oauth_mechanism.go` against kafka-go's `sasl.Mechanism`
interface.

## Configuration

### UI

In the datasource's **Authentication** section:

1. Set **Security Protocol** to `SASL_PLAINTEXT` or `SASL_SSL` (SASL_SSL is strongly
   recommended, since credentials and tokens should not be sent over plaintext).
2. Set **SASL Mechanism** to `OAUTHBEARER`.
3. Fill in:
   - **Token Endpoint**: your identity provider's OAuth 2.0 token URL. Must be `https://`
     (plaintext `http://` is only permitted for `localhost`/loopback addresses, to support
     local testing).
   - **Client ID**: the OAuth client identifier (not secret).
   - **Client Secret**: the OAuth client secret (stored encrypted).
   - **Scope** (optional): a space-delimited scope string requested from the token
     endpoint, if your provider requires one.

Selecting `OAUTHBEARER` replaces the SASL Username/Password fields used by PLAIN/SCRAM
with these OAuth fields; only one credential set applies at a time.

### API (jsonData / secureJsonData)

```json
{
  "jsonData": {
    "securityProtocol": "SASL_SSL",
    "saslMechanisms": "OAUTHBEARER",
    "saslOauthTokenEndpoint": "https://idp.example.com/oauth2/token",
    "saslOauthClientId": "my-client-id",
    "saslOauthScope": "kafka"
  },
  "secureJsonData": {
    "saslOauthClientSecret": "my-client-secret"
  }
}
```

`saslOauthClientSecret` is the only OAuth field treated as a secret; it is stored the
same way as `saslPassword` and other encrypted datasource fields.

## Behavior

### Token fetch and caching

- On the first connection, and whenever the cached token has expired, the plugin POSTs
  `grant_type=client_credentials` (plus `client_id`, `client_secret`, and `scope` if set)
  to the token endpoint as `application/x-www-form-urlencoded`, and expects a JSON
  response containing `access_token` and `expires_in` (seconds).
- The token is cached and reused across connections until shortly before it expires —
  refreshed at `expires_in` minus a skew of `min(30s, 20% of expires_in)` (at least 1
  second), so a typical 600s token refreshes around the 570s mark.
- Token fetches are safe for concurrent use: multiple partition-reader connections
  sharing a mechanism instance will not issue redundant token requests while a valid
  cached token exists.
- There is no internal retry on a failed token fetch; retries happen naturally through
  the existing health check polling and the Kafka reader's own reconnect behavior.

### Token endpoint security

- The token endpoint must be `https://`; plaintext `http://` is rejected unless the host
  is `localhost` or a loopback address (this exception exists solely so local/dev token
  servers work — real deployments should always use HTTPS).
- The token-fetch HTTP client does not follow redirects, so a token endpoint cannot
  forward the client secret to a different host or scheme via a 3xx response.
- The token endpoint's response body is capped at 1MB; larger responses are rejected
  before being parsed.

### Error surfacing

Token-fetch failures are wrapped as `failed to fetch OAuth token: <cause>` (for example,
an unreachable token endpoint, a non-200 response, or a malformed/missing
`access_token`/`expires_in` in the response), so they are distinguishable from generic
Kafka connection errors in the **Save & Test** / health check result.

## Limitations

- **No PDC tunneling for the token endpoint.** If your OAuth token endpoint is only
  reachable through the same private network as your Kafka brokers (i.e. it would need
  to go through Grafana's Private Data Source Connect tunnel), that is not supported yet
  — the token-fetch HTTP client dials directly, independent of the PDC dialer used for
  broker connections.
- **No live end-to-end broker test in this repo's local dev/e2e stack.** The bundled
  Docker Compose Kafka broker does not have an OAUTHBEARER listener configured (Kafka's
  default OAUTHBEARER callback handler only validates unsecured, self-signed JWTs, not a
  real OIDC exchange), so Playwright e2e coverage is limited to verifying the
  configuration UI. The token-fetch, caching, refresh, and wire-protocol logic is
  covered by Go unit tests instead (see below).

## Testing

Unit tests (no live broker required):

- `pkg/kafka_client/oauth_mechanism_test.go`: wire-format (`Start()`'s GS2 initial
  response), cache-hit behavior, refresh-after-expiry, concurrent-fetch de-duplication,
  token endpoint error handling, HTTPS/loopback enforcement, redirect rejection, and the
  response size cap — using `httptest.NewServer` to fake the OIDC token endpoint.
- `pkg/kafka_client/client_test.go`: `getSASLMechanism` OAUTHBEARER selection, connection
  validation (missing token endpoint/client id/secret), and composition with TLS and the
  Grafana PDC dial function.

Run tests:

```bash
go test ./pkg/kafka_client/... -run OAuth -v
go test ./pkg/kafka_client/... -run TestNewConnection_OAuthBearer -v
```

Frontend unit tests (`src/__tests__/ConfigEditor.test.tsx`) cover conditional rendering
of the OAuth fields and the client secret change/reset flow.

Playwright e2e (`tests/configEditor.spec.ts`, "should allow configuring datasource with
SASL_SSL and OAUTHBEARER") verifies the fields render, accept input, and persist across a
save — it does not exercise a live handshake, per the limitation above.

## Related Files

- `pkg/kafka_client/oauth_mechanism.go` - OAUTHBEARER mechanism implementation
- `pkg/kafka_client/oauth_mechanism_test.go` - Unit tests
- `pkg/kafka_client/client.go` - `Options`/`KafkaClient` fields, `getSASLMechanism`, `NewConnection`
- `pkg/plugin/plugin.go` - `getDatasourceSettings` secure field wiring
- `src/types.ts` - Frontend types
- `src/ConfigEditor.tsx` - UI components
- `tests/configEditor.spec.ts` - Playwright e2e smoke test
