package kafka_client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func truncatePreview(body []byte, max int) string {
	if len(body) <= max {
		return string(body)
	}
	return string(body[:max])
}

// SchemaRegistryClient handles communication with Confluent Schema Registry.
// It accepts one or more registry endpoints and fails over between them on
// connection errors and 5xx/429 responses.
type SchemaRegistryClient struct {
	BaseURLs []string
	Username string
	Password string
	Client   *http.Client

	// preferred is the index of the last endpoint that answered, so steady-state
	// requests go straight to a healthy instance.
	preferred atomic.Int32
}

// ParseSchemaRegistryURLs splits a comma-separated list of Schema Registry URLs,
// trimming whitespace and trailing slashes and dropping empty entries.
func ParseSchemaRegistryURLs(raw string) []string {
	var urls []string
	for _, part := range strings.Split(raw, ",") {
		u := strings.TrimSuffix(strings.TrimSpace(part), "/")
		if u != "" {
			urls = append(urls, u)
		}
	}
	return urls
}

// RedactSchemaRegistryURLs returns the comma-separated URL list with any
// userinfo credentials stripped, for safe logging.
func RedactSchemaRegistryURLs(raw string) string {
	urls := ParseSchemaRegistryURLs(raw)
	for i, u := range urls {
		urls[i] = redactURL(u)
	}
	return strings.Join(urls, ",")
}

func redactURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return "<invalid url>"
	}
	if parsed.User != nil {
		parsed.User = nil
	}
	return parsed.String()
}

// NewSchemaRegistryClient creates a new Schema Registry client.
// baseURL may contain several comma-separated endpoints.
// The Schema Registry HTTP client should be created using grafana-plugin-sdk-go/backend/httpclient
// to support Private Data Source Connect (PDC) with automatic SOCKS proxy handling
func NewSchemaRegistryClient(baseURL, username, password string, httpClient *http.Client) *SchemaRegistryClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &SchemaRegistryClient{
		BaseURLs: ParseSchemaRegistryURLs(baseURL),
		Username: username,
		Password: password,
		Client:   httpClient,
	}
}

// shouldFailover reports whether a response status means the endpoint is
// unhealthy (rather than the request being wrong), so another endpoint may help.
func shouldFailover(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests
}

// doGet issues a GET for path against the configured endpoints, starting at the
// last healthy one. It returns the first response that is not a failover
// candidate; the caller owns closing its body.
func (s *SchemaRegistryClient) doGet(path string) (*http.Response, error) {
	n := len(s.BaseURLs)
	if n == 0 {
		return nil, fmt.Errorf("no schema registry URL configured")
	}

	start := int(s.preferred.Load())
	if start >= n {
		start = 0
	}

	var failures []string
	var lastErr error
	var lastResp *http.Response
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		base := s.BaseURLs[idx]

		req, err := http.NewRequest("GET", base+path, nil)
		if err != nil {
			// The parse error echoes the raw URL, including any userinfo, so
			// report only the redacted form.
			lastErr = fmt.Errorf("invalid schema registry URL %s", redactURL(base))
			failures = append(failures, lastErr.Error())
			continue
		}
		if s.Username != "" && s.Password != "" {
			req.SetBasicAuth(s.Username, s.Password)
		}

		resp, err := s.Client.Do(req)
		if err != nil {
			// Stop on cancellation only. http.Client.Timeout surfaces as
			// context.DeadlineExceeded, and a hung instance must still fail over.
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			log.DefaultLogger.Debug("Schema registry endpoint unreachable, trying next",
				"endpoint", redactURL(base), "error", err)
			lastErr = err
			failures = append(failures, fmt.Sprintf("%s: %v", redactURL(base), err))
			continue
		}

		if shouldFailover(resp.StatusCode) && i < n-1 {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			log.DefaultLogger.Debug("Schema registry endpoint unhealthy, trying next",
				"endpoint", redactURL(base), "statusCode", resp.StatusCode,
				"responsePreview", truncatePreview(body, 200))
			failures = append(failures, fmt.Sprintf("%s: status %d", redactURL(base), resp.StatusCode))
			continue
		}

		if !shouldFailover(resp.StatusCode) {
			s.preferred.Store(int32(idx)) // #nosec G115 -- idx is bounded by the number of configured URLs
		}
		lastResp = resp
		break
	}

	if lastResp != nil {
		return lastResp, nil
	}
	if n == 1 {
		// Preserve the single-endpoint error shape.
		return nil, lastErr
	}
	return nil, fmt.Errorf("all %d schema registry endpoints failed: %s", n, strings.Join(failures, "; "))
}

// GetSchemaByID retrieves a schema by its ID
func (s *SchemaRegistryClient) GetSchemaByID(schemaID int) (string, error) {
	resp, err := s.doGet(fmt.Sprintf("/schemas/ids/%d", schemaID))
	if err != nil {
		return "", fmt.Errorf("failed to get schema: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.DefaultLogger.Debug("Schema registry returned error for schema ID",
			"schemaID", schemaID,
			"statusCode", resp.StatusCode,
			"responseBodyLength", len(body),
			"responsePreview", truncatePreview(body, 200))
		return "", fmt.Errorf("schema registry returned status %d for schema ID %d (body: %d bytes)", resp.StatusCode, schemaID, len(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode schema response: %w", err)
	}

	return result.Schema, nil
}

// GetLatestSchema retrieves the latest schema for a subject
func (s *SchemaRegistryClient) GetLatestSchema(subject string) (string, error) {
	log.DefaultLogger.Debug("Requesting latest schema from registry",
		"subject", subject,
		"endpoints", len(s.BaseURLs),
		"hasAuth", s.Username != "")

	resp, err := s.doGet(fmt.Sprintf("/subjects/%s/versions/latest", subject))
	if err != nil {
		log.DefaultLogger.Error("HTTP request to schema registry failed", "error", err)
		return "", fmt.Errorf("failed to get latest schema: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.DefaultLogger.Error("failed to close response body", "error", err)
		}
	}()

	log.DefaultLogger.Debug("Schema registry response received",
		"statusCode", resp.StatusCode,
		"contentLength", resp.ContentLength)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.DefaultLogger.Debug("Schema registry returned error",
			"subject", subject,
			"statusCode", resp.StatusCode,
			"responseBodyLength", len(body),
			"responsePreview", truncatePreview(body, 200))
		return "", fmt.Errorf("schema registry returned status %d for subject %q (body: %d bytes)", resp.StatusCode, subject, len(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.DefaultLogger.Error("Failed to decode schema registry response", "error", err)
		return "", fmt.Errorf("failed to decode schema response: %w", err)
	}

	log.DefaultLogger.Debug("Successfully retrieved schema from registry",
		"subject", subject,
		"schemaLength", len(result.Schema))

	return result.Schema, nil
}

// DecodeAvroMessage decodes an Avro message using the provided schema
func DecodeAvroMessage(data []byte, schema string) (interface{}, error) {
	return defaultMessageDecoder.DecodeAvroMessage(data, schema)
}

// GetSubjectName generates a subject name based on the strategy
// For TopicNameStrategy (default), returns "{topic}-value" to match Confluent Schema Registry convention
func GetSubjectName(topic, strategy string) string {
	var subject string
	switch strategy {
	case "recordName":
		subject = topic + "-value"
	case "topicRecordName":
		subject = topic + "-" + topic + "-value"
	default: // "topicName" - matches Confluent Schema Registry TopicNameStrategy
		subject = topic + "-value"
	}

	log.DefaultLogger.Debug("Generated subject name",
		"topic", topic,
		"strategy", strategy,
		"subject", subject)

	return subject
}
