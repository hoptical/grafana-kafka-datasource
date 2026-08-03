package kafka_client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func truncatePreview(body []byte, max int) string {
	if len(body) <= max {
		return string(body)
	}
	return string(body[:max])
}

// SchemaRegistryClient handles communication with Confluent Schema Registry
type SchemaRegistryClient struct {
	BaseURL  string
	Username string
	Password string
	Client   *http.Client
}

// NewSchemaRegistryClient creates a new Schema Registry client
// The httpClient should be created using grafana-plugin-sdk-go/backend/httpclient
// to support Private Data Source Connect (PDC) with automatic SOCKS proxy handling
func NewSchemaRegistryClient(baseURL, username, password string, httpClient *http.Client) *SchemaRegistryClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &SchemaRegistryClient{
		BaseURL:  strings.TrimSuffix(baseURL, "/"),
		Username: username,
		Password: password,
		Client:   httpClient,
	}
}

// GetSchemaByID retrieves a schema by its ID
func (s *SchemaRegistryClient) GetSchemaByID(schemaID int) (string, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", s.BaseURL, schemaID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	if s.Username != "" && s.Password != "" {
		req.SetBasicAuth(s.Username, s.Password)
	}

	resp, err := s.Client.Do(req)
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
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", s.BaseURL, subject)

	log.DefaultLogger.Debug("Requesting latest schema from registry",
		"url", url,
		"subject", subject,
		"hasAuth", s.Username != "")

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.DefaultLogger.Error("Failed to create schema registry request", "error", err)
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	if s.Username != "" && s.Password != "" {
		req.SetBasicAuth(s.Username, s.Password)
		log.DefaultLogger.Debug("Using basic auth for schema registry request")
	}

	log.DefaultLogger.Debug("Making HTTP request to schema registry")
	resp, err := s.Client.Do(req)
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
