package kafka_client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
)

// SchemaRegistryClient handles communication with Confluent Schema Registry
type SchemaRegistryClient struct {
	BaseURL  string
	Username string
	Password string
	Client   *http.Client
}

// NewSchemaRegistryClient creates a new Schema Registry client
func NewSchemaRegistryClient(baseURL, username, password string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		BaseURL:  strings.TrimSuffix(baseURL, "/"),
		Username: username,
		Password: password,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
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

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	if s.Username != "" && s.Password != "" {
		req.SetBasicAuth(s.Username, s.Password)
	}

	resp, err := s.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get latest schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode schema response: %w", err)
	}

	return result.Schema, nil
}

// DecodeAvroMessage decodes an Avro message using the provided schema
func DecodeAvroMessage(data []byte, schema string) (interface{}, error) {
	// Parse the Avro schema
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	// Decode the message
	decoded, _, err := codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro message: %w", err)
	}

	return decoded, nil
}

// GetSubjectName generates a subject name based on the strategy
func GetSubjectName(topic, strategy string) string {
	switch strategy {
	case "recordName":
		return topic + "-value"
	case "topicRecordName":
		return topic + "-" + topic + "-value"
	default: // "topicName"
		return topic
	}
}
