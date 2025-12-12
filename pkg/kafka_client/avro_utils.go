package kafka_client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
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
	defer resp.Body.Close()

	log.DefaultLogger.Debug("Schema registry response received",
		"statusCode", resp.StatusCode,
		"contentLength", resp.ContentLength)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.DefaultLogger.Error("Schema registry returned error",
			"statusCode", resp.StatusCode,
			"responseBody", string(body))
		return "", fmt.Errorf("schema registry returned status %d: %s", resp.StatusCode, string(body))
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
	log.DefaultLogger.Debug("Starting Avro message decoding",
		"dataLength", len(data),
		"schemaLength", len(schema))

	// Log the first few bytes for debugging
	if len(data) > 0 {
		preview := data
		if len(preview) > 20 {
			preview = preview[:20]
		}
		log.DefaultLogger.Debug("Avro data preview",
			"firstBytes", fmt.Sprintf("%x", preview),
			"firstBytesAsString", string(preview))
	}

	var avroData []byte

	// Check if this might be Confluent wire format (starts with magic byte 0x00)
	if len(data) > 5 && data[0] == 0x00 {
		log.DefaultLogger.Debug("Detected potential Confluent wire format, extracting schema ID")
		schemaID := int32(data[1])<<24 | int32(data[2])<<16 | int32(data[3])<<8 | int32(data[4])
		log.DefaultLogger.Debug("Extracted schema ID from wire format", "schemaID", schemaID)
		// Skip the 5-byte header and decode the rest
		avroData = data[5:]
		log.DefaultLogger.Debug("Stripped wire format header, remaining data length", "dataLength", len(avroData))
	} else {
		log.DefaultLogger.Debug("No Confluent wire format detected, treating as plain Avro binary")
		avroData = data
	}

	// Parse the Avro schema
	log.DefaultLogger.Debug("Parsing Avro schema")
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.DefaultLogger.Error("Failed to parse Avro schema", "error", err)
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	// Decode the message
	log.DefaultLogger.Debug("Decoding Avro binary data")
	decoded, remaining, err := codec.NativeFromBinary(avroData)
	if err != nil {
		log.DefaultLogger.Error("Failed to decode Avro message",
			"error", err,
			"dataLength", len(avroData),
			"remainingBytes", len(remaining))
		return nil, fmt.Errorf("failed to decode Avro message: %w", err)
	}

	log.DefaultLogger.Debug("Avro decoding successful",
		"decodedType", fmt.Sprintf("%T", decoded),
		"remainingBytes", len(remaining))

	return decoded, nil
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
