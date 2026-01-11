package kafka_client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	grafanalog "github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/segmentio/kafka-go"
)

const debugLogLevel = "debug"
const errorLogLevel = "error"
const dialerTimeout = 10 * time.Second // Fallback dialer timeout when user timeout not set
const defaultTimeoutMs = 2000          // Fallback general timeout (health check) in ms if user timeout not provided
// note: lastN offset calculation is approximate using kafka.LastOffset - N and clamped to kafka.FirstOffset

// Options holds datasource configuration passed from Grafana.
// Timeout (ms) is the single user-facing timeout controlling:
//   - Dialer & client request timeout
//   - Health check deadline
//
// If Timeout <= 0, sensible defaults are used.
type Options struct {
	BootstrapServers string `json:"bootstrapServers"`
	ClientId         string `json:"clientId"`
	SecurityProtocol string `json:"securityProtocol"`
	SaslMechanisms   string `json:"saslMechanisms"`
	SaslUsername     string `json:"saslUsername"`
	SaslPassword     string `json:"saslPassword"`
	LogLevel         string `json:"logLevel"`
	// TLS Configuration
	TLSAuthWithCACert bool   `json:"tlsAuthWithCACert"`
	TLSAuth           bool   `json:"tlsAuth"`
	TLSSkipVerify     bool   `json:"tlsSkipVerify"`
	ServerName        string `json:"serverName"`
	TLSCACert         string `json:"tlsCACert"`
	TLSClientCert     string `json:"tlsClientCert"`
	TLSClientKey      string `json:"tlsClientKey"`
	// Advanced settings
	Timeout            int32 `json:"timeout"`            // ms; primary timeout
	HealthcheckTimeout int32 `json:"healthcheckTimeout"` // ms; health check specific timeout
	// Avro Configuration
	MessageFormat          string `json:"messageFormat"`
	SchemaRegistryUrl      string `json:"schemaRegistryUrl"`
	SchemaRegistryUsername string `json:"schemaRegistryUsername"`
	SchemaRegistryPassword string `json:"schemaRegistryPassword"`
	FlattenMaxDepth        int    `json:"flattenMaxDepth"`
	FlattenFieldCap        int    `json:"flattenFieldCap"`
}

type KafkaClient struct {
	Dialer           *kafka.Dialer
	Reader           *kafka.Reader
	Conn             *kafka.Client
	BootstrapServers string
	Brokers          []string
	ClientId         string
	TimestampMode    string
	SecurityProtocol string
	SaslMechanisms   string
	SaslUsername     string
	SaslPassword     string
	LogLevel         string
	// TLS Configuration
	TLSAuthWithCACert bool
	TLSAuth           bool
	TLSSkipVerify     bool
	ServerName        string
	TLSCACert         string
	TLSClientCert     string
	TLSClientKey      string
	// Advanced settings
	Timeout            int32 // effective timeout (ms)
	HealthcheckTimeout int32 // health check specific timeout (ms)
	// Avro Configuration
	MessageFormat          string
	SchemaRegistryUrl      string
	SchemaRegistryUsername string
	SchemaRegistryPassword string
	HTTPClient             *http.Client // HTTP client for Schema Registry (from grafana-plugin-sdk-go)
}

// GetHTTPClient returns the HTTP client used for Schema Registry connections
func (client *KafkaClient) GetHTTPClient() *http.Client {
	return client.HTTPClient
}

type KafkaMessage struct {
	Value     interface{} `json:"value,omitempty"` // Can be map[string]interface{} or []interface{}
	RawValue  []byte      `json:"-"`               // Raw message bytes for Avro decoding - not serialized
	Timestamp time.Time   `json:"timestamp"`
	Offset    int64       `json:"offset"`
	Error     error       `json:"-"` // Error if decoding failed - not serialized
}

// ErrTopicNotFound indicates the requested topic does not exist.
var ErrTopicNotFound = errors.New("topic not found")

// decodeMessageValue decodes the message value based on the specified format.
// For "avro", it returns nil (decoding deferred).
// For "json", it attempts JSON decoding and returns an error if it fails.
// For other formats, it attempts JSON decoding but doesn't return an error on failure.
func (client *KafkaClient) decodeMessageValue(data []byte, format string) (interface{}, error) {
	switch format {
	case "avro":
		grafanalog.DefaultLogger.Debug("Skipping JSON decoding for Avro format message",
			"valueLength", len(data))
		// For Avro format, don't attempt JSON parsing
		return nil, nil
	case "json":
		// For JSON format, require successful JSON decoding
		var doc interface{}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		if err := dec.Decode(&doc); err != nil {
			previewLen := 10
			if len(data) < previewLen {
				previewLen = len(data)
			}
			grafanalog.DefaultLogger.Error("JSON decoding failed for JSON format message",
				"error", err,
				"valueLength", len(data),
				"errorType", fmt.Sprintf("%T", err),
				"firstBytes", fmt.Sprintf("%x", data[:previewLen]))
			return nil, fmt.Errorf("failed to decode message as JSON: %w", err)
		} else {
			// Accept both objects and arrays at the top level
			switch v := doc.(type) {
			case map[string]interface{}, []interface{}:
				grafanalog.DefaultLogger.Debug("JSON decoding successful",
					"decodedType", fmt.Sprintf("%T", v))
				return v, nil
			default:
				grafanalog.DefaultLogger.Error("JSON decoded but not object/array for JSON format",
					"decodedType", fmt.Sprintf("%T", v))
				return nil, fmt.Errorf("decoded JSON is not a valid object or array: %T", v)
			}
		}
	default:
		// For other formats or unspecified, try JSON for backward compatibility but don't fail
		var doc interface{}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		if err := dec.Decode(&doc); err != nil {
			previewLen := 10
			if len(data) < previewLen {
				previewLen = len(data)
			}
			grafanalog.DefaultLogger.Debug("JSON decoding failed, message may be binary",
				"error", err,
				"valueLength", len(data),
				"errorType", fmt.Sprintf("%T", err),
				"firstBytes", fmt.Sprintf("%x", data[:previewLen]))
			// If JSON decoding fails, we'll handle it later
			return nil, nil
		} else {
			// Accept both objects and arrays at the top level
			switch v := doc.(type) {
			case map[string]interface{}, []interface{}:
				grafanalog.DefaultLogger.Debug("JSON decoding successful",
					"decodedType", fmt.Sprintf("%T", v))
				return v, nil
			default:
				grafanalog.DefaultLogger.Debug("JSON decoded but not object/array",
					"decodedType", fmt.Sprintf("%T", v))
				// If it's not a valid JSON object/array, leave as nil
				return nil, nil
			}
		}
	}
}

// NewKafkaClient creates a new KafkaClient instance.
// The httpClient parameter should be created using grafana-plugin-sdk-go/backend/httpclient
// to support Private Data Source Connect (PDC) with automatic SOCKS proxy handling.
func NewKafkaClient(options Options, httpClient *http.Client) KafkaClient {
	// Build broker slice once
	raw := strings.Split(options.BootstrapServers, ",")
	brokers := make([]string, 0, len(raw))
	for _, b := range raw {
		bt := strings.TrimSpace(b)
		if bt != "" {
			brokers = append(brokers, bt)
		}
	}

	// Sanitize timeout; store 0 if unset to differentiate
	effectiveTimeoutMs := options.Timeout
	if effectiveTimeoutMs < 0 {
		effectiveTimeoutMs = 0
	}

	// Sanitize health check timeout
	effectiveHealthcheckMs := options.HealthcheckTimeout
	if effectiveHealthcheckMs < 0 {
		effectiveHealthcheckMs = 0
	}

	return KafkaClient{
		BootstrapServers:   options.BootstrapServers,
		Brokers:            brokers,
		ClientId:           options.ClientId,
		SecurityProtocol:   options.SecurityProtocol,
		SaslMechanisms:     options.SaslMechanisms,
		SaslUsername:       options.SaslUsername,
		SaslPassword:       options.SaslPassword,
		LogLevel:           options.LogLevel,
		TLSAuthWithCACert:  options.TLSAuthWithCACert,
		TLSAuth:            options.TLSAuth,
		TLSSkipVerify:      options.TLSSkipVerify,
		ServerName:         options.ServerName,
		TLSCACert:          options.TLSCACert,
		TLSClientCert:      options.TLSClientCert,
		TLSClientKey:       options.TLSClientKey,
		Timeout:            effectiveTimeoutMs,
		HealthcheckTimeout: effectiveHealthcheckMs,
		// Avro Configuration
		MessageFormat:          options.MessageFormat,
		SchemaRegistryUrl:      options.SchemaRegistryUrl,
		SchemaRegistryUsername: options.SchemaRegistryUsername,
		SchemaRegistryPassword: options.SchemaRegistryPassword,
		HTTPClient:             httpClient,
	}
}

func (client *KafkaClient) NewConnection() error {
	var mechanism sasl.Mechanism
	var err error

	// Check if SASL is enabled based on security protocol
	isSASL := client.SecurityProtocol == "SASL_PLAINTEXT" || client.SecurityProtocol == "SASL_SSL"
	if isSASL {
		// Validate SASL credentials are provided
		if client.SaslUsername == "" || client.SaslPassword == "" {
			return fmt.Errorf("SASL authentication requires both username and password")
		}
		mechanism, err = getSASLMechanism(client)
		if err != nil {
			return fmt.Errorf("unable to get SASL mechanism: %w", err)
		}
	}

	// Determine dialer timeout
	effectiveTimeout := dialerTimeout
	if client.Timeout > 0 {
		effectiveTimeout = time.Duration(client.Timeout) * time.Millisecond
	}

	dialer := &kafka.Dialer{
		Timeout:       effectiveTimeout,
		SASLMechanism: mechanism,
		ClientID:      client.ClientId,
	}

	transport := &kafka.Transport{
		SASL:     mechanism,
		ClientID: client.ClientId,
	}

	if client.SecurityProtocol == "SASL_SSL" || client.SecurityProtocol == "SSL" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: client.TLSSkipVerify}
		if client.ServerName != "" {
			tlsConfig.ServerName = client.ServerName
		}
		if client.TLSAuthWithCACert && client.TLSCACert != "" {
			roots := x509.NewCertPool()
			if ok := roots.AppendCertsFromPEM([]byte(client.TLSCACert)); !ok {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = roots
		}
		if client.TLSAuth && client.TLSClientCert != "" && client.TLSClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(client.TLSClientCert), []byte(client.TLSClientKey))
			if err != nil {
				return fmt.Errorf("failed to parse client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		dialer.TLS = tlsConfig
		transport.TLS = tlsConfig
	}

	client.Dialer = dialer
	client.Conn = &kafka.Client{Addr: kafka.TCP(client.Brokers...), Timeout: effectiveTimeout, Transport: transport}
	return nil
}

func (client *KafkaClient) newReader(topic string, partition int) *kafka.Reader {
	logger, errorLogger := getKafkaLogger(client.LogLevel)
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        client.Brokers,
		Topic:          topic,
		Partition:      partition,
		Dialer:         client.Dialer,
		CommitInterval: 0,
		Logger:         logger,
		ErrorLogger:    errorLogger,
	})
}

// getTopicMetadata fetches metadata for a single topic.
// metadataForTopic returns metadata for a specific topic.
func (client *KafkaClient) metadataForTopic(ctx context.Context, topicName string) (*kafka.Topic, error) {
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topicName}})
	if err != nil {
		return nil, fmt.Errorf("unable to get metadata: %w", err)
	}
	if len(meta.Topics) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topicName)
	}
	t := meta.Topics[0]
	if t.Error != nil {
		// Classify not-found errors deterministically if possible
		if isTopicNotFound(t.Error) {
			return nil, fmt.Errorf("%w: %s", ErrTopicNotFound, topicName)
		}
		return nil, fmt.Errorf("error getting topic %s metadata: %w", topicName, t.Error)
	}
	return &t, nil
}

func (client *KafkaClient) NewStreamReader(ctx context.Context, topic string, partition int32, autoOffsetReset string, lastN int32) (*kafka.Reader, error) {
	if client.Dialer == nil {
		if err := client.NewConnection(); err != nil {
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}
	}
	reader := client.newReader(topic, int(partition))
	// Determine starting offset based on mode
	switch autoOffsetReset {
	case "earliest":
		if err := reader.SetOffset(kafka.FirstOffset); err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to set earliest offset: %w", err)
		}
	case "lastN":
		// Query broker for latest and earliest offsets for this partition using a leader connection
		n := int64(lastN)
		if n <= 0 {
			n = 100
		}
		grafanalog.DefaultLogger.Debug("Setting up lastN reader",
			"topic", topic,
			"partition", partition,
			"requestedLastN", lastN,
			"effectiveLastN", n)
		if len(client.Brokers) == 0 {
			_ = reader.Close()
			return nil, fmt.Errorf("no brokers configured to read offsets")
		}
		// Bound leader dial by client.Timeout to avoid hanging indefinitely
		offCtx := ctx
		if client.Timeout > 0 {
			d := time.Duration(client.Timeout) * time.Millisecond
			var cancel context.CancelFunc
			offCtx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		leaderConn, err := client.Dialer.DialLeader(offCtx, "tcp", client.Brokers[0], topic, int(partition))
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to dial leader: %w", err)
		}
		defer func() {
			if err := leaderConn.Close(); err != nil {
				grafanalog.DefaultLogger.Error("failed to close leader connection", "error", err)
			}
		}()
		earliest, err := leaderConn.ReadFirstOffset()
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to read first offset: %w", err)
		}
		latest, err := leaderConn.ReadLastOffset()
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to read last offset: %w", err)
		}
		start := latest - n
		if start < earliest {
			start = earliest
		}
		grafanalog.DefaultLogger.Debug("LastN offset calculation",
			"topic", topic,
			"partition", partition,
			"earliest", earliest,
			"latest", latest,
			"requestedLastN", n,
			"calculatedStart", start)
		if err := reader.SetOffset(start); err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to set lastN start offset: %w", err)
		}
	default: // latest
		if err := reader.SetOffset(kafka.LastOffset); err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("unable to set latest offset: %w", err)
		}
	}
	return reader, nil
}

func (client *KafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader, messageFormat string) (KafkaMessage, error) {
	var message KafkaMessage
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return message, fmt.Errorf("error reading message from Kafka: %w", err)
	}

	grafanalog.DefaultLogger.Debug("Received Kafka message",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"valueLength", len(msg.Value),
		"timestamp", msg.Time,
		"messageFormat", messageFormat)

	// Store raw bytes for potential Avro decoding
	message.RawValue = msg.Value

	// Log first few bytes for debugging
	if len(msg.Value) > 0 {
		preview := msg.Value
		if len(preview) > 20 {
			preview = preview[:20]
		}
		grafanalog.DefaultLogger.Debug("Message content preview",
			"firstBytes", fmt.Sprintf("%x", preview),
			"firstChars", string(preview))
	}

	// Decode message value based on format
	value, err := client.decodeMessageValue(msg.Value, messageFormat)
	if err != nil {
		message.Error = err
		message.Value = nil
	} else {
		message.Value = value
	}

	message.Offset = msg.Offset
	message.Timestamp = msg.Time

	grafanalog.DefaultLogger.Debug("Message processing complete",
		"hasParsedValue", message.Value != nil,
		"rawValueLength", len(message.RawValue))

	return message, nil
}

func (client *KafkaClient) HealthCheck() error {
	if err := client.NewConnection(); err != nil {
		return fmt.Errorf("unable to initialize Kafka client: %w", err)
	}
	brokers := len(client.Brokers)

	// Determine timeout preference: HealthcheckTimeout > Timeout > Default
	timeoutMs := client.HealthcheckTimeout
	if timeoutMs <= 0 {
		timeoutMs = client.Timeout
	}
	if timeoutMs <= 0 {
		timeoutMs = defaultTimeoutMs
	}

	grafanalog.DefaultLogger.Debug("Attempting health check connection", "brokers", brokers, "timeoutMs", timeoutMs)

	deadline := time.After(time.Duration(timeoutMs) * time.Millisecond)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-deadline:
			if lastErr != nil {
				return fmt.Errorf("health check timed out after %d ms. Last error: %w", timeoutMs, lastErr)
			}
			return fmt.Errorf("health check timed out after %d ms", timeoutMs)
		case <-ticker.C:
			_, err := client.Conn.Metadata(context.Background(), &kafka.MetadataRequest{})
			if err == nil {
				return nil
			}
			lastErr = err
			grafanalog.DefaultLogger.Debug("Health check failed attempt", "error", err)
		}
	}
}

func (client *KafkaClient) Dispose() {
	if client.Reader != nil {
		if err := client.Reader.Close(); err != nil {
			grafanalog.DefaultLogger.Error("failed to close reader", "error", err)
		}
	}
}

func getSASLMechanism(client *KafkaClient) (sasl.Mechanism, error) {
	// Default to PLAIN if empty but usage is implied (this function is usually called when SASL is enabled)
	mechanism := client.SaslMechanisms
	if mechanism == "" {
		mechanism = "PLAIN"
	}

	grafanalog.DefaultLogger.Debug("Configuring SASL", "mechanism", mechanism)

	switch mechanism {
	case "PLAIN":
		return plain.Mechanism{Username: client.SaslUsername, Password: client.SaslPassword}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, client.SaslUsername, client.SaslPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, client.SaslUsername, client.SaslPassword)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

func (client *KafkaClient) GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error) {
	topic, err := client.metadataForTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}
	ids := make([]int32, len(topic.Partitions))
	for i, p := range topic.Partitions {
		ids[i] = int32(p.ID)
	}
	return ids, nil
}

func (client *KafkaClient) GetTopics(ctx context.Context, prefix string, limit int) ([]string, error) {
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("unable to get metadata: %w", err)
	}
	var matching []string
	for _, topic := range meta.Topics {
		if topic.Error != nil {
			continue
		}
		if prefix == "" || strings.HasPrefix(topic.Name, prefix) {
			matching = append(matching, topic.Name)
			if limit > 0 && len(matching) >= limit {
				break
			}
		}
	}
	return matching, nil
}

// isTopicNotFound attempts to classify broker errors that indicate a missing topic.
func isTopicNotFound(err error) bool {
	if err == nil {
		return false
	}
	// Prefer kafka-go sentinel if available
	if errors.Is(err, kafka.UnknownTopicOrPartition) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown topic") ||
		strings.Contains(s, "not found") ||
		strings.Contains(s, "does not exist")
}

func getKafkaLogger(level string) (kafka.LoggerFunc, kafka.LoggerFunc) {
	noop := kafka.LoggerFunc(func(msg string, args ...interface{}) {})
	logger := noop
	errorLogger := noop
	switch strings.ToLower(level) {
	case debugLogLevel:
		logger = func(msg string, args ...interface{}) {
			grafanalog.DefaultLogger.Debug(fmt.Sprintf("[KAFKA DEBUG] "+msg, args...))
		}
		errorLogger = func(msg string, args ...interface{}) {
			grafanalog.DefaultLogger.Error(fmt.Sprintf("[KAFKA ERROR] "+msg, args...))
		}
	case errorLogLevel:
		errorLogger = func(msg string, args ...interface{}) {
			grafanalog.DefaultLogger.Error(fmt.Sprintf("[KAFKA ERROR] "+msg, args...))
		}
	}
	return logger, errorLogger
}

// GetMessageFormat returns the message format setting
func (client *KafkaClient) GetMessageFormat() string {
	return client.MessageFormat
}

// GetSchemaRegistryUrl returns the Schema Registry URL
func (client *KafkaClient) GetSchemaRegistryUrl() string {
	return client.SchemaRegistryUrl
}

// GetSchemaRegistryUsername returns the Schema Registry username
func (client *KafkaClient) GetSchemaRegistryUsername() string {
	return client.SchemaRegistryUsername
}

// GetSchemaRegistryPassword returns the Schema Registry password
func (client *KafkaClient) GetSchemaRegistryPassword() string {
	return client.SchemaRegistryPassword
}

// GetAvroSubjectNamingStrategy returns the Avro subject naming strategy
func (client *KafkaClient) GetAvroSubjectNamingStrategy() string {
	return "recordName" // Uses record name from schema as subject name
}
