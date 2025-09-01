package kafka_client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

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
	Timeout int32 `json:"timeout"` // ms; primary timeout
	// Avro Configuration
	MessageFormat          string `json:"messageFormat"`
	SchemaRegistryUrl      string `json:"schemaRegistryUrl"`
	SchemaRegistryUsername string `json:"schemaRegistryUsername"`
	SchemaRegistryPassword string `json:"schemaRegistryPassword"`
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
	Timeout int32 // effective timeout (ms)
	// Avro Configuration
	MessageFormat          string
	SchemaRegistryUrl      string
	SchemaRegistryUsername string
	SchemaRegistryPassword string
}

type KafkaMessage struct {
	Value     interface{} // Can be map[string]interface{} or []interface{}
	RawValue  []byte      // Raw message bytes for Avro decoding
	Timestamp time.Time
	Offset    int64
}

// ErrTopicNotFound indicates the requested topic does not exist.
var ErrTopicNotFound = errors.New("topic not found")

func NewKafkaClient(options Options) KafkaClient {
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

	return KafkaClient{
		BootstrapServers:  options.BootstrapServers,
		Brokers:           brokers,
		ClientId:          options.ClientId,
		SecurityProtocol:  options.SecurityProtocol,
		SaslMechanisms:    options.SaslMechanisms,
		SaslUsername:      options.SaslUsername,
		SaslPassword:      options.SaslPassword,
		LogLevel:          options.LogLevel,
		TLSAuthWithCACert: options.TLSAuthWithCACert,
		TLSAuth:           options.TLSAuth,
		TLSSkipVerify:     options.TLSSkipVerify,
		ServerName:        options.ServerName,
		TLSCACert:         options.TLSCACert,
		TLSClientCert:     options.TLSClientCert,
		TLSClientKey:      options.TLSClientKey,
		Timeout:           effectiveTimeoutMs,
		// Avro Configuration
		MessageFormat:          options.MessageFormat,
		SchemaRegistryUrl:      options.SchemaRegistryUrl,
		SchemaRegistryUsername: options.SchemaRegistryUsername,
		SchemaRegistryPassword: options.SchemaRegistryPassword,
	}
}

func (client *KafkaClient) NewConnection() error {
	var mechanism sasl.Mechanism
	var err error

	if client.SaslMechanisms != "" {
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
			reader.Close()
			return nil, fmt.Errorf("unable to set earliest offset: %w", err)
		}
	case "lastN":
		// Query broker for latest and earliest offsets for this partition using a leader connection
		n := int64(lastN)
		if n <= 0 {
			n = 100
		}
		if len(client.Brokers) == 0 {
			reader.Close()
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
			reader.Close()
			return nil, fmt.Errorf("unable to dial leader: %w", err)
		}
		defer leaderConn.Close()
		earliest, err := leaderConn.ReadFirstOffset()
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("unable to read first offset: %w", err)
		}
		latest, err := leaderConn.ReadLastOffset()
		if err != nil {
			reader.Close()
			return nil, fmt.Errorf("unable to read last offset: %w", err)
		}
		start := latest - n
		if start < earliest {
			start = earliest
		}
		if err := reader.SetOffset(start); err != nil {
			reader.Close()
			return nil, fmt.Errorf("unable to set lastN start offset: %w", err)
		}
	default: // latest
		if err := reader.SetOffset(kafka.LastOffset); err != nil {
			reader.Close()
			return nil, fmt.Errorf("unable to set latest offset: %w", err)
		}
	}
	return reader, nil
}

func (client *KafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader) (KafkaMessage, error) {
	var message KafkaMessage
	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return message, fmt.Errorf("error reading message from Kafka: %w", err)
	}

	// Store raw bytes for potential Avro decoding
	message.RawValue = msg.Value

	// Try to decode as JSON first (for backward compatibility)
	var doc interface{}
	dec := json.NewDecoder(bytes.NewReader(msg.Value))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		// If JSON decoding fails, we'll handle it later (could be Avro)
		message.Value = nil
	} else {
		// Accept both objects and arrays at the top level
		switch v := doc.(type) {
		case map[string]interface{}, []interface{}:
			message.Value = v
		default:
			// If it's not a valid JSON object/array, it might be Avro
			message.Value = nil
		}
	}

	message.Offset = msg.Offset
	message.Timestamp = msg.Time
	return message, nil
}

func (client *KafkaClient) HealthCheck() error {
	if err := client.NewConnection(); err != nil {
		return fmt.Errorf("unable to initialize Kafka client: %w", err)
	}
	brokers := len(client.Brokers)
	log.Printf("[KAFKA DEBUG] Attempting health check connection to %d broker(s)", brokers)

	deadlineMs := client.Timeout
	if deadlineMs <= 0 {
		deadlineMs = defaultTimeoutMs
	}
	deadline := time.After(time.Duration(deadlineMs) * time.Millisecond)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("health check timed out after %d ms", deadlineMs)
		case <-ticker.C:
			_, err := client.Conn.Metadata(context.Background(), &kafka.MetadataRequest{})
			if err == nil {
				return nil
			}
		}
	}
}

func (client *KafkaClient) Dispose() {
	if client.Reader != nil {
		client.Reader.Close()
	}
}

func getSASLMechanism(client *KafkaClient) (sasl.Mechanism, error) {
	switch client.SaslMechanisms {
	case "PLAIN":
		return plain.Mechanism{Username: client.SaslUsername, Password: client.SaslPassword}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, client.SaslUsername, client.SaslPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, client.SaslUsername, client.SaslPassword)
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported mechanism SASL: %s", client.SaslMechanisms)
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
		logger = func(msg string, args ...interface{}) { log.Printf("[KAFKA DEBUG] "+msg, args...) }
		errorLogger = func(msg string, args ...interface{}) { log.Printf("[KAFKA ERROR] "+msg, args...) }
	case errorLogLevel:
		errorLogger = func(msg string, args ...interface{}) { log.Printf("[KAFKA ERROR] "+msg, args...) }
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
	return "topicName" // Default strategy
}
