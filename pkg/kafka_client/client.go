package kafka_client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/segmentio/kafka-go"
)

const maxEarliest int64 = 100
const debugLogLevel = "debug"
const errorLogLevel = "error"
const dialerTimeout = 10 * time.Second
const defaultHealthcheckTimeout = 2000 // 2 seconds in milliseconds

type Options struct {
	BootstrapServers   string `json:"bootstrapServers"`
	ClientId           string `json:"clientId"`
	SecurityProtocol   string `json:"securityProtocol"`
	SaslMechanisms     string `json:"saslMechanisms"`
	SaslUsername       string `json:"saslUsername"`
	SaslPassword       string `json:"saslPassword"`
	HealthcheckTimeout int32  `json:"healthcheckTimeout"`
	LogLevel           string `json:"logLevel"`
	// TLS Configuration
	TLSAuthWithCACert bool   `json:"tlsAuthWithCACert"`
	TLSAuth           bool   `json:"tlsAuth"`
	TLSSkipVerify     bool   `json:"tlsSkipVerify"`
	ServerName        string `json:"serverName"`
	TLSCACert         string `json:"tlsCACert"`
	TLSClientCert     string `json:"tlsClientCert"`
	TLSClientKey      string `json:"tlsClientKey"`
	// Advanced HTTP settings
	Timeout int32 `json:"timeout"`
}

type KafkaClient struct {
	Dialer             *kafka.Dialer
	Reader             *kafka.Reader
	Conn               *kafka.Client
	BootstrapServers   string
	ClientId           string
	TimestampMode      string
	SecurityProtocol   string
	SaslMechanisms     string
	SaslUsername       string
	SaslPassword       string
	LogLevel           string
	HealthcheckTimeout int32
	// TLS Configuration
	TLSAuthWithCACert bool
	TLSAuth           bool
	TLSSkipVerify     bool
	ServerName        string
	TLSCACert         string
	TLSClientCert     string
	TLSClientKey      string
	// Advanced settings
	Timeout int32
}

type KafkaMessage struct {
	Value     map[string]float64
	Timestamp time.Time
	Offset    int64
}

func NewKafkaClient(options Options) KafkaClient {
	healthcheckTimeout := options.HealthcheckTimeout
	if healthcheckTimeout <= 0 {
		healthcheckTimeout = defaultHealthcheckTimeout
	}

	client := KafkaClient{
		BootstrapServers:   options.BootstrapServers,
		ClientId:           options.ClientId,
		SecurityProtocol:   options.SecurityProtocol,
		SaslMechanisms:     options.SaslMechanisms,
		SaslUsername:       options.SaslUsername,
		SaslPassword:       options.SaslPassword,
		LogLevel:           options.LogLevel,
		HealthcheckTimeout: healthcheckTimeout,
		// TLS Configuration
		TLSAuthWithCACert: options.TLSAuthWithCACert,
		TLSAuth:           options.TLSAuth,
		TLSSkipVerify:     options.TLSSkipVerify,
		ServerName:        options.ServerName,
		TLSCACert:         options.TLSCACert,
		TLSClientCert:     options.TLSClientCert,
		TLSClientKey:      options.TLSClientKey,
		// Advanced settings
		Timeout: options.Timeout,
	}
	return client
}

func (client *KafkaClient) NewConnection() error {
	var mechanism sasl.Mechanism
	var err error

	// Set up SASL mechanism if provided
	if client.SaslMechanisms != "" {
		mechanism, err = getSASLMechanism(client)
		if err != nil {
			return fmt.Errorf("unable to get SASL mechanism: %w", err)
		}
	}

	// Configure Dialer
	dialer := &kafka.Dialer{
		Timeout:       dialerTimeout,
		SASLMechanism: mechanism,
		ClientID:      client.ClientId, // Add Client ID support
	}

	// Configure Transport
	transport := &kafka.Transport{
		SASL:     mechanism,
		ClientID: client.ClientId, // Add Client ID support
	}

	// Configure TLS if SSL or SASL_SSL is used
	if client.SecurityProtocol == "SASL_SSL" || client.SecurityProtocol == "SSL" {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: client.TLSSkipVerify,
		}

		// Set server name for TLS verification if provided
		if client.ServerName != "" {
			tlsConfig.ServerName = client.ServerName
		}

		// Add CA certificate if self-signed certificate option is enabled
		if client.TLSAuthWithCACert && client.TLSCACert != "" {
			roots := x509.NewCertPool()
			if ok := roots.AppendCertsFromPEM([]byte(client.TLSCACert)); !ok {
				return fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = roots
		}

		// Add client certificate if TLS client authentication is enabled
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
	client.Conn = &kafka.Client{
		Addr:      kafka.TCP(strings.Split(client.BootstrapServers, ",")...),
		Timeout:   dialerTimeout,
		Transport: transport,
	}

	return nil
}

func (client *KafkaClient) newReader(topic string, partition int) *kafka.Reader {
	logger, errorLogger := getKafkaLogger(client.LogLevel)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(client.BootstrapServers, ","),
		Topic:          topic,
		Partition:      partition,
		Dialer:         client.Dialer,
		CommitInterval: 0,
		Logger:         logger,
		ErrorLogger:    errorLogger,
	})

	return reader
}

func (client *KafkaClient) NewStreamReader(
	ctx context.Context,
	topic string,
	partition int32,
	autoOffsetReset string,
) (*kafka.Reader, error) {
	var offset int64

	// Create connection if not exists
	if client.Dialer == nil {
		if err := client.NewConnection(); err != nil {
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}
	}

	// Set up offset
	switch autoOffsetReset {
	case "latest":
		offset = kafka.LastOffset
	case "earliest":
		// For earliest, we'll set a calculated offset but avoid DialLeader
		// which could trigger topic auto-creation. We'll use a reasonable
		// fallback that gets recent messages without connecting to specific topics.
		offset = kafka.LastOffset - maxEarliest

		// If the calculated offset is negative, use FirstOffset
		if offset < 0 {
			offset = kafka.FirstOffset
		}
	default:
		offset = kafka.LastOffset
	}

	// Create new reader
	reader := client.newReader(topic, int(partition))
	if err := reader.SetOffset(offset); err != nil {
		reader.Close()
		return nil, fmt.Errorf("unable to set offset: %w", err)
	}

	return reader, nil
}

func (client *KafkaClient) ConsumerPull(ctx context.Context, reader *kafka.Reader) (KafkaMessage, error) {
	var message KafkaMessage

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		return message, fmt.Errorf("error reading message from Kafka: %w", err)
	}

	if err := json.Unmarshal(msg.Value, &message.Value); err != nil {
		return message, fmt.Errorf("error unmarshalling message: %w", err)
	}

	message.Offset = msg.Offset
	message.Timestamp = msg.Time

	return message, nil
}

func (client *KafkaClient) HealthCheck() error {
	if err := client.NewConnection(); err != nil {
		return fmt.Errorf("unable to initialize Kafka client: %w", err)
	}

	// Log connection attempt with non-sensitive info
	brokers := strings.Split(client.BootstrapServers, ",")
	brokerCount := len(brokers)
	log.Printf("[KAFKA DEBUG] Attempting health check connection to %d broker(s)", brokerCount)

	// It is better to try several times due to possible network issues
	timeout := time.After(time.Duration(client.HealthcheckTimeout) * time.Millisecond)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("health check timed out after %d ms", client.HealthcheckTimeout)
		case <-ticker.C:
			// Use metadata request instead of ReadPartitions to avoid auto-creation
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
		return plain.Mechanism{
			Username: client.SaslUsername,
			Password: client.SaslPassword,
		}, nil
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

func (client *KafkaClient) IsTopicExists(ctx context.Context, topicName string) (bool, error) {
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{topicName},
	})
	if err != nil {
		return false, fmt.Errorf("unable to get metadata: %w", err)
	}

	if len(meta.Topics) > 0 && meta.Topics[0].Error == nil {
		return true, nil
	}

	return false, nil
}

func (client *KafkaClient) GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error) {
	// Use metadata request to get topic partitions - safer than ReadPartitions
	// as it doesn't set AllowAutoTopicCreation
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{topicName},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to get metadata for topic %s: %w", topicName, err)
	}

	if len(meta.Topics) == 0 {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	topic := meta.Topics[0]
	if topic.Error != nil {
		return nil, fmt.Errorf("error getting topic %s metadata: %w", topicName, topic.Error)
	}

	partitionIDs := make([]int32, len(topic.Partitions))
	for i, partition := range topic.Partitions {
		partitionIDs[i] = int32(partition.ID)
	}

	return partitionIDs, nil
}

func (client *KafkaClient) GetTopics(ctx context.Context, prefix string, limit int) ([]string, error) {
	// Use metadata request without specifying topics to get all topics
	// This is safer than ReadPartitions as it doesn't set AllowAutoTopicCreation
	meta, err := client.Conn.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("unable to get metadata: %w", err)
	}

	var matchingTopics []string
	for _, topic := range meta.Topics {
		if topic.Error != nil {
			continue // Skip topics with errors
		}

		// Filter by prefix if provided
		if prefix == "" || strings.HasPrefix(topic.Name, prefix) {
			matchingTopics = append(matchingTopics, topic.Name)

			// Apply limit if specified
			if limit > 0 && len(matchingTopics) >= limit {
				break
			}
		}
	}

	return matchingTopics, nil
}

func getKafkaLogger(level string) (kafka.LoggerFunc, kafka.LoggerFunc) {
	noop := kafka.LoggerFunc(func(msg string, args ...interface{}) {})

	var logger = noop
	var errorLogger = noop

	switch strings.ToLower(level) {
	case debugLogLevel:
		logger = func(msg string, args ...interface{}) {
			log.Printf("[KAFKA DEBUG] "+msg, args...)
		}
		errorLogger = func(msg string, args ...interface{}) {
			log.Printf("[KAFKA ERROR] "+msg, args...)
		}
	case errorLogLevel:
		errorLogger = func(msg string, args ...interface{}) {
			log.Printf("[KAFKA ERROR] "+msg, args...)
		}
	}

	return logger, errorLogger
}
