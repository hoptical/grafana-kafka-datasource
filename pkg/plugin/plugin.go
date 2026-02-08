package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/segmentio/kafka-go"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// streamMessageBuffer defines the capacity of the buffered channel used to fan-in
// messages from per-partition goroutines in RunStream. It provides headroom for
// short bursts (multiple partitions producing at once) without blocking readers,
// yet remains small to keep memory usage low and latency tight.
const streamMessageBuffer = 100

// Stream management constants
const (
	streamCleanupDelay   = 500 * time.Millisecond // Delay to ensure previous stream is fully stopped
	streamCancelTimeout  = 500 * time.Millisecond // Max time to wait for stream context cancellation
	messageReadTimeout   = 5 * time.Second        // Timeout for reading individual messages
	channelDrainTimeout  = 100 * time.Millisecond // Timeout for draining message channels
	retryDelayAfterError = 100 * time.Millisecond // Brief pause between retries after errors
)

// Defaults for JSON flattening behavior
const (
	defaultFlattenMaxDepth = 5
	defaultFlattenFieldCap = 1000
)

// KafkaClientAPI abstracts the kafka client for easier testing.
type KafkaClientAPI interface {
	NewConnection() error
	GetTopicPartitions(ctx context.Context, topicName string) ([]int32, error)
	GetTopics(ctx context.Context, prefix string, limit int) ([]string, error)
	HealthCheck() error
	NewStreamReader(ctx context.Context, topic string, partition int32, autoOffsetReset string, lastN int32) (*kafka.Reader, error)
	ConsumerPull(ctx context.Context, reader *kafka.Reader, messageFormat string) (kafka_client.KafkaMessage, error)
	Dispose()
	// Avro-related methods
	GetMessageFormat() string
	GetSchemaRegistryUrl() string
	GetSchemaRegistryUsername() string
	GetSchemaRegistryPassword() string
	GetSubjectNamingStrategy() string
	GetHTTPClient() *http.Client
}

var (
	_ backend.QueryDataHandler      = (*KafkaDatasource)(nil)
	_ backend.CheckHealthHandler    = (*KafkaDatasource)(nil)
	_ backend.StreamHandler         = (*KafkaDatasource)(nil)
	_ backend.CallResourceHandler   = (*KafkaDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*KafkaDatasource)(nil)
)

func NewKafkaInstance(ctx context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)
	if err != nil {
		return nil, err
	}

	// Create HTTP client using Grafana Plugin SDK to support Private Data Source Connect (PDC)
	// This enables automatic SOCKS proxy handling for secure connections to private networks
	opts, err := s.HTTPClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP client options: %w", err)
	}

	httpClient, err := httpclient.New(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	kc := kafka_client.NewKafkaClient(*settings, httpClient)

	return &KafkaDatasource{client: &kc, settings: settings}, nil
}

func getDatasourceSettings(s backend.DataSourceInstanceSettings) (*kafka_client.Options, error) {
	settings := &kafka_client.Options{}

	if err := json.Unmarshal(s.JSONData, settings); err != nil {
		return nil, err
	}

	// Handle secure JSON data
	if saslPassword, exists := s.DecryptedSecureJSONData["saslPassword"]; exists {
		settings.SaslPassword = saslPassword
	}

	// TLS certificate fields from secure JSON data
	if caCert, exists := s.DecryptedSecureJSONData["tlsCACert"]; exists {
		settings.TLSCACert = caCert
	}
	if clientCert, exists := s.DecryptedSecureJSONData["tlsClientCert"]; exists {
		settings.TLSClientCert = clientCert
	}
	if clientKey, exists := s.DecryptedSecureJSONData["tlsClientKey"]; exists {
		settings.TLSClientKey = clientKey
	}

	// Schema Registry authentication fields from secure JSON data
	if schemaRegistryUsername, exists := s.DecryptedSecureJSONData["schemaRegistryUsername"]; exists {
		settings.SchemaRegistryUsername = schemaRegistryUsername
	}
	if schemaRegistryPassword, exists := s.DecryptedSecureJSONData["schemaRegistryPassword"]; exists {
		settings.SchemaRegistryPassword = schemaRegistryPassword
	}

	// Parse the JSONData to handle specific field types
	var jsonData map[string]interface{}
	if err := json.Unmarshal(s.JSONData, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON data: %w", err)
	}

	// Handle boolean fields that might come as different types from JSON
	if val, exists := jsonData["tlsSkipVerify"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSSkipVerify = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSSkipVerify = true
		}
	}

	if val, exists := jsonData["tlsAuthWithCACert"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSAuthWithCACert = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSAuthWithCACert = true
		}
	}

	if val, exists := jsonData["tlsAuth"]; exists {
		if b, ok := val.(bool); ok {
			settings.TLSAuth = b
		} else if str, ok := val.(string); ok && str == "true" {
			settings.TLSAuth = true
		}
	}

	// Validate and set JSON flattening parameters with proper bounds
	if settings.FlattenMaxDepth == 0 {
		settings.FlattenMaxDepth = defaultFlattenMaxDepth
	} else if settings.FlattenMaxDepth < 1 {
		settings.FlattenMaxDepth = 1
	}

	if settings.FlattenFieldCap == 0 {
		settings.FlattenFieldCap = defaultFlattenFieldCap
	} else if settings.FlattenFieldCap < 1 {
		settings.FlattenFieldCap = 1
	}

	return settings, nil
}

type KafkaDatasource struct {
	client   KafkaClientAPI
	settings *kafka_client.Options
}

func (d *KafkaDatasource) Dispose() { d.client.Dispose() }

// NewWithClient allows injecting a custom KafkaClientAPI (primarily for tests).
func NewWithClient(c KafkaClientAPI) *KafkaDatasource {
	return &KafkaDatasource{
		client: c,
		settings: &kafka_client.Options{
			FlattenMaxDepth: defaultFlattenMaxDepth,
			FlattenFieldCap: defaultFlattenFieldCap,
		},
	}
}

func (d *KafkaDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Debug("QueryData called", "request", req)

	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	Topic           string      `json:"topicName"`
	Partition       interface{} `json:"partition"` // Can be int32 or "all"
	AutoOffsetReset string      `json:"autoOffsetReset"`
	TimestampMode   string      `json:"timestampMode"`
	LastN           int32       `json:"lastN"`
	// Avro Configuration
	MessageFormat    string `json:"messageFormat"`
	AvroSchemaSource string `json:"avroSchemaSource"`
	AvroSchema       string `json:"avroSchema"`
	// Protobuf Configuration
	ProtobufSchemaSource string `json:"protobufSchemaSource"`
	ProtobufSchema       string `json:"protobufSchema"`
	// Metadata
	RefID string `json:"refId"`
	Alias string `json:"alias"`
}

func (d *KafkaDatasource) query(_ context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
	response := backend.DataResponse{}
	var qm queryModel
	response.Error = json.Unmarshal(query.JSON, &qm)

	if response.Error != nil {
		return response
	}

	frame := data.NewFrame("response")

	frame.Fields = append(frame.Fields,
		data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
		data.NewField("values", nil, []int64{0, 0}),
	)

	response.Frames = append(response.Frames, frame)

	return response
}

func (d *KafkaDatasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	log.DefaultLogger.Debug("CallResource called", "path", req.Path, "method", req.Method)
	if req.Path == "partitions" && req.Method == "GET" {
		return d.handleGetPartitions(ctx, req, sender)
	}
	if req.Path == "topics" && req.Method == "GET" {
		return d.handleSearchTopics(ctx, req, sender)
	}
	if req.Path == "validate-schema-registry" && req.Method == "GET" {
		return d.handleValidateSchemaRegistry(ctx, req, sender)
	}
	if req.Path == "validate-avro-schema" && req.Method == "POST" {
		return d.handleValidateAvroSchema(ctx, req, sender)
	}
	if req.Path == "validate-protobuf-schema" && req.Method == "POST" {
		return d.handleValidateProtobufSchema(ctx, req, sender)
	}
	return sendJSON(sender, 404, map[string]string{"error": "Not found"})
}

func sendJSON(sender backend.CallResourceResponseSender, status int, body interface{}) error {
	b, err := json.Marshal(body)
	if err != nil {
		return sender.Send(&backend.CallResourceResponse{
			Status:  500,
			Body:    []byte(`{"error": "Failed to marshal response"}`),
			Headers: map[string][]string{"Content-Type": {"application/json"}},
		})
	}
	return sender.Send(&backend.CallResourceResponse{
		Status:  status,
		Body:    b,
		Headers: map[string][]string{"Content-Type": {"application/json"}},
	})
}

func (d *KafkaDatasource) handleGetPartitions(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Parse URL to get query parameters
	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		return sendJSON(sender, 400, map[string]string{"error": "Invalid URL"})
	}

	topicName := parsedURL.Query().Get("topic")
	if topicName == "" {
		return sendJSON(sender, 400, map[string]string{"error": "topic parameter is required"})
	}

	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Failed to create connection", "error", err)
		return sendJSON(sender, 500, map[string]string{"error": fmt.Sprintf("Failed to connect: %s", err.Error())})
	}
	partitions, err := d.client.GetTopicPartitions(ctx, topicName)
	if err != nil {
		log.DefaultLogger.Error("Failed to get partitions", "topic", topicName, "error", err)
		// Classify not-found using sentinel error from client
		if errors.Is(err, kafka_client.ErrTopicNotFound) {
			return sendJSON(sender, 404, map[string]string{"error": fmt.Sprintf("topic %s not found", topicName)})
		}
		return sendJSON(sender, 500, map[string]string{"error": fmt.Sprintf("Failed to get partitions: %s", err.Error())})
	}

	return sendJSON(sender, 200, map[string]interface{}{"partitions": partitions, "topic": topicName})
}

func (d *KafkaDatasource) handleSearchTopics(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Parse URL to get query parameters
	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		return sendJSON(sender, 400, map[string]string{"error": "Invalid URL"})
	}
	prefix := parsedURL.Query().Get("prefix")
	limitStr := parsedURL.Query().Get("limit")
	limit := 5 // Default limit
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}
	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Failed to create connection", "error", err)
		return sendJSON(sender, 500, map[string]string{"error": fmt.Sprintf("Failed to connect: %s", err.Error())})
	}
	topics, err := d.client.GetTopics(ctx, prefix, limit)
	if err != nil {
		log.DefaultLogger.Error("Failed to get topics", "prefix", prefix, "error", err)
		return sendJSON(sender, 500, map[string]string{"error": fmt.Sprintf("Failed to get topics: %s", err.Error())})
	}
	log.DefaultLogger.Debug("Topic search results", "prefix", prefix, "count", len(topics))
	return sendJSON(sender, 200, map[string]interface{}{
		"topics": topics,
		"prefix": prefix,
		"limit":  limit,
	})
}

func (d *KafkaDatasource) handleValidateSchemaRegistry(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	result, err := d.ValidateSchemaRegistry(ctx)
	if err != nil {
		return sendJSON(sender, 500, map[string]string{"error": err.Error()})
	}

	status := 200
	if result.Status == backend.HealthStatusError {
		status = 400
	}

	return sendJSON(sender, status, map[string]interface{}{
		"status":  result.Status,
		"message": result.Message,
	})
}

func (d *KafkaDatasource) handleValidateAvroSchema(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	// Parse the request body to get the schema
	var requestBody struct {
		Schema string `json:"schema"`
	}

	if err := json.Unmarshal(req.Body, &requestBody); err != nil {
		return sendJSON(sender, 400, map[string]string{"error": "Invalid JSON in request body"})
	}

	result, err := d.ValidateAvroSchema(requestBody.Schema)
	if err != nil {
		return sendJSON(sender, 500, map[string]string{"error": err.Error()})
	}

	status := 200
	if result.Status == backend.HealthStatusError {
		status = 400
	}

	return sendJSON(sender, status, map[string]interface{}{
		"status":  result.Status,
		"message": result.Message,
	})
}

func (d *KafkaDatasource) handleValidateProtobufSchema(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	var requestBody struct {
		Schema string `json:"schema"`
	}

	if err := json.Unmarshal(req.Body, &requestBody); err != nil {
		return sendJSON(sender, 400, map[string]string{"error": "Invalid JSON in request body"})
	}

	result, err := d.ValidateProtobufSchema(requestBody.Schema)
	if err != nil {
		return sendJSON(sender, 500, map[string]string{"error": err.Error()})
	}

	status := 200
	if result.Status == backend.HealthStatusError {
		status = 400
	}

	return sendJSON(sender, status, map[string]interface{}{
		"status":  result.Status,
		"message": result.Message,
	})
}

// ValidateSchemaRegistry validates the schema registry configuration
func (d *KafkaDatasource) ValidateSchemaRegistry(ctx context.Context) (*backend.CheckHealthResult, error) {
	schemaRegistryUrl := d.client.GetSchemaRegistryUrl()
	if schemaRegistryUrl == "" {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Schema registry URL is not configured",
		}, nil
	}

	// Parse and validate URL format
	parsedUrl, err := url.Parse(schemaRegistryUrl)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Invalid schema registry URL format: %s", err.Error()),
		}, nil
	}

	// Check if it's a valid HTTP/HTTPS URL
	if parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https" {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Schema registry URL must use HTTP or HTTPS protocol",
		}, nil
	}

	// Try to connect to schema registry by attempting to get a schema
	// Use a dummy subject that likely doesn't exist to test connectivity

	// Get HTTP client from KafkaClient
	httpClient := d.client.GetHTTPClient()
	if httpClient == nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "HTTP client not available for Schema Registry health check",
		}, nil
	}

	schemaClient := kafka_client.NewSchemaRegistryClient(
		schemaRegistryUrl,
		d.client.GetSchemaRegistryUsername(),
		d.client.GetSchemaRegistryPassword(),
		httpClient,
	)

	testSubject := "_test_connectivity_subject_"
	_, err = schemaClient.GetLatestSchema(testSubject)
	// We expect this to fail with a 404 (subject not found), which means the connection works
	// Any other error indicates a connection problem
	if err != nil && !isSubjectNotFoundError(err) {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Cannot connect to schema registry: %s", err.Error()),
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Schema registry is accessible",
	}, nil
}

// ValidateAvroSchema validates an Avro schema string
func (d *KafkaDatasource) ValidateAvroSchema(schema string) (*backend.CheckHealthResult, error) {
	if schema == "" {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Schema cannot be empty",
		}, nil
	}

	// Basic JSON validation
	var schemaObj interface{}
	if err := json.Unmarshal([]byte(schema), &schemaObj); err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Invalid JSON format: %s", err.Error()),
		}, nil
	}

	// Check if it's a valid Avro schema structure
	schemaMap, ok := schemaObj.(map[string]interface{})
	if !ok {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Schema must be a JSON object",
		}, nil
	}

	// Check for required Avro schema fields
	if _, hasType := schemaMap["type"]; !hasType {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Avro schema must have a 'type' field",
		}, nil
	}

	// For record types, check for required fields
	if schemaType, ok := schemaMap["type"].(string); ok && schemaType == "record" {
		if _, hasName := schemaMap["name"]; !hasName {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: "Record schema must have a 'name' field",
			}, nil
		}
		if _, hasFields := schemaMap["fields"]; !hasFields {
			return &backend.CheckHealthResult{
				Status:  backend.HealthStatusError,
				Message: "Record schema must have a 'fields' field",
			}, nil
		}
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Schema is valid",
	}, nil
}

// ValidateProtobufSchema validates a Protobuf schema string
func (d *KafkaDatasource) ValidateProtobufSchema(schema string) (*backend.CheckHealthResult, error) {
	if strings.TrimSpace(schema) == "" {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Schema cannot be empty",
		}, nil
	}

	if _, err := kafka_client.ParseProtobufSchema(schema); err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("Invalid Protobuf schema: %s", err.Error()),
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Schema is valid",
	}, nil
}

// isSubjectNotFoundError checks if the error indicates a subject not found (404)
func isSubjectNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "404") || strings.Contains(errStr, "Subject not found")
}

func (d *KafkaDatasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var datasourceID int64 = 0
	if req.PluginContext.DataSourceInstanceSettings != nil {
		datasourceID = req.PluginContext.DataSourceInstanceSettings.ID
	}
	log.DefaultLogger.Debug("CheckHealth called", "datasourceID", datasourceID)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	if d.client == nil {
		status = backend.HealthStatusError
		message = "client not initialized"
		return &backend.CheckHealthResult{Status: status, Message: message}, nil
	}

	err := d.client.HealthCheck()
	if err != nil {
		status = backend.HealthStatusError
		message = err.Error()
		log.DefaultLogger.Error("Plugin health check failed.", "error", err)
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (d *KafkaDatasource) SubscribeStream(ctx context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	log.DefaultLogger.Debug("SubscribeStream called", "path", req.Path)

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("SubscribeStream unmarshal error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	if qm.Topic == "" {
		err := fmt.Errorf("empty topic in stream path: %q", req.Path)
		log.DefaultLogger.Error("SubscribeStream topic error", "error", err)
		return &backend.SubscribeStreamResponse{
			Status: backend.SubscribeStreamStatusPermissionDenied,
		}, err
	}

	log.DefaultLogger.Debug("SubscribeStream prepared", "topic", qm.Topic, "partition", qm.Partition)
	return &backend.SubscribeStreamResponse{Status: backend.SubscribeStreamStatusOK}, nil
}

func (d *KafkaDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Debug("RunStream called", "path", req.Path, "dataLength", len(req.Data))

	// Log the raw request data for debugging
	if len(req.Data) > 0 {
		log.DefaultLogger.Debug("RunStream raw request data", "data", string(req.Data))
	}

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("RunStream unmarshal error", "error", err)
		return err
	}

	log.DefaultLogger.Debug("Parsed queryModel", "topic", qm.Topic, "partition", qm.Partition, "partitionType", fmt.Sprintf("%T", qm.Partition))

	log.DefaultLogger.Debug("Starting Kafka stream with configuration",
		"topic", qm.Topic,
		"partition", qm.Partition,
		"messageFormat", qm.MessageFormat,
		"avroSchemaSource", qm.AvroSchemaSource,
		"avroSchemaLength", len(qm.AvroSchema),
		"protobufSchemaSource", qm.ProtobufSchemaSource,
		"protobufSchemaLength", len(qm.ProtobufSchema),
		"autoOffsetReset", qm.AutoOffsetReset,
		"timestampMode", qm.TimestampMode)

	// Create connection
	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Failed to create Kafka connection for streaming", "error", err)
		return fmt.Errorf("failed to establish Kafka connection: %w", err)
	}

	// Create stream manager and validate partitions
	streamManager := NewStreamManager(d.client, d.settings.FlattenMaxDepth, d.settings.FlattenFieldCap)
	partitions, err := streamManager.ValidateAndGetPartitions(ctx, qm)
	if err != nil {
		return err
	}

	log.DefaultLogger.Debug("RunStream partitions", "topic", qm.Topic, "partitions", partitions)

	// Create new stream configuration
	streamConfig := &StreamConfig{
		MessageFormat:        qm.MessageFormat,
		AvroSchemaSource:     qm.AvroSchemaSource,
		AvroSchema:           qm.AvroSchema,
		ProtobufSchemaSource: qm.ProtobufSchemaSource,
		ProtobufSchema:       qm.ProtobufSchema,
		AutoOffsetReset:      qm.AutoOffsetReset,
		TimestampMode:        qm.TimestampMode,
		LastN:                qm.LastN, // Added LastN to stream config
		RefID:                qm.RefID,
		Alias:                qm.Alias,
	}

	// Set default values if not provided
	if streamConfig.MessageFormat == "" {
		streamConfig.MessageFormat = "json"
	}
	if streamConfig.AvroSchemaSource == "" {
		streamConfig.AvroSchemaSource = "schemaRegistry"
	}
	if streamConfig.ProtobufSchemaSource == "" {
		streamConfig.ProtobufSchemaSource = "schemaRegistry"
	}
	if streamConfig.AutoOffsetReset == "" {
		streamConfig.AutoOffsetReset = "latest"
	}
	if streamConfig.TimestampMode == "" {
		streamConfig.TimestampMode = "message"
	}

	// Create message channel and start partition readers
	messagesCh := make(chan messageWithPartition, streamMessageBuffer)
	streamManager.StartPartitionReaders(ctx, partitions, qm, streamConfig, messagesCh)

	log.DefaultLogger.Debug("Started new stream",
		"topic", qm.Topic,
		"partitions", partitions)

	// Main processing loop
	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Debug("Stream context done, finishing",
				"path", req.Path,
				"totalMessages", messageCount)
			return nil

		case msgWithPartition := <-messagesCh:
			messageCount++
			log.DefaultLogger.Debug("Processing message from channel",
				"messageNumber", messageCount,
				"partition", msgWithPartition.partition,
				"offset", msgWithPartition.msg.Offset)

			frame, err := streamManager.ProcessMessage(
				msgWithPartition.msg,
				msgWithPartition.partition,
				partitions,
				streamConfig,
				qm.Topic,
			)
			if err != nil {
				log.DefaultLogger.Error("Error processing message",
					"messageNumber", messageCount,
					"partition", msgWithPartition.partition,
					"offset", msgWithPartition.msg.Offset,
					"error", err)
				continue
			}

			fieldCount := 0
			if obj, ok := msgWithPartition.msg.Value.(map[string]interface{}); ok {
				fieldCount = len(obj)
			} else if arr, ok := msgWithPartition.msg.Value.([]interface{}); ok {
				fieldCount = len(arr)
			}

			log.DefaultLogger.Debug("Message frame created",
				"messageNumber", messageCount,
				"partition", msgWithPartition.partition,
				"offset", msgWithPartition.msg.Offset,
				"timestamp", frame.Fields[0].At(0),
				"fieldCount", fieldCount)

			log.DefaultLogger.Debug("Sending frame to Grafana",
				"messageNumber", messageCount,
				"partition", msgWithPartition.partition)
			err = sender.SendFrame(frame, data.IncludeAll)
			if err != nil {
				log.DefaultLogger.Error("Error sending frame",
					"messageNumber", messageCount,
					"partition", msgWithPartition.partition,
					"error", err)
				continue
			}

			log.DefaultLogger.Debug("Successfully sent frame to Grafana",
				"messageNumber", messageCount,
				"partition", msgWithPartition.partition,
				"frameFields", len(frame.Fields),
				"frameName", frame.Name)
		}
	}
}

type messageWithPartition struct {
	msg       kafka_client.KafkaMessage
	partition int32
}

func (d *KafkaDatasource) PublishStream(_ context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	log.DefaultLogger.Debug("PublishStream called",
		"path", req.Path)

	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
