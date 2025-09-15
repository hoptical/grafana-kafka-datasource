package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
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
	GetAvroSubjectNamingStrategy() string
}

var (
	_ backend.QueryDataHandler      = (*KafkaDatasource)(nil)
	_ backend.CheckHealthHandler    = (*KafkaDatasource)(nil)
	_ backend.StreamHandler         = (*KafkaDatasource)(nil)
	_ backend.CallResourceHandler   = (*KafkaDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*KafkaDatasource)(nil)
)

func NewKafkaInstance(_ context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)

	if err != nil {
		return nil, err
	}

	kc := kafka_client.NewKafkaClient(*settings)

	return &KafkaDatasource{client: &kc}, nil
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

	return settings, nil
}

type KafkaDatasource struct {
	client            KafkaClientAPI
	streamManager     *StreamManager
	streamConfig      *StreamConfig
	currentTopic      string
	currentPartitions []int32
	streamCtx         context.Context
	streamCancel      context.CancelFunc
}

// configChanged checks if the new query model differs from the current stream configuration
// Note: Topic changes are handled separately and should not be included here
func (d *KafkaDatasource) configChanged(qm queryModel) bool {
	if d.streamConfig == nil {
		return true
	}

	return d.streamConfig.MessageFormat != qm.MessageFormat ||
		d.streamConfig.AvroSchemaSource != qm.AvroSchemaSource ||
		d.streamConfig.AvroSchema != qm.AvroSchema ||
		d.streamConfig.AutoOffsetReset != qm.AutoOffsetReset ||
		d.streamConfig.TimestampMode != qm.TimestampMode ||
		d.streamConfig.LastN != qm.LastN // Added LastN to config change detection
}

// partitionsChanged checks if the partitions have changed
func (d *KafkaDatasource) partitionsChanged(qm queryModel, partitions []int32) bool {
	if len(d.currentPartitions) != len(partitions) {
		return true
	}

	// Check if partition list has changed
	for i, p := range partitions {
		if i >= len(d.currentPartitions) || d.currentPartitions[i] != p {
			return true
		}
	}

	return false
}

func (d *KafkaDatasource) Dispose() { d.client.Dispose() }

// NewWithClient allows injecting a custom KafkaClientAPI (primarily for tests).
// NewWithClient allows injecting a custom KafkaClientAPI (primarily for tests).
func NewWithClient(c KafkaClientAPI) *KafkaDatasource {
	return &KafkaDatasource{client: c}
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
	schemaClient := kafka_client.NewSchemaRegistryClient(
		schemaRegistryUrl,
		d.client.GetSchemaRegistryUsername(),
		d.client.GetSchemaRegistryPassword(),
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
	log.DefaultLogger.Info("RunStream called", "path", req.Path, "dataLength", len(req.Data))

	// Log the raw request data for debugging
	if len(req.Data) > 0 {
		log.DefaultLogger.Info("RunStream raw request data", "data", string(req.Data))
	}

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("RunStream unmarshal error", "error", err)
		return err
	}

	log.DefaultLogger.Info("Parsed queryModel", "topic", qm.Topic, "partition", qm.Partition, "partitionType", fmt.Sprintf("%T", qm.Partition))

	log.DefaultLogger.Info("Starting Kafka stream with configuration",
		"topic", qm.Topic,
		"partition", qm.Partition,
		"messageFormat", qm.MessageFormat,
		"avroSchemaSource", qm.AvroSchemaSource,
		"avroSchemaLength", len(qm.AvroSchema),
		"autoOffsetReset", qm.AutoOffsetReset,
		"timestampMode", qm.TimestampMode)

	// Create connection
	if err := d.client.NewConnection(); err != nil {
		log.DefaultLogger.Error("Failed to create Kafka connection for streaming", "error", err)
		return fmt.Errorf("failed to establish Kafka connection: %w", err)
	}

	// Create stream manager and validate partitions
	streamManager := NewStreamManager(d.client)
	partitions, err := streamManager.ValidateAndGetPartitions(ctx, qm)
	if err != nil {
		return err
	}

	log.DefaultLogger.Debug("RunStream partitions", "topic", qm.Topic, "partitions", partitions)

	// Create new stream configuration
	newStreamConfig := &StreamConfig{
		MessageFormat:    qm.MessageFormat,
		AvroSchemaSource: qm.AvroSchemaSource,
		AvroSchema:       qm.AvroSchema,
		AutoOffsetReset:  qm.AutoOffsetReset,
		TimestampMode:    qm.TimestampMode,
		LastN:            qm.LastN, // Added LastN to stream config
	}

	// Set default values if not provided
	if newStreamConfig.MessageFormat == "" {
		newStreamConfig.MessageFormat = "json"
	}
	if newStreamConfig.AvroSchemaSource == "" {
		newStreamConfig.AvroSchemaSource = "schemaRegistry"
	}
	if newStreamConfig.AutoOffsetReset == "" {
		newStreamConfig.AutoOffsetReset = "latest"
	}
	if newStreamConfig.TimestampMode == "" {
		newStreamConfig.TimestampMode = "message"
	}

	// Check if we need to restart the stream or just update configuration
	topicChanged := d.currentTopic != qm.Topic
	partitionsChanged := d.partitionsChanged(qm, partitions)
	configChanged := d.configChanged(qm)

	log.DefaultLogger.Debug("Stream change detection",
		"topicChanged", topicChanged,
		"partitionsChanged", partitionsChanged,
		"configChanged", configChanged,
		"hasExistingStream", d.streamManager != nil,
		"currentMessageFormat", func() string {
			if d.streamConfig != nil {
				return d.streamConfig.MessageFormat
			}
			return "none"
		}(),
		"newMessageFormat", qm.MessageFormat,
		"currentLastN", func() int32 {
			if d.streamConfig != nil {
				return d.streamConfig.LastN
			}
			return 0
		}(),
		"newLastN", qm.LastN)

	// If topic, partitions, or configuration changed, or no existing stream, start fresh
	if topicChanged || partitionsChanged || configChanged || d.streamManager == nil {
		// Stop existing stream if running
		if d.streamCancel != nil {
			reason := "topic/partition changes"
			if configChanged && !topicChanged && !partitionsChanged {
				reason = "configuration changes"
			}
			log.DefaultLogger.Info("Stopping existing stream due to "+reason,
				"oldMessageFormat", func() string {
					if d.streamConfig != nil {
						return d.streamConfig.MessageFormat
					}
					return "none"
				}(),
				"newMessageFormat", qm.MessageFormat)
			d.streamCancel()

			// Wait for the stream context to be fully cancelled
			// This ensures the stream processing loop has exited cleanly
			if d.streamCtx != nil {
				select {
				case <-d.streamCtx.Done():
					log.DefaultLogger.Info("Previous stream context confirmed cancelled")
				case <-time.After(streamCancelTimeout):
					log.DefaultLogger.Warn("Previous stream context cancellation timed out after 500ms")
				}
			}

			d.streamCancel = nil

			// Additional delay to ensure all partition readers have stopped
			// and message channels are drained - increased to 500ms for better reliability
			time.Sleep(streamCleanupDelay)
			log.DefaultLogger.Info("Stream cleanup delay completed - ready for new stream")
		}

		// Clear all previous state to ensure fresh start
		d.streamManager = nil
		d.streamConfig = nil

		// Update persistent state with new configuration
		d.streamManager = streamManager
		d.streamConfig = newStreamConfig
		d.currentTopic = qm.Topic
		d.currentPartitions = partitions
		d.streamCtx, d.streamCancel = context.WithCancel(ctx)

		log.DefaultLogger.Info("Updated stream state with new configuration",
			"messageFormat", d.streamConfig.MessageFormat,
			"avroSchemaSource", d.streamConfig.AvroSchemaSource,
			"autoOffsetReset", d.streamConfig.AutoOffsetReset,
			"topic", d.currentTopic,
			"partitions", d.currentPartitions)

		// Create message channel and start partition readers
		messagesCh := make(chan messageWithPartition, streamMessageBuffer)
		d.streamManager.StartPartitionReaders(d.streamCtx, partitions, qm, d.streamConfig, messagesCh)

		log.DefaultLogger.Info("Started new stream",
			"topic", qm.Topic,
			"partitions", partitions)

		// Main processing loop
		messageCount := 0
		for {
			select {
			case <-d.streamCtx.Done():
				log.DefaultLogger.Info("Stream context done, finishing",
					"path", req.Path,
					"totalMessages", messageCount)
				// Drain any remaining messages in the channel to prevent them from being processed
				// with the wrong configuration when a new stream starts
				drainedCount := 0
				drainStart := time.Now()
				for {
					select {
					case <-messagesCh:
						drainedCount++
						// Continue draining messages
					case <-time.After(channelDrainTimeout):
						// Stop draining after 100ms to avoid blocking too long
						log.DefaultLogger.Info("Message channel drain completed",
							"drainedMessages", drainedCount,
							"drainDuration", time.Since(drainStart))
						return nil
					}
				}
			case msgWithPartition := <-messagesCh:
				messageCount++
				log.DefaultLogger.Debug("Processing message from channel",
					"messageNumber", messageCount,
					"partition", msgWithPartition.partition,
					"offset", msgWithPartition.msg.Offset)

				frame, err := d.streamManager.ProcessMessage(
					msgWithPartition.msg,
					msgWithPartition.partition,
					partitions,
					d.streamConfig,
					d.currentTopic,
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
	} else {
		// No changes, just wait for context cancellation
		log.DefaultLogger.Info("No changes detected, waiting for context cancellation")
		<-ctx.Done()
		return nil
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
