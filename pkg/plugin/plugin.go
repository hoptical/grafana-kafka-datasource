package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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
	ConsumerPull(ctx context.Context, reader *kafka.Reader) (kafka_client.KafkaMessage, error)
	Dispose()
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

type KafkaDatasource struct{ client KafkaClientAPI }

func (d *KafkaDatasource) Dispose() { d.client.Dispose() }

// NewWithClient allows injecting a custom KafkaClientAPI (primarily for tests).
func NewWithClient(c KafkaClientAPI) *KafkaDatasource { return &KafkaDatasource{client: c} }

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
	log.DefaultLogger.Debug("RunStream called", "path", req.Path)

	var qm queryModel
	err := json.Unmarshal(req.Data, &qm)
	if err != nil {
		log.DefaultLogger.Error("RunStream unmarshal error", "error", err)
		return err
	}

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

	// Create message channel and start partition readers
	messagesCh := make(chan messageWithPartition, streamMessageBuffer)
	streamManager.StartPartitionReaders(ctx, partitions, qm, messagesCh)

	// Main processing loop
	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Debug("Context done, finish streaming", "path", req.Path)
			return nil
		case msgWithPartition := <-messagesCh:
			frame, err := streamManager.ProcessMessage(
				msgWithPartition.msg,
				msgWithPartition.partition,
				partitions,
				qm,
			)
			if err != nil {
				log.DefaultLogger.Error("Error processing message", "error", err)
				continue
			}

			log.DefaultLogger.Debug("Message received",
				"topic", qm.Topic,
				"partition", msgWithPartition.partition,
				"offset", msgWithPartition.msg.Offset,
				"timestamp", frame.Fields[0].At(0),
				"fieldCount", len(msgWithPartition.msg.Value))

			err = sender.SendFrame(frame, data.IncludeAll)
			if err != nil {
				log.DefaultLogger.Error("Error sending frame", "error", err)
				continue
			}
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
