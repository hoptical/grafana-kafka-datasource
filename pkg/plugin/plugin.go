package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/live"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

var (
	_ backend.QueryDataHandler      = (*KafkaDatasource)(nil)
	_ backend.CheckHealthHandler    = (*KafkaDatasource)(nil)
	_ backend.StreamHandler         = (*KafkaDatasource)(nil)
	_ instancemgmt.InstanceDisposer = (*KafkaDatasource)(nil)
)

func NewKafkaInstance(s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	settings, err := getDatasourceSettings(s)

	if err != nil {
		return nil, err
	}

	kafka_client := kafka_client.NewKafkaClient(*settings)

	return &KafkaDatasource{kafka_client}, nil
}

func getDatasourceSettings(s backend.DataSourceInstanceSettings) (*kafka_client.Options, error) {
	settings := &kafka_client.Options{}

	if err := json.Unmarshal(s.JSONData, settings); err != nil {
		return nil, err
	}

	if sasl_password, exists := s.DecryptedSecureJSONData["saslPassword"]; exists {
		settings.SaslPassword = sasl_password
	}

	return settings, nil
}

type KafkaDatasource struct {
	client kafka_client.KafkaClient
}

func (d *KafkaDatasource) Dispose() {
	// Clean up datasource instance resources.
}

func (d *KafkaDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	log.DefaultLogger.Info("QueryData called", "request", req)

	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, q)

		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	Topic           string `json:"topicName"`
	Partition       int32  `json:"partition"`
	WithStreaming   bool   `json:"withStreaming"`
	AutoOffsetReset string `json:"autoOffsetReset"`
	TimestampMode   string `json:"timestampMode"`
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

	topic := qm.Topic
	partition := qm.Partition
	autoOffsetReset := qm.AutoOffsetReset
	timestampMode := qm.TimestampMode
	if qm.WithStreaming {
		channel := live.Channel{
			Scope:     live.ScopeDatasource,
			Namespace: pCtx.DataSourceInstanceSettings.UID,
			Path:      fmt.Sprintf("%v_%d_%v_%v", topic, partition, autoOffsetReset, timestampMode),
		}
		frame.SetMeta(&data.FrameMeta{Channel: channel.String()})
	}

	response.Frames = append(response.Frames, frame)

	return response
}

func (d *KafkaDatasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	log.DefaultLogger.Info("CheckHealth called", "request", req)

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	err := d.client.HealthCheck()

	if err != nil {
		status = backend.HealthStatusError
		message = "Cannot connect to the brokers!"
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func (d *KafkaDatasource) SubscribeStream(_ context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	log.DefaultLogger.Info("SubscribeStream called", "request", req)
	// Extract the query parameters
	var path []string = strings.Split(req.Path, "_")
	topic := path[0]
	partition, _ := strconv.Atoi(path[1])
	autoOffsetReset := path[2]
	timestampMode := path[3]
	// Initialize Consumer and Assign the topic
	d.client.TopicAssign(topic, int32(partition), autoOffsetReset, timestampMode)
	status := backend.SubscribeStreamStatusPermissionDenied
	status = backend.SubscribeStreamStatusOK

	return &backend.SubscribeStreamResponse{
		Status: status,
	}, nil
}

func (d *KafkaDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Info("RunStream called", "request", req)

	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Info("Context done, finish streaming", "path", req.Path)
			return nil
		default:
			msg, event := d.client.ConsumerPull()
			if event == nil {
				continue
			}
			frame := data.NewFrame("response")
			frame.Fields = append(frame.Fields,
				data.NewField("time", nil, make([]time.Time, 1)),
			)
			var frame_time time.Time
			if d.client.TimestampMode == "now" {
				frame_time = time.Now()
			} else {
				frame_time = msg.Timestamp
			}
			log.DefaultLogger.Info("Offset", msg.Offset)
			log.DefaultLogger.Info("timestamp", frame_time)
			frame.Fields[0].Set(0, frame_time)

			cnt := 1

			for key, value := range msg.Value {
				frame.Fields = append(frame.Fields,
					data.NewField(key, nil, make([]float64, 1)))
				frame.Fields[cnt].Set(0, value)
				cnt++
			}

			err := sender.SendFrame(frame, data.IncludeAll)

			if err != nil {
				log.DefaultLogger.Error("Error sending frame", "error", err)
				continue
			}
		}
	}
}

func (d *KafkaDatasource) PublishStream(_ context.Context, req *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	log.DefaultLogger.Info("PublishStream called", "request", req)

	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
