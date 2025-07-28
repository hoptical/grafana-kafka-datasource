package plugin_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/hoptical/grafana-kafka-datasource/pkg/plugin"
)

func TestQueryData(t *testing.T) {
	ds := plugin.KafkaDatasource{}
	// Simulate a query with TLS and clientId config in JSONData
	jsonData := map[string]interface{}{
		"clientId":          "test-client",
		"tlsAuthWithCACert": true,
		"tlsAuth":           true,
		"tlsSkipVerify":     true,
		"serverName":        "test-server",
		"keepCookies":       []string{"cookie1", "cookie2"},
		"timeout":           1234,
	}
	jsonBytes, _ := json.Marshal(jsonData)
	resp, err := ds.QueryData(
		context.Background(),
		&backend.QueryDataRequest{
			Queries: []backend.DataQuery{
				{RefID: "A", JSON: jsonBytes},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}
	if len(resp.Responses) != 1 {
		t.Fatal("QueryData must return a response")
	}
}

func TestCheckHealth_OK(t *testing.T) {
	ds := plugin.KafkaDatasource{}
	// Simulate DataSourceInstanceSettings with TLS and clientId config
	jsonData := map[string]interface{}{
		"clientId":          "test-client",
		"tlsAuthWithCACert": true,
		"tlsAuth":           true,
		"tlsSkipVerify":     true,
		"serverName":        "test-server",
		"keepCookies":       []string{"cookie1", "cookie2"},
		"timeout":           1234,
	}
	jsonBytes, _ := json.Marshal(jsonData)
	result, err := ds.CheckHealth(context.Background(), &backend.CheckHealthRequest{
		PluginContext: backend.PluginContext{
			DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
				JSONData: jsonBytes,
			},
		},
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result.Status != backend.HealthStatusOk && result.Status != backend.HealthStatusError {
		t.Errorf("Unexpected health status: %v", result.Status)
	}
}

func TestQuery_UnmarshalError(t *testing.T) {
	ds := plugin.KafkaDatasource{}
	badQuery := backend.DataQuery{JSON: []byte(`not-json`)}
	resp, _ := ds.QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{badQuery}})
	if resp == nil {
		t.Fatal("Expected a response, got nil")
	}
}

func TestSubscribeStream_EmptyTopic(t *testing.T) {
	ds := plugin.KafkaDatasource{}
	data, _ := json.Marshal(map[string]interface{}{})
	resp, err := ds.SubscribeStream(context.Background(), &backend.SubscribeStreamRequest{Data: data})
	if err == nil {
		t.Error("Expected error for empty topic")
	}
	if resp.Status != backend.SubscribeStreamStatusPermissionDenied {
		t.Errorf("Expected permission denied, got %v", resp.Status)
	}
}
