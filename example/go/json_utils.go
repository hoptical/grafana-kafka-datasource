package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// EncodeJSONMessage encodes a message using JSON
func EncodeJSONMessage(shape string, counter int, hostName, hostIP string, valuesOffset float64) ([]byte, error) {
	// Create sample data (flat, nested, or list)
	value1 := valuesOffset - rand.Float64()
	value2 := valuesOffset + rand.Float64()

	var payload interface{}
	switch shape {
	case "flat":
		payload = map[string]interface{}{
			"host.name":        hostName,
			"host.ip":          hostIP,
			"metrics.cpu.load": value1,
			"metrics.cpu.temp": 60.0 + rand.Float64()*10.0,
			"metrics.mem.used": 1000 + rand.Intn(2000),
			"metrics.mem.free": 8000 + rand.Intn(2000),
			"value1":           value1,
			"value2":           value2,
			"tags":             []string{"prod", "edge"},
		}
	case "nested":
		payload = map[string]interface{}{
			"host": map[string]interface{}{
				"name": hostName,
				"ip":   hostIP,
			},
			"metrics": map[string]interface{}{
				"cpu": map[string]interface{}{
					"load": value1,
					"temp": 60.0 + rand.Float64()*10.0,
				},
				"mem": map[string]interface{}{
					"used": 1000 + rand.Intn(2000),
					"free": 8000 + rand.Intn(2000),
				},
			},
			"value1": value1,
			"value2": value2,
			"tags":   []string{"prod", "edge"},
			"alerts": []interface{}{
				map[string]interface{}{
					"type":     "cpu_high",
					"severity": "warning",
					"value":    value1 * 100,
				},
				map[string]interface{}{
					"type":     "mem_low",
					"severity": "info",
					"value":    value2 * 50,
				},
			},
			"processes": []string{"nginx", "mysql", "redis"},
		}
	case "list":
		// JSON that starts with an array containing multiple records
		payload = []interface{}{
			map[string]interface{}{
				"id":   counter,
				"type": "metric",
				"host": map[string]interface{}{
					"name": hostName,
					"ip":   hostIP,
				},
				"value":     value1,
				"timestamp": time.Now().Unix(),
			},
			map[string]interface{}{
				"id":   counter + 1,
				"type": "metric",
				"host": map[string]interface{}{
					"name": hostName,
					"ip":   hostIP,
				},
				"value":     value2,
				"timestamp": time.Now().Unix(),
			},
			map[string]interface{}{
				"id":   counter + 1000,
				"type": "event",
				"host": map[string]interface{}{
					"name": hostName,
					"ip":   hostIP,
				},
				"value":     value1 * 1.5,
				"timestamp": time.Now().Unix(),
			},
			map[string]interface{}{
				"id":   counter + 1001,
				"type": "event",
				"host": map[string]interface{}{
					"name": hostName,
					"ip":   hostIP,
				},
				"value":     value2 * 0.8,
				"timestamp": time.Now().Unix(),
			},
			map[string]interface{}{
				"id":        counter + 2000,
				"type":      "log",
				"message":   fmt.Sprintf("Sample log entry #%d", counter),
				"level":     "info",
				"tags":      []string{"prod", "edge"},
				"timestamp": time.Now().Unix(),
			},
			map[string]interface{}{
				"id":        counter + 2001,
				"type":      "log",
				"message":   fmt.Sprintf("Sample log entry #%d (batch)", counter),
				"level":     "debug",
				"tags":      []string{"prod", "edge", "batch"},
				"timestamp": time.Now().Unix(),
			},
		}
	default:
		// Handle unknown shape
		return nil, fmt.Errorf("unknown shape %q. Valid options: nested, flat, list", shape)
	}

	// Convert payload to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshaling JSON: %w", err)
	}

	return jsonData, nil
}
