package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Data is kept for reference but we generate dynamic maps for nested/flat payloads.
type Data struct {
	Value1 float64 `json:"value1"`
	Value2 float64 `json:"value2"`
}

func createTopicIfNotExists(brokerURL, topic string, partitions int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", brokerURL)
	if err != nil {
		return fmt.Errorf("failed to dial leader: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctx2, cancel2 := context.WithTimeout(context.Background(), timeout)
	defer cancel2()
	controllerConn, err := kafka.DialContext(ctx2, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

func main() {
	// Define command line flags with default values
	brokerURL := flag.String("broker", "localhost:9094", "Kafka broker URL")
	topic := flag.String("topic", "test", "Kafka topic name")
	sleepTime := flag.Int("interval", 500, "Sleep interval in milliseconds")
	numPartitions := flag.Int("num-partitions", 1, "Number of partitions when creating topic")
	valuesOffset := flag.Float64("values-offset", 1.0, "Offset for the values")
	connectTimeout := flag.Int("connect-timeout", 5000, "Broker connect timeout in milliseconds")
	shape := flag.String("shape", "nested", "Payload shape: nested or flat")
	flag.Parse()

	// Create topic if it doesn't exist
	timeout := time.Duration(*connectTimeout) * time.Millisecond
	if err := createTopicIfNotExists(*brokerURL, *topic, *numPartitions, timeout); err != nil {
		fmt.Printf("Error: Failed to create/verify topic: %v\n", err)
		os.Exit(1)
	}

	// Configure the writer
	w := &kafka.Writer{
		Addr:  kafka.TCP(*brokerURL),
		Topic: *topic,
	}

	defer w.Close()

	counter := 1
	hostName := "srv-01"
	hostIP := "127.0.0.1"

	for {
		// Create sample data (flat or nested)
		value1 := *valuesOffset - rand.Float64()
		value2 := *valuesOffset + rand.Float64()

		var payload map[string]interface{}
		switch *shape {
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
			}
		default:
			// Handle unknown shape
			fmt.Printf("Error: Unknown shape %q\n", *shape)
			os.Exit(1)
		}

		// Convert payload to JSON
		jsonData, err := json.Marshal(payload)
		if err != nil {
			fmt.Printf("Error marshaling JSON: %v\n", err)
			os.Exit(1)
		}

		// Produce message
		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Value: jsonData,
			},
		)
		if err != nil {
			fmt.Printf("Error writing message: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Sample #%d produced to topic %s (shape=%s)!\n", counter, *topic, *shape)
		counter++
		time.Sleep(time.Duration(*sleepTime) * time.Millisecond)
	}
}
