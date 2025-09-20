package main

import (
	"context"
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
	shape := flag.String("shape", "nested", "Payload shape: nested, flat, or list")
	format := flag.String("format", "json", "Message format: json or avro")
	schemaRegistryURL := flag.String("schema-registry", "", "Schema registry URL (for Avro with schema registry)")
	schemaRegistryUser := flag.String("schema-registry-user", "", "Schema registry username")
	schemaRegistryPass := flag.String("schema-registry-pass", "", "Schema registry password")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	flag.Parse()

	// Validate format
	if *format != "json" && *format != "avro" {
		fmt.Printf("Error: Invalid format %q. Valid options: json, avro\n", *format)
		os.Exit(1)
	}

	// Validate Avro compatibility
	if *format == "avro" && *shape == "list" {
		fmt.Printf("Error: Avro format does not support 'list' shape. Use 'flat' or 'nested' shapes with Avro.\n")
		os.Exit(1)
	}

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
		var messageData []byte
		var err error

		if *format == "json" {
			messageData, err = EncodeJSONMessage(*shape, counter, hostName, hostIP, *valuesOffset)
		} else if *format == "avro" {
			if *verbose {
				fmt.Printf("[PRODUCER DEBUG] Using Avro format for message #%d\n", counter)
			}
			// For Avro, we need to create the data structure first, then encode it
			var payload interface{}
			value1 := *valuesOffset - rand.Float64()
			value2 := *valuesOffset + rand.Float64()

			switch *shape {
			case "flat":
				if *format == "avro" {
					// Use Avro-compatible field names
					payload = map[string]interface{}{
						"host_name":        hostName,
						"host_ip":          hostIP,
						"metrics_cpu_load": value1,
						"metrics_cpu_temp": 60.0 + rand.Float64()*10.0,
						"metrics_mem_used": 1000 + rand.Intn(2000),
						"metrics_mem_free": 8000 + rand.Intn(2000),
						"value1":           value1,
						"value2":           value2,
						"tags":             []string{"prod", "edge"},
					}
				} else {
					// Use JSON-compatible field names
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
			}
			messageData, err = EncodeAvroMessage(*shape, payload, *schemaRegistryURL, *schemaRegistryUser, *schemaRegistryPass, *topic, *verbose)
		}

		if err != nil {
			fmt.Printf("Error encoding message: %v\n", err)
			os.Exit(1)
		}

		if *verbose {
			fmt.Printf("[PRODUCER DEBUG] Final message length: %d bytes, format: %s\n", len(messageData), *format)
			if len(messageData) > 0 && len(messageData) <= 20 {
				fmt.Printf("[PRODUCER DEBUG] Message content: %x\n", messageData)
			} else if len(messageData) > 20 {
				fmt.Printf("[PRODUCER DEBUG] Message preview: %x...\n", messageData[:20])
			}
		}

		// Produce message
		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Value: messageData,
			},
		)
		if err != nil {
			fmt.Printf("Error writing message: %v\n", err)
			os.Exit(1)
		}

		if *verbose {
			fmt.Printf("Sample #%d produced to topic %s (shape=%s, format=%s)!\n", counter, *topic, *shape, *format)
		} else {
			fmt.Printf("Sample #%d produced to topic %s\n", counter, *topic)
		}
		counter++
		time.Sleep(time.Duration(*sleepTime) * time.Millisecond)
	}
}
