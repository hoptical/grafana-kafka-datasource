package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
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
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("failed to close connection: %v\n", err)
		}
	}()

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
	defer func() {
		if err := controllerConn.Close(); err != nil {
			fmt.Printf("failed to close controller connection: %v\n", err)
		}
	}()

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

func waitForTopicLeaders(brokerURL, topic string, partitions int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		allReady := true
		for p := 0; p < partitions; p++ {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return fmt.Errorf("timed out waiting for leaders for topic %s", topic)
			}
			attemptTimeout := 2 * time.Second
			if remaining < attemptTimeout {
				attemptTimeout = remaining
			}
			ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
			conn, err := kafka.DialLeader(ctx, "tcp", brokerURL, topic, p)
			cancel()
			if err != nil {
				allReady = false
				break
			}
			_ = conn.Close()
		}

		if allReady {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for leaders for topic %s", topic)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// batchSize returns the kafka.Writer BatchSize for a given target rate.
// In benchmark mode (rate > 0) we flush every message to avoid the default
// 1-second BatchTimeout blocking WriteMessages at low rates.
func batchSize(rate int) int {
	if rate > 0 {
		return 1
	}
	return 100 // kafka-go default
}

// batchTimeout returns the kafka.Writer BatchTimeout for a given target rate.
func batchTimeout(rate int) time.Duration {
	if rate > 0 {
		return time.Millisecond
	}
	return time.Second // kafka-go default
}

func main() {
	// Define command line flags with default values
	brokerURL := flag.String("broker", "localhost:9094", "Kafka broker URL")
	topic := flag.String("topic", "test", "Kafka topic name")
	sleepTime := flag.Int("interval", 500, "Sleep interval in milliseconds (ignored when -rate is set)")
	numPartitions := flag.Int("num-partitions", 1, "Number of partitions when creating topic")
	valuesOffset := flag.Float64("values-offset", 1.0, "Offset for the values")
	connectTimeout := flag.Int("connect-timeout", 5000, "Broker connect timeout in milliseconds")
	leaderWaitTimeout := flag.Int("leader-wait-timeout", 30000, "Timeout in milliseconds to wait for topic leader election")
	shape := flag.String("shape", "nested", "Payload shape: nested, flat, or list")
	format := flag.String("format", "json", "Message format: json, avro, or protobuf")
	schemaRegistryURL := flag.String("schema-registry", "", "Schema registry URL (for Avro/Protobuf with schema registry)")
	schemaRegistryUser := flag.String("schema-registry-user", "", "Schema registry username (optional)")
	schemaRegistryPass := flag.String("schema-registry-pass", "", "Schema registry password (optional)")
	keyFormat := flag.String("key-format", "string", "Message key format: none, string, json, or binary (raw bytes, displayed as base64 in Grafana)")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	transactionalID := flag.String("transactional-id", "", "Transactional producer ID; when set, each message is produced in its own transaction")
	// Benchmark flags
	rate := flag.Int("rate", 0, "Target messages/second using a ticker (0 = use -interval instead). Overrides -interval.")
	msgSize := flag.Int("msg-size", 0, "Pad message payload to approximately this many bytes using a _padding field (0 = no padding)")
	duration := flag.Int("duration", 0, "Stop after this many seconds (0 = run forever)")
	reportInterval := flag.Int("report-interval", 10, "Print throughput stats every N seconds (benchmark mode only; requires -rate > 0)")
	flag.Parse()

	// Validate format
	if *format != "json" && *format != "avro" && *format != "protobuf" {
		fmt.Printf("Error: Invalid format %q. Valid options: json, avro, protobuf\n", *format)
		os.Exit(1)
	}

	// Validate key format
	if *keyFormat != "none" && *keyFormat != "string" && *keyFormat != "json" && *keyFormat != "binary" {
		fmt.Printf("Error: Invalid key-format %q. Valid options: none, string, json, binary\n", *keyFormat)
		os.Exit(1)
	}

	// Validate Avro compatibility
	if (*format == "avro" || *format == "protobuf") && *shape == "list" {
		fmt.Printf("Error: %s format does not support 'list' shape. Use 'flat' or 'nested' shapes.\n", *format)
		os.Exit(1)
	}

	// Create topic if it doesn't exist
	timeout := time.Duration(*connectTimeout) * time.Millisecond
	if err := createTopicIfNotExists(*brokerURL, *topic, *numPartitions, timeout); err != nil {
		fmt.Printf("Error: Failed to create/verify topic: %v\n", err)
		os.Exit(1)
	}
	leaderTimeout := time.Duration(*leaderWaitTimeout) * time.Millisecond
	if err := waitForTopicLeaders(*brokerURL, *topic, *numPartitions, leaderTimeout); err != nil {
		fmt.Printf("Error: topic leader not ready: %v\n", err)
		os.Exit(1)
	}

	// Configure non-transactional writer by default.
	var w *kafka.Writer
	var txProducer *transactionalProducer
	if *transactionalID == "" {
		w = &kafka.Writer{
			Addr:  kafka.TCP(*brokerURL),
			Topic: *topic,
			// In benchmark mode, flush every message immediately instead of waiting for
			// kafka-go's default 1-second BatchTimeout. Without this, WriteMessages blocks
			// for up to 1 second per call regardless of the -rate flag, capping effective
			// throughput at 1 msg/s for low-rate tests.
			BatchSize:    batchSize(*rate),
			BatchTimeout: batchTimeout(*rate),
		}

		defer func() {
			if err := w.Close(); err != nil {
				fmt.Printf("failed to close writer: %v\n", err)
			}
		}()
	} else {
		producer, err := newTransactionalProducer([]string{*brokerURL}, *transactionalID)
		if err != nil {
			fmt.Printf("Error creating transactional producer: %v\n", err)
			os.Exit(1)
		}
		txProducer = producer
		defer func() {
			if err := txProducer.Close(); err != nil {
				fmt.Printf("failed to close transactional producer: %v\n", err)
			}
		}()
		fmt.Printf("Transactional mode enabled with transactional.id=%s\n", *transactionalID)
	}

	counter := 1
	hostName := "srv-01"
	hostIP := "127.0.0.1"

	// Benchmark mode: ticker-based rate control and optional duration limit.
	var ticker *time.Ticker
	var deadline time.Time
	var benchStart time.Time
	var benchProduced int
	var benchDropped int
	var nextReport time.Time
	isBenchMode := *rate > 0

	if isBenchMode {
		tickInterval := time.Duration(float64(time.Second) / float64(*rate))
		ticker = time.NewTicker(tickInterval)
		defer ticker.Stop()
		benchStart = time.Now()
		nextReport = benchStart.Add(time.Duration(*reportInterval) * time.Second)
		if *duration > 0 {
			deadline = benchStart.Add(time.Duration(*duration) * time.Second)
		}
	}

	for {
		// In benchmark mode, block on the ticker instead of sleeping at the end.
		if isBenchMode {
			<-ticker.C
			if *duration > 0 && time.Now().After(deadline) {
				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[BENCH] done produced=%d dropped=%d elapsed=%.1fs avg_rate=%.1f msg/s\n",
					benchProduced, benchDropped, elapsed, float64(benchProduced)/elapsed)
				break
			}
		}
		// Create sample data (flat, nested, or list)
		// Periodically set value1/value2 to null to reproduce the bug.
		rawValue1 := *valuesOffset - rand.Float64()
		rawValue2 := *valuesOffset + rand.Float64()
		var value1 interface{} = rawValue1
		var value2 interface{} = rawValue2
		if counter%7 == 0 {
			value1 = nil
		}
		if counter%11 == 0 {
			value2 = nil
		}

		var messageData []byte
		var err error

		// ts_ms is embedded in every message for end-to-end latency measurement.
		// In Grafana: create a Transform "Add field from calculation": now() - ts_ms
		// to compute latency in milliseconds.
		tsMs := time.Now().UnixMilli()

		// Create payload based on shape
		var payload interface{}
		switch *shape {
		case "flat":
			// Use appropriate field names based on format
			if *format == "avro" || *format == "protobuf" {
				// Avro requires valid field names (no dots)
				payload = map[string]interface{}{
					"ts_ms":            tsMs,
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
				// JSON can use dotted field names
				payload = map[string]interface{}{
					"ts_ms":            tsMs,
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
				"ts_ms": tsMs,
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
						"value":    rawValue1 * 100,
					},
					map[string]interface{}{
						"type":     "mem_low",
						"severity": "info",
						"value":    rawValue2 * 50,
					},
				},
				"processes": []string{"nginx", "mysql", "redis"},
			}
		case "list":
			// Top-level array of records
			payload = []interface{}{
				map[string]interface{}{
					"ts_ms": tsMs,
					"id":    counter,
					"type":  "metric",
					"host": map[string]interface{}{
						"name": hostName,
						"ip":   hostIP,
					},
					"value":     value1,
					"timestamp": time.Now().Unix(),
				},
				map[string]interface{}{
					"ts_ms": tsMs,
					"id":    counter + 1,
					"type":  "metric",
					"host": map[string]interface{}{
						"name": hostName,
						"ip":   hostIP,
					},
					"value":     value2,
					"timestamp": time.Now().Unix(),
				},
				map[string]interface{}{
					"ts_ms": tsMs,
					"id":    counter + 1000,
					"type":  "event",
					"host": map[string]interface{}{
						"name": hostName,
						"ip":   hostIP,
					},
					"message":   "Sample log entry",
					"timestamp": time.Now().Unix(),
				},
			}
		default:
			// Handle unknown shape
			fmt.Printf("Error: Unknown shape %q. Valid options: nested, flat, list\n", *shape)
			os.Exit(1)
		}

		// Encode based on format
		switch *format {
		case "json":
			messageData, err = json.Marshal(payload)
		case "avro":
			if *verbose {
				fmt.Printf("[PRODUCER DEBUG] Using Avro format for message #%d\n", counter)
			}
			messageData, err = EncodeAvroMessage(*shape, payload, *schemaRegistryURL, *schemaRegistryUser, *schemaRegistryPass, *topic, *verbose)
		case "protobuf":
			if *verbose {
				fmt.Printf("[PRODUCER DEBUG] Using Protobuf format for message #%d\n", counter)
			}
			messageData, err = EncodeProtobufMessage(*shape, payload, *schemaRegistryURL, *schemaRegistryUser, *schemaRegistryPass, *topic, *verbose)
		}

		if err != nil {
			fmt.Printf("Error encoding message: %v\n", err)
			continue
		}

		// Pad JSON messages to the requested byte size by injecting a _padding field.
		// We re-marshal after padding so the final size is accurate. Non-JSON formats
		// are not padded because their schemas are pre-compiled.
		if *msgSize > 0 && *format == "json" && len(messageData) < *msgSize {
			padLen := *msgSize - len(messageData) - len(`,"_padding":""}`) - 1
			if padLen > 0 {
				paddedPayload, ok := payload.(map[string]interface{})
				if ok {
					paddedPayload["_padding"] = strings.Repeat("X", padLen)
					messageData, err = json.Marshal(paddedPayload)
					if err != nil {
						fmt.Printf("Error encoding padded message: %v\n", err)
						continue
					}
				}
			}
		}

		if *verbose {
			fmt.Printf("[PRODUCER DEBUG] Final message length: %d bytes, format: %s\n", len(messageData), *format)
			if len(messageData) > 0 && len(messageData) <= 20 {
				fmt.Printf("[PRODUCER DEBUG] Message content: %x\n", messageData)
			} else if len(messageData) > 20 {
				fmt.Printf("[PRODUCER DEBUG] Message preview: %x...\n", messageData[:20])
			}
		}

		// Build message key based on key format
		var msgKey []byte
		switch *keyFormat {
		case "string":
			msgKey = []byte(fmt.Sprintf("key-%d", counter))
		case "json":
			keyObj := map[string]interface{}{
				"serverId": hostName,
				"region":   "us-east",
				"counter":  counter,
			}
			msgKey, err = json.Marshal(keyObj)
			if err != nil {
				fmt.Printf("Error encoding JSON key: %v\n", err)
				continue
			}
		case "binary":
			// Send raw 8-byte big-endian binary key (no pre-encoding).
			// The plugin will base64-encode these bytes for display.
			binKey := make([]byte, 8)
			binary.BigEndian.PutUint64(binKey, uint64(counter))
			msgKey = binKey
		case "none":
			msgKey = nil
		}

		// Produce message
		if txProducer != nil {
			err = txProducer.WriteMessage(context.Background(), *topic, msgKey, messageData)
		} else {
			err = w.WriteMessages(context.Background(),
				kafka.Message{
					Key:   msgKey,
					Value: messageData,
				},
			)
		}
		if err != nil {
			fmt.Printf("Error writing message: %v\n", err)
			if txProducer != nil {
				if isRetryableTransactionalError(err) {
					fmt.Printf("Transient transactional write error; retrying next loop iteration\n")
					time.Sleep(300 * time.Millisecond)
					continue
				}
				if requiresTransactionalProducerRecreation(err) {
					fmt.Printf("Transactional producer state became invalid; recreating producer\n")
					_ = txProducer.Close()
					recreated, recreateErr := newTransactionalProducer([]string{*brokerURL}, *transactionalID)
					if recreateErr != nil {
						fmt.Printf("Error recreating transactional producer: %v\n", recreateErr)
						os.Exit(1)
					}
					txProducer = recreated
					time.Sleep(300 * time.Millisecond)
					continue
				}
			}
			os.Exit(1)
		}

		if isBenchMode {
			if err == nil {
				benchProduced++
			} else {
				benchDropped++
			}
			// Periodic throughput report.
			if time.Now().After(nextReport) {
				elapsed := time.Since(benchStart).Seconds()
				fmt.Printf("[BENCH] produced=%d dropped=%d elapsed=%.1fs rate=%.1f msg/s bytes_per_msg=%d\n",
					benchProduced, benchDropped, elapsed,
					float64(benchProduced)/elapsed, len(messageData))
				nextReport = time.Now().Add(time.Duration(*reportInterval) * time.Second)
			}
		} else {
			if *verbose {
				fmt.Printf("Sample #%d produced to topic %s (shape=%s, format=%s, key-format=%s)!\n", counter, *topic, *shape, *format, *keyFormat)
			} else {
				fmt.Printf("Sample #%d produced to topic %s\n", counter, *topic)
			}
			time.Sleep(time.Duration(*sleepTime) * time.Millisecond)
		}
		counter++
	}
}
