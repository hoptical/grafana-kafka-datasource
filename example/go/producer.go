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

type Data struct {
	Value1 float64 `json:"value1"`
	Value2 float64 `json:"value2"`
}

func createTopicIfNotExists(brokerURL, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", brokerURL)
	if err != nil {
		return fmt.Errorf("failed to dial leader: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
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
	brokerURL := flag.String("broker", "localhost:9092", "Kafka broker URL")
	topic := flag.String("topic", "test", "Kafka topic name")
	sleepTime := flag.Int("interval", 500, "Sleep interval in milliseconds")
	numPartitions := flag.Int("num-partitions", 1, "Number of partitions when creating topic")
	flag.Parse()

	// Create topic if it doesn't exist
	if err := createTopicIfNotExists(*brokerURL, *topic, *numPartitions); err != nil {
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

	for {
		// Create sample data
		data := Data{
			Value1: rand.Float64(),
			Value2: 1.0 + rand.Float64(),
		}

		// Convert data to JSON
		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Printf("Error marshaling JSON: %v\n", err)
			continue
		}

		// Produce message
		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Value: jsonData,
			},
		)
		if err != nil {
			fmt.Printf("Error writing message: %v\n", err)
			continue
		}

		fmt.Printf("Sample #%d produced to topic %s!\n", counter, *topic)
		counter++
		time.Sleep(time.Duration(*sleepTime) * time.Millisecond)
	}
}
