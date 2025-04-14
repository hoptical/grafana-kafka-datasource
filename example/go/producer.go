package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type Data struct {
	Value1 float64 `json:"value1"`
	Value2 float64 `json:"value2"`
}

func main() {
	// Define command line flags with default values
	brokerURL := flag.String("broker", "localhost:9092", "Kafka broker URL")
	topic := flag.String("topic", "test", "Kafka topic name")
	sleepTime := flag.Int("interval", 500, "Sleep interval in milliseconds")
	flag.Parse()

	// Configure the writer
	w := &kafka.Writer{
		Addr:     kafka.TCP(*brokerURL),
		Topic:    *topic,
		Balancer: &kafka.LeastBytes{},
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
