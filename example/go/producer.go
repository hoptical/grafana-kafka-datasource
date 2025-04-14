package main

import (
	"context"
	"encoding/json"
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
	// Configure the writer
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test",
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

		fmt.Printf("Sample #%d produced!\n", counter)
		counter++
		time.Sleep(500 * time.Millisecond)
	}
}
