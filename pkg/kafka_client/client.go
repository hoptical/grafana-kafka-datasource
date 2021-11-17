package kafka_client

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const MAX_EARLIEST int64 = 100

type Options struct {
	BootstrapServers string `json:"bootstrapServers"`
}

type KafkaClient struct {
	Consumer         *kafka.Consumer
	BootstrapServers string
}

type KafkaMessage struct {
	Value     map[string]float64
	Timestamp time.Time
	Offset    kafka.Offset
}

func NewKafkaClient(options Options) KafkaClient {
	client := KafkaClient{BootstrapServers: options.BootstrapServers}
	return client
}

func (client *KafkaClient) ConsumerInitialize() {
	var err error
	client.Consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  client.BootstrapServers,
		"group.id":           "kafka-datasource",
		"enable.auto.commit": "false",
	})

	if err != nil {
		panic(err)
	}
}

func (client *KafkaClient) TopicAssign(topic string, partition int32, autoOffsetReset string) {
	var err error
	var offset int64
	var high, low int64
	switch autoOffsetReset {
	case "latest":
		offset = int64(kafka.OffsetEnd)
	case "earliest":
		low, high, err = client.Consumer.QueryWatermarkOffsets(topic, partition, 100)
		if err != nil {
			panic(err)
		}
		if high-low > MAX_EARLIEST {
			offset = high - MAX_EARLIEST
		} else {
			offset = low
		}
	default:
		offset = int64(kafka.OffsetEnd)
	}

	topic_partition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset),
		Metadata:  new(string),
		Error:     err,
	}
	partitions := []kafka.TopicPartition{topic_partition}
	err = client.Consumer.Assign(partitions)

	if err != nil {
		panic(err)
	}
}

func (client *KafkaClient) ConsumerPull() (KafkaMessage, kafka.Event) {
	var message KafkaMessage
	ev := client.Consumer.Poll(100)

	if ev == nil {
		return message, ev
	}

	switch e := ev.(type) {
	case *kafka.Message:
		json.Unmarshal([]byte(e.Value), &message.Value)
		message.Offset = e.TopicPartition.Offset
		message.Timestamp = e.Timestamp
	case kafka.Error:
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		if e.Code() == kafka.ErrAllBrokersDown {
			panic(e)
		}
	default:
	}
	return message, ev
}

func (client KafkaClient) HealthCheck() error {
	client.ConsumerInitialize()

	topic := ""
	_, err := client.Consumer.GetMetadata(&topic, false, 200)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTransport {
			return err
		}
	}

	return nil
}

func (client *KafkaClient) Dispose() {
	client.Consumer.Close()
}
