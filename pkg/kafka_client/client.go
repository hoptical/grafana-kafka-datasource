package kafka_client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafka2 "github.com/segmentio/kafka-go"
)

const MAX_EARLIEST int64 = 100
const consumerGroupID = "kafka-datasource"

type Options struct {
	BootstrapServers   string `json:"bootstrapServers"`
	SecurityProtocol   string `json:"securityProtocol"`
	SaslMechanisms     string `json:"saslMechanisms"`
	SaslUsername       string `json:"saslUsername"`
	SaslPassword       string `json:"saslPassword"`
	HealthcheckTimeout int32  `json:"healthcheckTimeout"`
	Debug              string `json:"debug"`
}

type KafkaClient struct {
	Consumer           *kafka.Consumer
	dialer             *kafka2.Dialer
	reader             *kafka2.Reader
	BootstrapServers   string
	TimestampMode      string
	SecurityProtocol   string
	SaslMechanisms     string
	SaslUsername       string
	SaslPassword       string
	Debug              string
	HealthcheckTimeout int32
}

type KafkaMessage struct {
	Value     map[string]float64
	Timestamp time.Time
	Offset    kafka.Offset
}

func NewKafkaClient(options Options) KafkaClient {
	client := KafkaClient{
		BootstrapServers:   options.BootstrapServers,
		SecurityProtocol:   options.SecurityProtocol,
		SaslMechanisms:     options.SaslMechanisms,
		SaslUsername:       options.SaslUsername,
		SaslPassword:       options.SaslPassword,
		Debug:              options.Debug,
		HealthcheckTimeout: options.HealthcheckTimeout,
	}
	return client
}

func (client *KafkaClient) consumerInitialize() {
	var err error

	config := kafka.ConfigMap{
		"bootstrap.servers":  client.BootstrapServers,
		"group.id":           "kafka-datasource",
		"enable.auto.commit": "false",
	}

	if client.SecurityProtocol != "" {
		config.SetKey("security.protocol", client.SecurityProtocol)
	}
	if client.SaslMechanisms != "" {
		config.SetKey("sasl.mechanisms", client.SaslMechanisms)
	}
	if client.SaslMechanisms != "" {
		config.SetKey("sasl.username", client.SaslUsername)
	}
	if client.SaslMechanisms != "" {
		config.SetKey("sasl.password", client.SaslPassword)
	}
	if client.Debug != "" {
		config.SetKey("debug", client.Debug)
	}

	client.Consumer, err = kafka.NewConsumer(&config)

	if err != nil {
		panic(err)
	}

	// *********** kafka-go ************
	dialer := &kafka2.Dialer{
		Timeout: 10 * time.Second,
	}

	client.dialer = dialer
}

func (client *KafkaClient) newReader(topic string, partition int, offset int64) *kafka2.Reader {
	return kafka2.NewReader(kafka2.ReaderConfig{
		Brokers:     strings.Split(client.BootstrapServers, ","),
		GroupID:     consumerGroupID,
		Topic:       topic,
		Partition:   partition,
		Dialer:      client.dialer,
		StartOffset: offset,
	})
}

func (client *KafkaClient) TopicAssign(topic string, partition int32, autoOffsetReset string,
	timestampMode string) {
	client.consumerInitialize()
	client.TimestampMode = timestampMode
	var err error
	var offset int64
	var high, low int64
	switch autoOffsetReset {
	case "latest":
		offset = int64(kafka.OffsetEnd)
	case "earliest":
		low, high, err = client.Consumer.QueryWatermarkOffsets(topic, partition, 100) // low=300, high=800
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

	// ********** kafka-go ************
	switch autoOffsetReset {
	case "latest":
		offset = kafka2.LastOffset
	case "earliest":
		// We have to connect to the partition leader to read offsets
		conn, err := client.dialer.DialLeader(context.Background(), "tcp", client.BootstrapServers, topic, int(partition))
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		low, high, err = conn.ReadOffsets()
		if err != nil {
			panic(err)
		}

		if high-low > MAX_EARLIEST {
			offset = high - MAX_EARLIEST
		} else {
			offset = low
		}
	default:
		offset = kafka2.LastOffset
	}

	client.reader = client.newReader(topic, int(partition), offset)
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

func (client *KafkaClient) ConsumerPull2() (KafkaMessage, error) {
	var message KafkaMessage

	msg, err := client.reader.ReadMessage(context.Background())
	if err != nil {
		return message, fmt.Errorf("error reading message from Kafka: %v", err)
	}

	if err := json.Unmarshal(msg.Value, &message.Value); err != nil {
		return message, fmt.Errorf("error unmarshalling message: %w", err)
	}

	message.Offset = kafka.Offset(msg.Offset)
	message.Timestamp = msg.Time

	return message, nil
}

func (client *KafkaClient) HealthCheck() error {
	client.consumerInitialize()

	_, err := client.Consumer.GetMetadata(nil, true, int(client.HealthcheckTimeout))
	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTransport {
			return err
		}
	}

	// *********** kafka-go *********
	conn, err := client.dialer.Dial("tcp", client.BootstrapServers)
	if err != nil {
		return fmt.Errorf("error connecting to Kafka: %w", err)
	}
	defer conn.Close()

	if _, err = conn.ReadPartitions(); err != nil {
		return fmt.Errorf("error reading partitions: %w", err)
	}

	return nil
}

func (client *KafkaClient) Dispose() {
	client.Consumer.Close()
	client.reader.Close()
}
