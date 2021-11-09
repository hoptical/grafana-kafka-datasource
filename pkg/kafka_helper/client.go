package kafka_helper

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	Consumer *kafka.Consumer
}

func (client *KafkaClient) ConsumerInitialize() {
	var err error
	client.Consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "kafka-datasource",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		panic(err)
	}
}

func (client *KafkaClient) TopicAssign(topic string, partition int32, offset int64) {
	var err error
	topic_partition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset),
		//Offset:   kafka.OffsetEnd,
		Metadata: new(string),
		Error:    err,
	}
	partitions := []kafka.TopicPartition{topic_partition}
	err = client.Consumer.Assign(partitions)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Topic Assigned!\n")
}

type Data struct {
	Value1 int64
}

func (client *KafkaClient) ConsumerPull() (Data, kafka.Event) {
	var data Data
	ev := client.Consumer.Poll(100)
	if ev == nil {
		return data, ev
	}

	switch e := ev.(type) {
	case *kafka.Message:
		json.Unmarshal([]byte(e.Value), &data)
		client.Consumer.Commit()
	case kafka.Error:
		// Errors should generally be considered
		// informational, the client will try to
		// automatically recover.
		// But in this example we choose to terminate
		// the application if all brokers are down.
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		if e.Code() == kafka.ErrAllBrokersDown {
			panic(e)
		}
	default:
	}
	return data, ev
}
