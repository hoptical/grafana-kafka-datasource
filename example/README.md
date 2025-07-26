# Sample Producer

In this folder, there are simple producers for different langaues that generate json values in Kafka.

## Go

### Requirements
- Go 1.17 or later
- [kafka-go](github.com/segmentio/kafka-go) v0.4.47 or later

### Usage

```bash
cd example/go
go get github.com/segmentio/kafka-go
```

Then, run the producer:
```bash
go run producer.go -broker <broker> -topic <topic> -interval <interval in milliseconds between producing messages> -num-partitions <number of partitions when creating the topic>
```

> Note: The producer will create the topic if it does not exist.

For example, to produce messages to `test` topic on `localhost:9094` with 3 partitions:

```bash
go run producer.go -broker localhost:9094 -topic test -interval 500 -num-partitions 3
```
## Python

The Python code will produces JSON messages to the Kafka topic `test` every 500 milliseconds.

### Requirements

- Python 3.7 or later
- confluent-kafka==2.9.0

### Usage

```bash
python producer.py
```
