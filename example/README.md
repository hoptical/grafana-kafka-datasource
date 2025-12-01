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
go run producer.go -broker <broker> -topic <topic> -interval <interval ms> -num-partitions <partitions> -shape <flat|nested|list>
```

> Note: The producer will create the topic if it does not exist.

#### Example: produce nested JSON messages to `test` topic on `localhost:9094` with 3 partitions every 500ms
```bash
go run producer.go -broker localhost:9094 -topic test -interval 500 -num-partitions 3 -shape nested
```

### Supported Shapes

- `flat`: Flat key-value JSON
	```json
	{
		"host.name": "srv-01",
		"metrics.cpu.load": 0.95,
		"tags": ["prod", "edge"]
	}
	```
- `nested`: Nested JSON objects, arrays, metrics, alerts
	```json
	{
		"host": {"name": "srv-01", "ip": "127.0.0.1"},
		"metrics": {"cpu": {"load": 0.95}, "mem": {"used": 1200}},
		"alerts": [{"type": "cpu_high", "value": 95}]
	}
	```
- `list`: Top-level array of records (metrics, events, logs)
	```json
	[
		{"id": 1, "type": "metric", "value": 0.95},
		{"id": 2, "type": "event", "message": "Sample log entry"}
	]
	```

Null reproduction: All shapes periodically set fields like `value1` or `value2` to `null` to reproduce the Grafana frame type flip in realistic payloads.

All shapes are supported by the plugin and help test flattening, array handling, and nested data.

#### Other options
- `-values-offset <float>`: Offset for generated values
- `-connect-timeout <ms>`: Broker connect timeout

See the Go source for more advanced options and sample payloads.

## Python

The Python code will produces simple flat JSON messages to the Kafka topic `test` every 500 milliseconds.

### Requirements

- Python 3.7 or later
- confluent-kafka==2.9.0

### Usage

```bash
python producer.py --broker localhost:9092 --topic test --interval 0.5 --shape flat

Or the default flat messages:

```bash
python producer.py
```
```
