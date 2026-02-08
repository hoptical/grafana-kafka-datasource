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
go run producer.go -broker <broker> -topic <topic> -interval <interval ms> -num-partitions <partitions> -shape <flat|nested|list> -format <json|avro|protobuf>
```

> Note: The producer will create the topic if it does not exist.

### Message Format Examples

#### JSON Format

JSON is the default format with support for all shapes (flat, nested, list):

```bash
# Flat JSON
go run producer.go -broker localhost:9094 -topic test -interval 500 -shape flat -format json

# Nested JSON
go run producer.go -broker localhost:9094 -topic test -interval 500 -shape nested -format json

# Top-level array (JSON only)
go run producer.go -broker localhost:9094 -topic test -interval 500 -shape list -format json
```

#### Avro Format

Avro supports flat and nested shapes. Without Schema Registry, it uses inline schemas:

```bash
# Inline Avro schema
go run producer.go -broker localhost:9094 -topic test-avro -interval 1000 -shape flat -format avro

# With Schema Registry (Confluent wire format)
go run producer.go -broker localhost:9094 -topic test-avro -interval 1000 -shape flat -format avro -schema-registry http://localhost:8081
```

#### Protobuf Format

Protobuf supports flat and nested shapes. Without Schema Registry, it uses inline schemas:

```bash
# Inline Protobuf schema
go run producer.go -broker localhost:9094 -topic test-proto -interval 1000 -shape flat -format protobuf

# With Schema Registry (Confluent wire format)
go run producer.go -broker localhost:9094 -topic test-proto -interval 1000 -shape flat -format protobuf -schema-registry http://localhost:8081
```

#### Verbose Mode

Enable verbose logging for debugging any format:

```bash
go run producer.go -broker localhost:9094 -topic test -interval 1000 -format json -verbose
```

> **Schema Registry Note**: When using Schema Registry, messages are encoded in Confluent wire format with a schema ID prefix for efficient deserialization and schema evolution. Behavior on registry failures differs by format: the Avro encoder will automatically fall back to inline schema encoding when registration or retrieval fails (see `pkg/kafka_client/avro_utils.go` fallback logic); the Protobuf encoder does **not** fall back on registration failure and will return an error — Protobuf only uses inline schema when no Schema Registry URL is provided at startup (see `pkg/kafka_client/protobuf_utils.go`).

### Supported Shapes

- `flat`: Flat key-value structure (supported in JSON, Avro, and Protobuf)
  ```json
  {
    "host.name": "srv-01",
    "metrics.cpu.load": 0.95,
    "tags": ["prod", "edge"]
  }
  ```
- `nested`: Nested objects and arrays (supported in JSON, Avro, and Protobuf)
  ```json
  {
    "host": { "name": "srv-01", "ip": "127.0.0.1" },
    "metrics": { "cpu": { "load": 0.95 }, "mem": { "used": 1200 } },
    "alerts": [{ "type": "cpu_high", "value": 95 }]
  }
  ```
- `list`: Top-level array of records (metrics, events, logs) - JSON only
  ```json
  [
    { "id": 1, "type": "metric", "value": 0.95 },
    { "id": 2, "type": "event", "message": "Sample log entry" }
  ]
  ```

All shapes are supported by the plugin and help test flattening, array handling, and nested data. Avro and Protobuf formats support `flat` and `nested` shapes only.

Null reproduction: All shapes periodically set fields like `value1` or `value2` to `null` to reproduce the Grafana frame type flip in realistic payloads.

#### Other options

- `-format <json|avro|protobuf>`: Message format (default: json)
- `-values-offset <float>`: Offset for generated values
- `-connect-timeout <ms>`: Broker connect timeout
- `-verbose`: Enable verbose logging for debugging
- `-schema-registry <url>`: Schema registry URL for Avro schema management (e.g., http://localhost:8081)

See the Go source for more advanced options and sample payloads.

## Python

The Python code will produces simple flat JSON messages to the Kafka topic `test` every 500 milliseconds.

### Requirements

- Python 3.7 or later
- confluent-kafka==2.9.0

### Usage

```bash
python producer.py --broker localhost:9092 --topic test --interval 0.5 --shape flat
```

Or the default flat messages:

```bash
python producer.py
```
