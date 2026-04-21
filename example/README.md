# Sample Producer

In this folder, there are simple producers for different languages that generate Kafka messages in JSON, Avro, Protobuf, and Plaintext formats.

## Go

### Requirements

- Go 1.17 or later
- [kafka-go](github.com/segmentio/kafka-go) v0.4.47 or later
- [sarama](https://github.com/IBM/sarama) (used for transactional producer mode)

### Usage

```bash
cd example/go
go mod tidy
```

Then, run the producer:

```bash
go run producer.go \
  -broker <broker> \
  -topic <topic> \
  -interval <interval-ms> \
  -num-partitions <partitions> \
  -shape <flat|nested|list> \
  -format <json|avro|protobuf|plaintext> \
  -key-format <none|string|json|binary> \
  -schema-registry <url> \
  -schema-registry-user <username> \
  -schema-registry-pass <password> \
  -transactional-id <transactional-id>
```

> Note: The producer will create the topic if it does not exist.

#### Transactional mode

Enable Kafka transactions by setting `-transactional-id`. In this mode, each message is produced in its own transaction.

```bash
go run producer.go -broker localhost:9094 -topic txn-test -format json -transactional-id txn-producer-1
```

This is useful for validating transactional topic behavior and control-record handling in the datasource.

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

#### Plaintext Format

Plaintext mode emits a human-readable line per message. This is useful for demonstrating the datasource's raw-byte mode side by side with structured formats.

```bash
go run producer.go -broker localhost:9094 -topic test-plaintext -interval 1000 -shape nested -format plaintext -key-format binary
```

#### Dockerized Multi-Format Demo

The root `docker-compose.yaml` now includes a `demo` profile that runs four producer containers concurrently against the same Kafka stack:

- `showcase-json`
- `showcase-avro`
- `showcase-protobuf`
- `showcase-plaintext`

Start the showcase stack from the repository root:

```bash
docker compose --profile demo up -d
```

This profile is designed to power the provisioned multi-format dashboard in Grafana.

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

- `-format <json|avro|protobuf|plaintext>`: Message format (default: json)
- `-key-format <none|string|json|binary>`: Message key format (default: string). Use `binary` to send raw 8-byte binary keys that the plugin will encode as base64 for display.
- `-values-offset <float>`: Offset for generated values
- `-connect-timeout <ms>`: Broker connect timeout
- `-leader-wait-timeout <ms>`: Timeout waiting for topic leader election after topic creation
- `-interval <ms>`: Delay between produced messages
- `-num-partitions <n>`: Number of partitions to create when topic does not exist
- `-shape <flat|nested|list>`: Payload shape (list is JSON-only)
- `-verbose`: Enable verbose logging for debugging
- `-schema-registry <url>`: Schema registry URL for Avro schema management (e.g., <http://localhost:8081>)
- `-schema-registry-user <user>`: Schema Registry basic-auth username (optional)
- `-schema-registry-pass <pass>`: Schema Registry basic-auth password (optional)
- `-transactional-id <id>`: Enable transactional producer mode

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
