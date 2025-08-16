

# Kafka Datasource for Grafana

[![License](https://img.shields.io/github/license/hoptical/grafana-kafka-datasource)](LICENSE)
[![CI](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/ci.yml/badge.svg)](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/ci.yml)
[![Release](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/release.yml/badge.svg)](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/release.yml)
[![Go Version](https://img.shields.io/badge/go-1.24.1-blue?logo=go)](https://golang.org/doc/go1.24)
[![Grafana v10.2+](https://img.shields.io/badge/grafana-10.2%2B-orange?logo=grafana)](https://grafana.com)

---

**Visualize real-time Kafka data in Grafana dashboards.**

---

## Why Kafka Datasource?

- **Live streaming:** Monitor Kafka topics in real time.
- **Flexible queries:** Select topics, partitions, offsets, and timestamp modes.
- **Rich JSON support:** Handles flat, nested, and array data.
- **Secure:** SASL authentication & SSL/TLS encryption.
- **Easy setup:** Install and configure in minutes.

## How It Works

This plugin connects your Grafana instance directly to Kafka brokers, allowing you to query, visualize, and explore streaming data with powerful time-series panels and dashboards.

## Requirements

- Apache Kafka v0.9+
- Grafana v10.2+

> Note: This is a backend plugin, so the Grafana server should have access to the Kafka broker.

## Features

- Real-time monitoring of Kafka topics
- Query all or specific partitions
- Autocomplete for topic names
- Flexible offset options (latest, last N, earliest)
- Timestamp modes (Kafka event time, dashboard received time)
- Advanced JSON support (flat, nested, arrays, mixed types)
- Kafka authentication (SASL) & encryption (SSL/TLS)

## Installation

### Via grafana-cli
```bash
grafana-cli plugins install hamedkarbasi93-kafka-datasource
```

### Via zip file
Download the [latest release](https://github.com/hoptical/grafana-kafka-datasource/releases/latest) and unpack it into your Grafana plugins directory (default: `/var/lib/grafana/plugins`).

### Provisioning
You can automatically configure the Kafka datasource using Grafana's provisioning feature. For a ready-to-use template and configuration options, refer to `provisioning/datasources/datasource.yaml` in this repository.

## Usage

### Configuration
1. Add a new data source in Grafana and select "Kafka Datasource".
2. Configure connection settings:
	 - **Broker address** (e.g. `localhost:9094` or `kafka:9092`)
	 - **Authentication** (SASL, SSL/TLS, optional)
	 - **Timeout settings** (default: two seconds)

### Build the Query
1. Create a new dashboard panel in Grafana.
2. Select your Kafka data source.
3. Configure the query:
	 - **Topic**: Enter or select your Kafka topic (autocomplete available).
	 - **Fetch Partitions**: Click to retrieve available partitions.
	 - **Partition**: Choose a specific partition or "all" for all partitions.
	 - **Offset Reset**:
		 - `latest`: Only new messages
		 - `last N messages`: Start from the most recent N messages (set N in the UI)
		 - `earliest`: Start from the oldest message
	 - **Timestamp Mode**: Choose between Kafka event time or dashboard received time.

**Tip:** Numeric fields become time series, string fields are labels, arrays and nested objects are automatically flattened for visualization.

## Supported JSON Structures

- Flat objects
- Nested objects (flattened)
- Top-level arrays
- Mixed types

**Examples:**

Simple flat object:
```json
{
	"temperature": 23.5,
	"humidity": 65.2,
	"status": "active"
}
```

Nested object (flattened as `user.name`, `user.age`, `settings.theme`):
```json
{
	"user": {
		"name": "John Doe",
		"age": 30
	},
	"settings": {
		"theme": "dark"
	}
}
```

Top-level array (flattened as `item_0.id`, `item_0.value`, `item_1.id`, etc.):
```json
[
	{"id": 1, "value": 10.5},
	{"id": 2, "value": 20.3}
]
```

## Limitations

- Max flattening depth: 5
- Max fields per message: 1000
- Protobuf/AVRO not yet supported

## Live Demo

![Kafka dashboard](https://raw.githubusercontent.com/hoptical/grafana-kafka-datasource/86ea8d360bfd67cfed41004f80adc39219983210/src/img/graph.gif)

## Sample Data Generator

Want to test the plugin? Use our [Go sample producer](example/go/producer.go) to generate realistic Kafka messages:

```bash
go run example/go/producer.go -broker localhost:9094 -topic test -interval 500 -num-partitions 3 -shape nested
```

Supports flat, nested, and array JSON payloads. See [example/README.md](example/README.md) for details.

## FAQ & Troubleshooting

- **Can I use this with any Kafka broker?** Yes, supports Apache Kafka v0.9+ and compatible brokers.
- **Does it support secure connections?** Yes, SASL and SSL/TLS are supported.
- **What JSON formats are supported?** Flat, nested, arrays, mixed types.
- **How do I generate test data?** Use the included Go or Python producers.
- **Where do I find more help?** See this README or open an issue.

## Documentation & Links

- [Sample Producers](example/README.md)
- [Changelog](CHANGELOG.md)
- [Contribution Guidelines](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)

---

## Support & Community

If you find this plugin useful, please consider giving it a ⭐ on GitHub or supporting development:

[![Buy Me a Coffee](https://img.shields.io/badge/buy%20me%20a%20coffee-donate-yellow?logo=buy-me-a-coffee&style=flat)](https://www.buymeacoffee.com/hoptical)

For more information, see the documentation files above or open an issue/PR.
