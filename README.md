# Kafka Datasource for Grafana

[![License](https://img.shields.io/github/license/hoptical/grafana-kafka-datasource)](LICENSE)
[![CI](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/ci.yml/badge.svg)](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/ci.yml)
[![Release](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/release.yml/badge.svg)](https://github.com/hoptical/grafana-kafka-datasource/actions/workflows/release.yml)

The Kafka data source plugin allows you to visualize streaming Kafka data from within Grafana.

## Features

- Real-time monitoring of Kafka topics
- Query all partitions or specific partitions
- Autocomplete support for topic names
- Flexible offset options:
   - **Latest**: Stream new messages as they arrive (default)
   - **Last N messages**: Fetch the most recent N messages (user-selectable)
   - **Earliest**: Start from the oldest available message in the topic
- Timestamp modes: 
   - **Kafka Event Time** (from message metadata, default)
   - **Dashboard received time** (when Grafana plugin got the message)
- Advanced JSON data format support:
   - **Flat JSON objects**: Simple key-value pairs
   - **Nested JSON objects**: Automatically flattened with dotted notation (e.g., `user.profile.name`)
   - **JSON arrays**: Both top-level arrays and nested arrays are supported
   - **Mixed data types**: Preserves strings, numbers, booleans, and null values
   - **Configurable flattening**: Depth limit (default: 5) and field cap (default: 1000)
- Kafka authentication support (SASL)
- Encryption support (SSL/TLS)

## Requirements

- Apache Kafka v0.9+
- Grafana v10.2+

> Note: This is a backend plugin, so the Grafana server should've access to the Kafka broker.

## Getting started

### Installation via grafana-cli tool

Use the grafana-cli tool to install the plugin from the commandline:

```bash
grafana-cli plugins install hamedkarbasi93-kafka-datasource
```

The plugin will be installed into your grafana plugins directory; the default is `/var/lib/grafana/plugins`. [More information on the cli tool](https://grafana.com/docs/grafana/latest/administration/cli/#plugins-commands).

### Installation via zip file

Alternatively, you can manually download the [latest](https://github.com/hoptical/grafana-kafka-datasource/releases/latest) release .zip file and unpack it into your grafana plugins directory; the default is `/var/lib/grafana/plugins`.

## Configure and use the plugin

For configuration and usage instructions, see below and the [README.md](src/README.md) file in the `src` directory of this repository.

## Screenshots

![kafka dashboard](https://raw.githubusercontent.com/hoptical/grafana-kafka-datasource/86ea8d360bfd67cfed41004f80adc39219983210/src/img/graph.gif)

## Known limitations

This plugin supports JSON formatted messages with the following capabilities:

**Supported JSON structures:**
- Flat objects with simple key-value pairs
- Nested objects (automatically flattened with dotted notation)
- Top-level JSON arrays
- Mixed data types (strings, numbers, booleans, null values)

**Examples of supported formats:**

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

**Current limitations:**
- Maximum flattening depth: 5 levels (configurable)
- Maximum fields per message: 1000 (configurable)
- Binary data formats (Protobuf, AVRO) are not yet supported

We plan to support Protobuf and AVRO in upcoming releases. Contributions are highly encouraged!

## Sample producers

In the [example folder](https://github.com/hoptical/grafana-kafka-datasource/tree/main/example), there are simple producers for different languages that generate json sample values in Kafka. For more details on how to run them, please check the [`README.md`](https://github.com/hoptical/grafana-kafka-datasource/blob/main/example/README.md).

## Contribution & development

Thank you for considering contributing! If you find an issue or have a better way to do something, feel free to open an issue or a PR. To setup the development environment, follow these steps:

### Prerequisites

- Ubuntu 24.04 LTS
- node v22.15
- go 1.24.1

A data source backend plugin consists of both frontend and backend components.

### Frontend

1. Install dependencies

   ```bash
   npm install
   ```

2. Build plugin in development mode and run in watch mode

   ```bash
   npm run dev
   ```

3. Build plugin in production mode

   ```bash
   npm run build
   ```


4. Run the frontend unit tests (using Jest)

   ```bash
   # Runs the Jest unit tests and watches for changes
   npm run test

   # Exits after running all the tests (CI mode)
   npm run test:ci
   ```

5. Spin up a Grafana instance and run the plugin inside it (using Docker)

   ```bash
   npm run server
   ```


6. Run the E2E tests (using Playwright)

   ```bash
   # Spins up a Grafana instance first that we tests against
   npm run server

   # If you wish to start a certain Grafana version. If not specified will use latest by default
   GRAFANA_VERSION=11.3.0 npm run server

   # Starts the tests
   npm run e2e
   ```

7. Run the linter

   ```bash
   npm run lint

   # or

   npm run lint:fix
   ```

### Backend

1. Update [Grafana plugin SDK for Go](https://grafana.com/developers/plugin-tools/key-concepts/backend-plugins/grafana-plugin-sdk-for-go) dependency to the latest minor version:

   ```bash
   go get -u github.com/grafana/grafana-plugin-sdk-go
   go mod tidy
   ```

2. Build backend plugin binaries for Linux:

   ```bash
   mage build:linux
   ```

   To build for any other platform, use mage targets:

   ```bash
     mage -v build:darwin
     mage -v build:windows
     mage -v build:linuxArm
     mage -v build:linuxArm64
     mage -v build:darwinArm64
   ```

   `mage -v build:backend` builds for your current platform.  
   `mage -v buildAll` builds for all platforms at once.

Grafana will be available on `localhost:3000` with plugin already installed.  
Kafka will be available on `localhost:9094` for localhost connections and on `kafka:9092` for docker connections.

## License

This repository is open-sourced software licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
