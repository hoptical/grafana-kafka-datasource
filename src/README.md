# Kafka Datasource Plugin

 Visualize real-time streaming data from Apache Kafka directly in your Grafana dashboards. This plugin enables you to monitor live Kafka topics with automatic updates and time-series visualizations.
 
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

## Version Compatibility

- Apache Kafka v0.9 or later
- Grafana v10.2 or later
- Network access from Grafana server to Kafka brokers

## Installation

For the installation process, please refer to the [plugin installation docs](https://grafana.com/docs/grafana/latest/administration/plugin-management/).

### Configuration

After installation, configure the plugin by adding a new Kafka data source in Grafana and filling out the following fields:

 - **Bootstrap Servers**: Comma-separated list of Kafka bootstrap servers (e.g. `broker1:9092, broker2:9092`)
 - **Security Protocol**: Choose the protocol (e.g. `PLAINTEXT`, `SASL_PLAINTEXT`)
 - **SASL Mechanisms**: Specify SASL mechanism if required (e.g. `PLAIN`, `SCRAM-SHA-512`)
 - **SASL Username/Password**: Provide credentials if SASL authentication is enabled
 - **Log Level**: Set log verbosity (`debug`, `error`)
 - **API Key**: (Deprecated) This field is deprecated and will be removed in future versions. Avoid using it for new configurations.

### Provisioning

You can automatically configure the Kafka datasource using Grafana's provisioning feature. Create a YAML file in your Grafana provisioning directory just like example in `provisioning/datasources/datasource.yaml`. This allows you to set up the Kafka data source without manual configuration through the Grafana UI.

## Build The Query

1. Create a new dashboard panel
2. Select your Kafka data source
3. Configure the query:
   - **Topic**: Your Kafka topic name. You can search your topic and select it from the autocomplete dropdown.
   - Click the **Fetch** button to retrieve the available partitions
   - **Partition**: Partition number (or "all" for all partitions)
   - **Offset Reset**:
     - **Latest**: Only new messages
     - **Last N messages**: Start from the most recent N messages per partition (set N in the UI)
     - **Earliest**: Start from the oldest message
  - **Timestamp Mode**: Use "Kafka Event Time" (from message metadata, default) or "Dashboard received time" (when Grafana plugin got the message)

## Supported Data Format

Your Kafka messages can contain various JSON structures that are automatically processed:

**Simple flat objects:**
```json
{
  "temperature": 23.5,
  "humidity": 65.2,
  "pressure": 1013.25,
  "status": "active"
}
```

**Nested objects** (automatically flattened with dotted notation):
```json
{
  "sensor": {
    "location": "warehouse_a",
    "readings": {
      "temperature": 23.5,
      "humidity": 65.2
    }
  },
  "timestamp": "2025-01-15T10:30:00Z"
}
```
Fields become: `sensor.location`, `sensor.readings.temperature`, `sensor.readings.humidity`, `timestamp`

**Top-level arrays:**
```json
[
  {"id": 1, "value": 10.5, "type": "cpu"},
  {"id": 2, "value": 20.3, "type": "memory"}
]
```
Fields become: `item_0.id`, `item_0.value`, `item_0.type`, `item_1.id`, `item_1.value`, `item_1.type`

**Data features:**
- All numeric fields become separate time series in your graph
- String fields are preserved as labels
- Boolean and null values are handled appropriately
- Message offset and partition information are automatically included
- Configurable flattening depth (default: 5 levels) and field limits (default: 1000 fields)

## Getting Help

- Check the [GitHub repository](https://github.com/hoptical/grafana-kafka-datasource) for documentation and examples
- Review sample producer code in different programming languages
- Report issues or request features via GitHub Issues
