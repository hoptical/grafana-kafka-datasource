# Kafka Message Key Support

## Overview

This datasource supports three modes for processing Kafka message keys:

1. **None** (default): Keys are ignored. This is the backward-compatible behavior.
2. **String**: Keys are decoded as UTF-8 strings and added as a single "key" column.
3. **JSON**: Keys are parsed as JSON objects and flattened with "key." prefix (e.g., key.userId, key.region).

## Configuration

### UI

In the Query Editor, select the **Key Format** dropdown:

- **None**: Ignore message keys (default, backward compatible)
- **String**: Decode as UTF-8 string
- **JSON**: Parse and flatten as JSON

### API

In the query JSON, add the `keyFormat` field:

```json
{
  "topicName": "my-topic",
  "partition": "all",
  "keyFormat": "json"
}
```

## Behavior

### String Keys

- Decoded as UTF-8 string
- Added as single field named "key"
- Null/missing keys: empty string ""
- Example:
  - Raw key: `user-123`
  - Result: `key = "user-123"`

### JSON Keys

- Parsed as JSON object
- Flattened with "key." prefix
- Uses same flatten depth (default 5) and field cap (default 1000) as message values
- Null/missing keys: no key fields added
- Invalid JSON: warning logged, key fields skipped, message NOT failed
- Non-object JSON (arrays, primitives): warning logged, key fields skipped
- Example:
  - Raw key: `{"userId": "123", "region": "us-east"}`
  - Result: `key.userId = "123"`, `key.region = "us-east"`

### Field Ordering

Key fields appear before message value fields in the data frame:

1. time
2. partition (if multiple partitions)
3. offset
4. key fields (sorted alphabetically)
5. value fields (sorted alphabetically)

## Error Handling

- **Null/missing keys**: Handled gracefully per format (empty string or no fields)
- **Invalid JSON keys**: Warning logged, key parsing skipped, message processing continues
- **Non-object JSON**: Warning logged, key parsing skipped
- **Key field name collides with value field name**: If a decoded key field has the same name as a flattened value field (e.g. `"key"` in string mode if the message value also contains a top-level `"key"` field, or `"key.region"` in JSON mode if the value flattens to a `"key.region"` field), the key field is dropped and a WARN is logged:
  ```
  Key field name conflicts with value field, skipping key field  field=<name>
  ```
  Value fields always take precedence. To avoid collisions, ensure your message values do not contain fields named `"key"` (string format) or with the `"key."` prefix (JSON format).

## Implementation Details

### Key Decoding

Function: `decodeMessageKey(rawKey []byte, keyFormat string) (interface{}, bool, error)`

- Location: `pkg/plugin/stream_manager.go`
- Returns: (decodedKey, shouldAddToFrame, error)

### JSON Flattening

JSON keys reuse `FlattenJSON("key", decodedKey, out, depth, maxDepth, fieldCap)` from `pkg/plugin/json_utils.go`.

### Data Flow

1. Key bytes captured in `ConsumerPull()` → `KafkaMessage.RawKey` (`pkg/kafka_client/client.go:441`)
2. Key decoded in `ProcessMessageToFrame()` based on `StreamConfig.KeyFormat` (`pkg/plugin/stream_manager.go:272-300`)
3. Key fields added to data frame before value fields (`pkg/plugin/stream_manager.go:334-373`)

## Testing

Unit tests:

- `pkg/plugin/key_utils_test.go`: Key decoding tests
  - `TestDecodeMessageKeyString`: String key tests
  - `TestDecodeMessageKeyJSON`: JSON key tests (valid, invalid, edge cases)
  - `TestDecodeMessageKeyNone`: None format tests
- `pkg/plugin/json_utils_test.go`: JSON flattening with "key" prefix
  - `TestFlattenJSONWithKeyPrefix`: Tests key prefix functionality

Run tests:

```bash
go test ./pkg/plugin/. -v -run="TestDecodeMessageKey"
go test ./pkg/plugin/. -v -run="TestFlattenJSONWithKeyPrefix"
```

## Architecture

### Backend (Go)

**Data Model:**

- `KafkaMessage` struct (`pkg/kafka_client/client.go`): Added `Key` and `RawKey` fields
- `queryModel` struct (`pkg/plugin/plugin.go`): Added `KeyFormat` field
- `StreamConfig` struct (`pkg/plugin/stream_manager.go`): Added `KeyFormat` field

**Key Functions:**

- `decodeMessageKey()` (`pkg/plugin/stream_manager.go:515-562`): Decodes keys based on format
- `ProcessMessageToFrame()` (`pkg/plugin/stream_manager.go:117-376`): Integrates key decoding into frame construction

### Frontend (TypeScript/React)

**Types:**

- `KeyFormat` enum (`src/types.ts`): NONE, STRING, JSON
- `KafkaQuery` interface (`src/types.ts`): Added `keyFormat` field
- `defaultQuery` (`src/types.ts`): Defaults to `KeyFormat.NONE`

**UI:**

- `QueryEditor.tsx`: Added Key Format selector dropdown
  - Constant: `keyFormats` array
  - Handler: `onKeyFormatChanged()`
  - UI: InlineFieldRow with Select component

## Future Enhancements (Not in Phase 1)

- **Avro key decoding**: Requires Schema Registry integration for keys
- **Protobuf key decoding**: Requires schema handling for keys
- **Separate flatten config for keys**: Keys currently share value flatten limits
- **Key-specific error fields**: Invalid keys are logged only

## Backward Compatibility

- Default `KeyFormat` is "none" → keys ignored (current behavior)
- Existing queries without `keyFormat` field continue to work unchanged
- No breaking changes to API or data model
- New fields are additive

## Example Usage

### String Keys

Query:

```json
{
  "topicName": "user-events",
  "keyFormat": "string"
}
```

Kafka message:

- Key: `user-123`
- Value: `{"event": "login", "timestamp": 1234567890}`

Result Data Frame:

```
time             | offset | key      | event | timestamp
2024-01-15 10:00 | 1001   | user-123 | login | 1234567890
```

### JSON Keys

Query:

```json
{
  "topicName": "orders",
  "keyFormat": "json"
}
```

Kafka message:

- Key: `{"userId": "123", "region": "us-east"}`
- Value: `{"total": 99.99, "items": 3}`

Result Data Frame:

```
time             | offset | key.userId | key.region | total | items
2024-01-15 10:00 | 2001   | 123        | us-east    | 99.99 | 3
```

### None (Default)

Query:

```json
{
  "topicName": "logs"
}
```

Kafka message:

- Key: `ignored-key`
- Value: `{"level": "info", "message": "Server started"}`

Result Data Frame:

```
time             | offset | level | message
2024-01-15 10:00 | 3001   | info  | Server started
```

## Troubleshooting

### Keys not appearing

- Check Key Format is set to "String" or "JSON" (not "None")
- Verify messages actually have keys (check with kafka-console-consumer)
- Check browser console and Grafana logs for errors

### JSON key parsing fails

- Verify keys are valid JSON objects (not arrays or primitives)
- Check logs for warning messages about parsing failures
- Use "String" format as fallback to see raw key content

### Performance issues with JSON keys

- JSON keys with many fields count toward global field cap
- Consider using "String" format for complex keys
- Adjust flatten depth and field cap in datasource settings if needed

## Related Files

- `pkg/kafka_client/client.go` - KafkaMessage struct, key capture
- `pkg/plugin/plugin.go` - queryModel, RunStream
- `pkg/plugin/stream_manager.go` - decodeMessageKey, ProcessMessageToFrame
- `pkg/plugin/json_utils.go` - FlattenJSON (reused for keys)
- `src/types.ts` - Frontend types
- `src/QueryEditor.tsx` - UI components
- `pkg/plugin/key_utils_test.go` - Unit tests
- `pkg/plugin/json_utils_test.go` - Flatten tests
