package plugin

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// ProcessMessageFrames is the multi-frame equivalent of ProcessMessage. For
// the line-protocol format the parsed lines are emitted as a single
// long-format frame (one row per LP field) with a stable, streaming-friendly
// schema: every message produces the same column shape, so Grafana's Live
// channel can append rows cleanly. Each row carries its own `_measurement`,
// `_field`, `value` / `value_str`, and per-tag columns — so Grafana's
// "Partition by values" transform can pivot the data back into Influx-style
// per-series frames for dashboards that were built around the InfluxDB
// datasource.
//
// Tombstones (nil RawValue) on a line-protocol topic emit zero frames. A
// message that fails to produce any well-formed line emits a single error
// frame so the operator notices the bad payload.
func (sm *StreamManager) ProcessMessageFrames(
	msg kafka_client.KafkaMessage,
	partition int32,
	partitions []int32,
	config *StreamConfig,
	topic string,
) ([]*data.Frame, error) {
	if config != nil && config.MessageFormat == "lineprotocol" {
		if msg.Error != nil {
			errFrame, err := createErrorFrame(msg, partition, partitions, msg.Error, config, topic)
			if err != nil {
				return nil, err
			}
			return []*data.Frame{errFrame}, nil
		}
		if len(msg.RawValue) == 0 {
			return nil, nil
		}
		lines, parseErrs := ParseLines(msg.RawValue)
		for _, e := range parseErrs {
			log.DefaultLogger.Debug("lineprotocol parse error (line skipped)",
				"error", e,
				"partition", partition,
				"offset", msg.Offset)
		}
		if len(lines) == 0 {
			cause := fmt.Errorf("lineprotocol payload produced no parseable lines")
			if len(parseErrs) > 0 {
				cause = parseErrs[0]
			}
			errFrame, err := createErrorFrame(msg, partition, partitions, cause, config, topic)
			if err != nil {
				return nil, err
			}
			return []*data.Frame{errFrame}, nil
		}
		return sm.buildLineProtocolFrames(msg, lines, partition, partitions, config, topic), nil
	}

	frame, err := sm.ProcessMessage(msg, partition, partitions, config, topic)
	if err != nil {
		return nil, err
	}
	if frame == nil {
		return nil, nil
	}
	return []*data.Frame{frame}, nil
}

// buildLineProtocolFrames builds a single long-format frame per Kafka message:
// one row per LP field, fixed columns —
//
//	Time           time.Time
//	_measurement   string
//	_field         string
//	value          *float64    (set for numeric/bool LP types)
//	value_str      *string     (set for quoted-string LP types)
//	<tag-key>      *string     (one column per tag key seen in the message)
//	partition?     int32       (multi-partition consumers only)
//	offset         int64
//
// Tag columns are nullable per row because different LP lines within one
// Kafka message can carry different tag sets — cells where a row's line
// didn't have a given tag key are nil.
func (sm *StreamManager) buildLineProtocolFrames(
	msg kafka_client.KafkaMessage,
	lines []ParsedLine,
	partition int32,
	partitions []int32,
	config *StreamConfig,
	topic string,
) []*data.Frame {
	filter := buildLineProtocolFilter(config)
	if filter != nil {
		lines = applyLineProtocolFilter(lines, filter)
	}

	totalRows := 0
	for _, l := range lines {
		totalRows += len(l.Fields)
	}
	if totalRows == 0 {
		return nil
	}

	tagKeys := sm.mergeLPTagKeys(lines)
	multiPartition := len(partitions) > 1

	times := make([]time.Time, 0, totalRows)
	measurements := make([]string, 0, totalRows)
	fieldNames := make([]string, 0, totalRows)
	values := make([]*float64, 0, totalRows)
	valueStrs := make([]*string, 0, totalRows)
	var partitions32 []int32
	if multiPartition {
		partitions32 = make([]int32, 0, totalRows)
	}
	offsets := make([]int64, 0, totalRows)
	tagCols := make(map[string][]*string, len(tagKeys))
	for _, k := range tagKeys {
		tagCols[k] = make([]*string, 0, totalRows)
	}

	for _, line := range lines {
		ts := resolveLineProtocolTimestamp(line, config, msg.Timestamp)
		tagMap := make(map[string]string, len(line.Tags))
		for _, t := range line.Tags {
			tagMap[t.Key] = t.Value
		}
		for _, f := range line.Fields {
			times = append(times, ts)
			measurements = append(measurements, line.Measurement)
			fieldNames = append(fieldNames, f.Key)
			fv, fs := coerceLineProtocolValue(f.Value)
			values = append(values, fv)
			valueStrs = append(valueStrs, fs)
			if multiPartition {
				partitions32 = append(partitions32, partition)
			}
			offsets = append(offsets, msg.Offset)
			for _, k := range tagKeys {
				if v, ok := tagMap[k]; ok {
					s := v
					tagCols[k] = append(tagCols[k], &s)
				} else {
					tagCols[k] = append(tagCols[k], nil)
				}
			}
		}
	}

	frame := data.NewFrame("lineprotocol")
	// Carry query metadata so the configured alias/RefID survive, matching
	// ProcessMessage. Without this the user's Alias is silently dropped for
	// line-protocol streams.
	if config != nil {
		if config.RefID != "" {
			frame.RefID = config.RefID
		}
		if config.Alias != "" {
			frame.Name = formatAlias(config.Alias, config, topic, partition, "")
		}
	}
	frame.Fields = append(frame.Fields,
		data.NewField("Time", nil, times),
		data.NewField("_measurement", nil, measurements),
		data.NewField("_field", nil, fieldNames),
		data.NewField("value", nil, values),
		data.NewField("value_str", nil, valueStrs),
	)
	for _, k := range tagKeys {
		frame.Fields = append(frame.Fields, data.NewField(k, nil, tagCols[k]))
	}
	if multiPartition {
		frame.Fields = append(frame.Fields, data.NewField("partition", nil, partitions32))
	}
	frame.Fields = append(frame.Fields, data.NewField("offset", nil, offsets))

	return []*data.Frame{frame}
}

// mergeLPTagKeys folds the tag keys from the current message's lines into the
// stream's running union, then returns the full union sorted for stable column
// order. Maintaining the union across messages (rather than per-message) keeps
// the frame schema stable for Grafana Live: once a tag key has been seen, every
// later frame keeps that column (nil-padded when a message omits the tag),
// instead of the column set oscillating message to message.
func (sm *StreamManager) mergeLPTagKeys(lines []ParsedLine) []string {
	for _, l := range lines {
		for _, t := range l.Tags {
			sm.lpTagKeys[t.Key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(sm.lpTagKeys))
	for k := range sm.lpTagKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// maxExactFloat64Int is the largest integer magnitude float64 can hold without
// losing precision (2^53 - 1). Integers beyond this are emitted as decimal
// strings in value_str rather than being silently rounded in value.
const maxExactFloat64Int = int64(1)<<53 - 1

// coerceLineProtocolValue routes a parsed LP value into the long-format
// `value` / `value_str` column pair. Numeric and boolean LP types land in
// `value` as float64; quoted-string LP values land in `value_str`. Booleans
// become 1 (true) or 0 (false). Integers too large for float64 to represent
// exactly are preserved as decimal strings in `value_str`.
func coerceLineProtocolValue(v interface{}) (*float64, *string) {
	switch x := v.(type) {
	case float64:
		f := x
		return &f, nil
	case int64:
		if x >= -maxExactFloat64Int && x <= maxExactFloat64Int {
			f := float64(x)
			return &f, nil
		}
		s := strconv.FormatInt(x, 10)
		return nil, &s
	case uint64:
		if x <= uint64(maxExactFloat64Int) {
			f := float64(x)
			return &f, nil
		}
		s := strconv.FormatUint(x, 10)
		return nil, &s
	case bool:
		var f float64
		if x {
			f = 1
		}
		return &f, nil
	case string:
		s := x
		return nil, &s
	default:
		return nil, nil
	}
}

// resolveLineProtocolTimestamp picks the right time.Time for a parsed line
// based on the user's TimestampMode and timestamp-precision configuration.
func resolveLineProtocolTimestamp(line ParsedLine, config *StreamConfig, kafkaTime time.Time) time.Time {
	if config != nil && config.TimestampMode == "now" {
		return time.Now()
	}
	if !line.HasTimestamp {
		return kafkaTime
	}
	precision := ""
	if config != nil {
		precision = config.LineProtocolTimestampPrecision
	}
	switch precision {
	case "ns":
		return time.Unix(0, line.Timestamp)
	case "us", "µs":
		return time.UnixMicro(line.Timestamp)
	case "ms":
		return time.UnixMilli(line.Timestamp)
	case "s":
		return time.Unix(line.Timestamp, 0)
	default:
		return autoDetectTimestampPrecision(line.Timestamp)
	}
}

// autoDetectTimestampPrecision picks ns / µs / ms / s from the magnitude of a
// raw integer timestamp. Boundaries are chosen so any plausible Unix epoch
// value from year ~2001 onward classifies correctly.
func autoDetectTimestampPrecision(ts int64) time.Time {
	abs := ts
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= 1_000_000_000_000_000_000: // ≥ 1e18 → nanoseconds
		return time.Unix(0, ts)
	case abs >= 1_000_000_000_000_000: // ≥ 1e15 → microseconds
		return time.UnixMicro(ts)
	case abs >= 1_000_000_000_000: // ≥ 1e12 → milliseconds
		return time.UnixMilli(ts)
	default:
		return time.Unix(ts, 0)
	}
}
