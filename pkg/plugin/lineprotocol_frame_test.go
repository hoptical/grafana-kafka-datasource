package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

func fieldByName(f *data.Frame, name string) *data.Field {
	for _, fld := range f.Fields {
		if fld.Name == name {
			return fld
		}
	}
	return nil
}

func strAt(f *data.Field, i int) string {
	switch v := f.At(i).(type) {
	case string:
		return v
	case *string:
		if v != nil {
			return *v
		}
	}
	return ""
}

func strPtrAt(f *data.Field, i int) *string {
	v, _ := f.At(i).(*string)
	return v
}

func floatPtrAt(f *data.Field, i int) *float64 {
	v, _ := f.At(i).(*float64)
	return v
}

func buildFrames(t *testing.T, msg kafka_client.KafkaMessage, lines []ParsedLine, partition int32, partitions []int32, cfg *StreamConfig) []*data.Frame {
	t.Helper()
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	return sm.buildLineProtocolFrames(msg, lines, partition, partitions, cfg, "topic")
}

// TestCoerceLineProtocolValue_LargeIntegerPreservedAsString locks in that
// integers beyond float64's exact range land in value_str (as decimal strings)
// instead of being silently rounded in value.
func TestCoerceLineProtocolValue_LargeIntegerPreservedAsString(t *testing.T) {
	// 2^53 + 1 is the first int64 that float64 cannot represent exactly.
	v, s := coerceLineProtocolValue(int64(9007199254740993))
	if v != nil {
		t.Errorf("large int64 should not go to value, got %v", *v)
	}
	if s == nil || *s != "9007199254740993" {
		t.Errorf("large int64 should be decimal string %q, got %v", "9007199254740993", s)
	}

	// Values within range still go to the numeric column.
	v2, s2 := coerceLineProtocolValue(int64(42))
	if v2 == nil || *v2 != 42 || s2 != nil {
		t.Errorf("small int64 should be value=42 with nil value_str, got v=%v s=%v", v2, s2)
	}

	// max uint64 exceeds float64 range too.
	vu, su := coerceLineProtocolValue(uint64(18446744073709551615))
	if vu != nil || su == nil || *su != "18446744073709551615" {
		t.Errorf("large uint64 should be decimal string, got v=%v s=%v", vu, su)
	}
}

// TestBuildLineProtocolFrames_StableLongShape verifies the streaming-friendly
// long format: one frame per Kafka message with a stable schema
// (Time, _measurement, _field, value, value_str, <tag cols>, offset).
func TestBuildLineProtocolFrames_StableLongShape(t *testing.T) {
	parsed := []ParsedLine{{
		Measurement: "Breaker Data",
		Tags: []TagKV{
			{"Building", "DCM102"},
			{"Device_tag", "-XQ002"},
		},
		Fields: []FieldKV{
			{"PT Primary", float64(46.37)},
			{"Number Of Poles", int64(45559)},
			{"Firmware revision", "XXXXXX"},
			{"Circuit breaker closed", true},
		},
		Timestamp: 1779186714, HasTimestamp: true,
	}}
	cfg := &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
	}
	msg := kafka_client.KafkaMessage{Offset: 42, Timestamp: time.Unix(1700000000, 0)}

	frames := buildFrames(t, msg, parsed, 0, []int32{0}, cfg)
	if len(frames) != 1 {
		t.Fatalf("want 1 frame per Kafka message, got %d", len(frames))
	}
	f := frames[0]

	// 4 rows (one per LP field).
	if got := fieldByName(f, "Time").Len(); got != 4 {
		t.Fatalf("want 4 rows, got %d", got)
	}

	// Mandatory columns present.
	for _, want := range []string{"Time", "_measurement", "_field", "value", "value_str", "Building", "Device_tag", "offset"} {
		if fieldByName(f, want) == nil {
			t.Errorf("missing column %q", want)
		}
	}

	// _measurement is the same across all rows (single LP line).
	m := fieldByName(f, "_measurement")
	for i := 0; i < m.Len(); i++ {
		if strAt(m, i) != "Breaker Data" {
			t.Errorf("row %d _measurement: %q", i, strAt(m, i))
		}
	}

	// _field rows match LP field order.
	fk := fieldByName(f, "_field")
	want := []string{"PT Primary", "Number Of Poles", "Firmware revision", "Circuit breaker closed"}
	for i, w := range want {
		if got := strAt(fk, i); got != w {
			t.Errorf("row %d _field: want %q, got %q", i, w, got)
		}
	}

	// value column: numeric/bool rows populated; string row is nil.
	v := fieldByName(f, "value")
	if got := floatPtrAt(v, 0); got == nil || *got != 46.37 {
		t.Errorf("row 0 value: %v", v.At(0))
	}
	if got := floatPtrAt(v, 1); got == nil || *got != 45559 {
		t.Errorf("row 1 value: %v", v.At(1))
	}
	if floatPtrAt(v, 2) != nil {
		t.Errorf("row 2 value should be nil (string field): %v", v.At(2))
	}
	if got := floatPtrAt(v, 3); got == nil || *got != 1 {
		t.Errorf("row 3 value (bool true → 1): %v", v.At(3))
	}

	// value_str column: only the string row is populated.
	vs := fieldByName(f, "value_str")
	if strPtrAt(vs, 0) != nil || strPtrAt(vs, 1) != nil || strPtrAt(vs, 3) != nil {
		t.Errorf("non-string rows should have value_str nil")
	}
	if got := strPtrAt(vs, 2); got == nil || *got != "XXXXXX" {
		t.Errorf("row 2 value_str: %v", vs.At(2))
	}

	// Tag column values populated on every row.
	b := fieldByName(f, "Building")
	for i := 0; i < b.Len(); i++ {
		if got := strPtrAt(b, i); got == nil || *got != "DCM102" {
			t.Errorf("row %d Building: %v", i, b.At(i))
		}
	}
}

// TestBuildLineProtocolFrames_TagColumnUnionAcrossLines: when one Kafka message
// contains LP lines with different tag SETS, the resulting frame has a single
// column per tag KEY, with cells nil where a line didn't have that tag.
func TestBuildLineProtocolFrames_TagColumnUnionAcrossLines(t *testing.T) {
	parsed := []ParsedLine{
		{
			Measurement: "a",
			Tags:        []TagKV{{"shared", "x"}, {"only_a", "1"}},
			Fields:      []FieldKV{{"v", float64(1)}},
		},
		{
			Measurement: "b",
			Tags:        []TagKV{{"shared", "y"}, {"only_b", "2"}},
			Fields:      []FieldKV{{"v", float64(2)}},
		},
	}
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	frames := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 0, []int32{0}, cfg)
	f := frames[0]

	for _, k := range []string{"shared", "only_a", "only_b"} {
		if fieldByName(f, k) == nil {
			t.Errorf("missing tag column %q", k)
		}
	}
	// row 0 from "a": only_a populated, only_b nil.
	if v := strPtrAt(fieldByName(f, "only_a"), 0); v == nil || *v != "1" {
		t.Errorf("row 0 only_a: %v", fieldByName(f, "only_a").At(0))
	}
	if strPtrAt(fieldByName(f, "only_b"), 0) != nil {
		t.Errorf("row 0 only_b should be nil")
	}
	// row 1 from "b": only_b populated, only_a nil.
	if v := strPtrAt(fieldByName(f, "only_b"), 1); v == nil || *v != "2" {
		t.Errorf("row 1 only_b: %v", fieldByName(f, "only_b").At(1))
	}
	if strPtrAt(fieldByName(f, "only_a"), 1) != nil {
		t.Errorf("row 1 only_a should be nil")
	}
}

func TestBuildLineProtocolFrames_PartitionColumnOnlyWhenMulti(t *testing.T) {
	parsed := []ParsedLine{{Measurement: "m", Fields: []FieldKV{{"f", float64(1)}}, Timestamp: 0, HasTimestamp: true}}
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	single := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 0, []int32{0}, cfg)
	if fieldByName(single[0], "partition") != nil {
		t.Errorf("partition should be absent on single-partition consumer")
	}
	multi := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 1, []int32{0, 1, 2}, cfg)
	if fieldByName(multi[0], "partition") == nil {
		t.Errorf("partition should be present on multi-partition consumer")
	}
}

func TestBuildLineProtocolFrames_TimestampModeNow(t *testing.T) {
	parsed := []ParsedLine{{Measurement: "m", Fields: []FieldKV{{"f", float64(1)}}, Timestamp: 1, HasTimestamp: true}}
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "now", LineProtocolTimestampPrecision: "auto"}
	before := time.Now()
	frames := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 0, []int32{0}, cfg)
	after := time.Now()
	got := frames[0].Fields[0].At(0).(time.Time)
	if got.Before(before) || got.After(after) {
		t.Errorf("now mode: got %v not within [%v,%v]", got, before, after)
	}
}

func TestBuildLineProtocolFrames_FallbackToKafkaTimestamp(t *testing.T) {
	kafkaTs := time.Unix(1700000000, 0)
	parsed := []ParsedLine{{Measurement: "m", Fields: []FieldKV{{"f", float64(1)}}, HasTimestamp: false}}
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "auto"}
	frames := buildFrames(t, kafka_client.KafkaMessage{Timestamp: kafkaTs}, parsed, 0, []int32{0}, cfg)
	got := frames[0].Fields[0].At(0).(time.Time)
	if !got.Equal(kafkaTs) {
		t.Errorf("ts fallback: want %v, got %v", kafkaTs, got)
	}
}

func TestBuildLineProtocolFrames_AutoPrecisionDetect(t *testing.T) {
	cases := []struct {
		name string
		ts   int64
		want time.Time
	}{
		{"ns", 1779186714000000000, time.Unix(0, 1779186714000000000)},
		{"us", 1779186714000000, time.UnixMicro(1779186714000000)},
		{"ms", 1779186714000, time.UnixMilli(1779186714000)},
		{"s", 1779186714, time.Unix(1779186714, 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed := []ParsedLine{{Measurement: "m", Fields: []FieldKV{{"f", float64(1)}}, Timestamp: tc.ts, HasTimestamp: true}}
			cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "auto"}
			frames := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 0, []int32{0}, cfg)
			got := frames[0].Fields[0].At(0).(time.Time)
			if !got.Equal(tc.want) {
				t.Errorf("%s: want %v, got %v", tc.name, tc.want, got)
			}
		})
	}
}

// TestBuildLineProtocolFrames_TagKeyCollisionWithReservedColumn is a
// regression test: a tag key that collides with a reserved column name (e.g.
// "value") used to overwrite/shadow that column since data.Frame.FieldByName
// only returns the first match. The colliding tag must now be renamed to
// "tag_<key>" so both columns remain independently readable.
func TestBuildLineProtocolFrames_TagKeyCollisionWithReservedColumn(t *testing.T) {
	parsed := []ParsedLine{{
		Measurement:  "m",
		Tags:         []TagKV{{"value", "abc"}},
		Fields:       []FieldKV{{"f", float64(1)}},
		Timestamp:    100,
		HasTimestamp: true,
	}}
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	frames := buildFrames(t, kafka_client.KafkaMessage{}, parsed, 0, []int32{0}, cfg)
	f := frames[0]

	reservedValue := fieldByName(f, "value")
	if reservedValue == nil {
		t.Fatalf("missing reserved 'value' numeric column")
	}
	if got := floatPtrAt(reservedValue, 0); got == nil || *got != 1 {
		t.Errorf("reserved 'value' column should hold the numeric field value, got %v", reservedValue.At(0))
	}

	tagValue := fieldByName(f, "tag_value")
	if tagValue == nil {
		t.Fatalf("expected colliding tag key 'value' to be renamed to 'tag_value'")
	}
	if got := strPtrAt(tagValue, 0); got == nil || *got != "abc" {
		t.Errorf("tag_value column: want %q, got %v", "abc", tagValue.At(0))
	}
}

// TestGetLineProtocolFilter_CachedAcrossCalls is a regression test: the
// compiled filter used to be rebuilt from scratch (with fresh regexp.Compile
// calls) on every message. It must now be built once per stream and reused.
func TestGetLineProtocolFilter_CachedAcrossCalls(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	cfg := &StreamConfig{
		MessageFormat:            "lineprotocol",
		LineProtocolMeasurements: "Breaker Data",
	}

	first := sm.getLineProtocolFilter(cfg)
	second := sm.getLineProtocolFilter(cfg)
	if first == nil {
		t.Fatalf("expected a non-nil filter given a non-empty measurement filter")
	}
	if first != second {
		t.Errorf("expected the cached filter pointer to be reused across calls, got different instances")
	}
}
