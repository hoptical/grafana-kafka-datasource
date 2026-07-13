package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

func TestProcessMessageFrames_LineProtocolSingleLongFrame(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	// 3 LP lines × 1 field each = 1 frame, 3 rows.
	raw := []byte("a,t=v f=1 100\nb,t=v g=2 200\nc,t=v h=3 300\n")

	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{
			MessageFormat:                  "lineprotocol",
			TimestampMode:                  "message",
			LineProtocolTimestampPrecision: "s",
		},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("want 1 frame per Kafka message, got %d", len(frames))
	}
	f := frames[0]
	if got := fieldByName(f, "_measurement").Len(); got != 3 {
		t.Fatalf("want 3 rows, got %d", got)
	}
	m := fieldByName(f, "_measurement")
	for i, want := range []string{"a", "b", "c"} {
		if strAt(m, i) != want {
			t.Errorf("row %d _measurement: want %q, got %q", i, want, strAt(m, i))
		}
	}
}

// TestProcessMessageFrames_TagSchemaStableAcrossMessages verifies the
// stream-level tag-key union: once a tag key is seen, later frames keep that
// column (nil-padded) even when a message omits it, instead of the column set
// shrinking message to message.
func TestProcessMessageFrames_TagSchemaStableAcrossMessages(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	cfg := &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
	}

	// Message 1 carries tag 'host' only.
	f1, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("a,host=h1 f=1 100\n"), Offset: 1, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("msg1 err: %v", err)
	}
	if fieldByName(f1[0], "host") == nil {
		t.Fatalf("frame1 should have 'host' column")
	}

	// Message 2 carries tag 'region' only — but the frame must still include
	// 'host' (nil-padded) from the running union.
	f2, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("b,region=r1 g=2 200\n"), Offset: 2, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("msg2 err: %v", err)
	}
	host := fieldByName(f2[0], "host")
	region := fieldByName(f2[0], "region")
	if host == nil {
		t.Fatalf("frame2 must retain 'host' column from earlier message")
	}
	if region == nil {
		t.Fatalf("frame2 must have 'region' column")
	}
	if strPtrAt(host, 0) != nil {
		t.Errorf("frame2 'host' should be nil-padded (msg2 has no host), got %v", host.At(0))
	}
	if v := strPtrAt(region, 0); v == nil || *v != "r1" {
		t.Errorf("frame2 'region' should be 'r1', got %v", region.At(0))
	}
}

// TestProcessMessageFrames_AliasAndRefIDOnFrame verifies the configured RefID
// and Alias survive onto the line-protocol frame (previously dropped).
func TestProcessMessageFrames_AliasAndRefIDOnFrame(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("a,t=v f=1 100\n"), Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{
			MessageFormat:                  "lineprotocol",
			TimestampMode:                  "message",
			LineProtocolTimestampPrecision: "s",
			RefID:                          "A",
			Alias:                          "my-stream",
		},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if frames[0].RefID != "A" {
		t.Errorf("frame RefID: want 'A', got %q", frames[0].RefID)
	}
	if frames[0].Name == "" {
		t.Errorf("frame Name should be set from alias, got empty")
	}
}

func TestProcessMessageFrames_LineProtocolTombstoneEmitsNothing(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: nil, Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message"},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("tombstone should emit 0 frames, got %d", len(frames))
	}
}

func TestProcessMessageFrames_LineProtocolUnparseableProducesErrorFrame(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	raw := []byte("this is not line protocol at all #")

	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message"},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("want 1 error frame, got %d", len(frames))
	}
	if frames[0].Name != "error" && fieldByName(frames[0], "error") == nil {
		t.Errorf("expected an error frame, got %v", frames[0])
	}
}

func TestProcessMessageFrames_JSONStillReturnsSingleFrame(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	raw := []byte(`{"a":1,"b":2}`)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{MessageFormat: "json", TimestampMode: "message"},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(frames) != 1 {
		t.Errorf("json should yield 1 frame, got %d", len(frames))
	}
}

func TestProcessMessageFrames_LineProtocolPartialBatchSkipsBadLines(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	raw := []byte("good f=1 100\nbroken_line_no_equals\ngood2 f=2 200\n")
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 1, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// 2 good lines (bad line dropped) → 1 frame, 2 rows.
	if len(frames) != 1 {
		t.Fatalf("want 1 frame, got %d", len(frames))
	}
	if got := fieldByName(frames[0], "_measurement").Len(); got != 2 {
		t.Errorf("want 2 rows, got %d", got)
	}
}

// TestProcessMessageFrames_FullRealPayload — regression on user's real payload.
// Asserts the long-format shape: one frame with N rows, stable columns including
// _measurement / _field / value / value_str / one column per tag key.
func TestProcessMessageFrames_FullRealPayload(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "lineprotocol", "full_real_sample.txt"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 0, Timestamp: time.Now()},
		0, []int32{0},
		&StreamConfig{
			MessageFormat:                  "lineprotocol",
			TimestampMode:                  "message",
			LineProtocolTimestampPrecision: "s",
		},
		"topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("want 1 long-format frame, got %d", len(frames))
	}
	f := frames[0]

	for _, want := range []string{
		"Time", "_measurement", "_field", "value", "value_str", "offset",
		"Building", "Dashboard", "Description", "Device_tag", "Equipment-tag",
		"Floor", "Full_tag", "Gapit-product-code", "Module", "POD", "POD_nr",
		"Site", "System", "uid",
	} {
		if fieldByName(f, want) == nil {
			t.Errorf("missing column %q", want)
		}
	}

	// First row from 'Last Trip'.
	m := fieldByName(f, "_measurement")
	fk := fieldByName(f, "_field")
	if strAt(m, 0) != "Last Trip" {
		t.Errorf("row 0 _measurement: want 'Last Trip', got %q", strAt(m, 0))
	}
	if strAt(fk, 0) != "Last trip event Timestamp" {
		t.Errorf("row 0 _field: want 'Last trip event Timestamp', got %q", strAt(fk, 0))
	}
	if v := floatPtrAt(fieldByName(f, "value"), 0); v == nil || *v != 4523548585 {
		t.Errorf("row 0 value: want 4523548585, got %v", fieldByName(f, "value").At(0))
	}

	// Building tag populated on every row.
	bldg := fieldByName(f, "Building")
	for i := 0; i < bldg.Len(); i++ {
		if v := strPtrAt(bldg, i); v == nil || *v != "DCM102" {
			t.Errorf("row %d Building: %v", i, bldg.At(i))
		}
	}

	// A string-typed field (Firmware revision in Breaker Data) lives in value_str.
	v := fieldByName(f, "value")
	vs := fieldByName(f, "value_str")
	foundFirmware := false
	for i := 0; i < fk.Len(); i++ {
		if strAt(fk, i) == "Firmware revision" {
			foundFirmware = true
			if floatPtrAt(v, i) != nil {
				t.Errorf("Firmware revision row %d: value should be nil, got %v", i, v.At(i))
			}
			if s := strPtrAt(vs, i); s == nil || *s == "" {
				t.Errorf("Firmware revision row %d: value_str should be non-nil string", i)
			}
			break
		}
	}
	if !foundFirmware {
		t.Errorf("expected to find 'Firmware revision' row")
	}
}

// TestProcessMessageFrames_ErrorFrameSchemaCompatibleWithSuccessFrame is a
// regression test: the line-protocol error frame used to have a completely
// different schema (lowercase "time", no _measurement/_field/value/tag
// columns) from the success-path frame. Both are sent on the same Grafana
// Live channel, so a malformed message among valid ones used to flip the
// channel's schema. The error frame must now share the same core columns
// (name and type) as the success frame.
func TestProcessMessageFrames_ErrorFrameSchemaCompatibleWithSuccessFrame(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	// A well-formed message first, establishing the 'host' tag-key schema.
	good, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("m,host=h1 f=1 100\n"), Offset: 1, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("good message err: %v", err)
	}

	// Then a malformed message on the same stream.
	bad, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("not line protocol #"), Offset: 2, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("bad message err: %v", err)
	}
	if len(bad) != 1 {
		t.Fatalf("want 1 error frame, got %d", len(bad))
	}

	successFrame := good[0]
	errorFrame := bad[0]

	for _, want := range []string{"Time", "_measurement", "_field", "value", "value_str", "host", "offset"} {
		sf := fieldByName(successFrame, want)
		ef := fieldByName(errorFrame, want)
		if sf == nil || ef == nil {
			t.Fatalf("column %q missing from success frame (present=%v) or error frame (present=%v)", want, sf != nil, ef != nil)
		}
		if sf.Type() != ef.Type() {
			t.Errorf("column %q type mismatch: success=%v error=%v", want, sf.Type(), ef.Type())
		}
	}
	if fieldByName(errorFrame, "error") == nil {
		t.Errorf("error frame should carry an 'error' column")
	}
}

func TestProcessMessageFrames_ErrorFrameKeepsErrorTagDistinct(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	_, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("m,error=tagval f=1 100\n"), Offset: 1, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("good message err: %v", err)
	}

	bad, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("not line protocol #"), Offset: 2, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("bad message err: %v", err)
	}
	if len(bad) != 1 {
		t.Fatalf("want 1 error frame, got %d", len(bad))
	}

	errorFrame := bad[0]
	if fieldByName(errorFrame, "tag_error") == nil {
		t.Fatalf("expected tag key 'error' to map to 'tag_error' column")
	}
	if fieldByName(errorFrame, "error") == nil {
		t.Fatalf("expected dedicated error column")
	}

	seen := map[string]struct{}{}
	for _, f := range errorFrame.Fields {
		name := f.Name
		if _, dup := seen[name]; dup {
			t.Fatalf("duplicate field name in error frame: %q", name)
		}
		seen[name] = struct{}{}
	}
}

func TestProcessMessageFrames_ErrorFrameRespectsTimestampModeNow(t *testing.T) {
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "now", LineProtocolTimestampPrecision: "s"}

	msgTs := time.Unix(123, 0)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: []byte("not line protocol #"), Offset: 2, Timestamp: msgTs},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("bad message err: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("want 1 error frame, got %d", len(frames))
	}

	got := frames[0].Fields[0].At(0).(time.Time)
	if got.Equal(msgTs) {
		t.Fatalf("TimestampMode=now should not use kafka timestamp; got %v", got)
	}
}

// TestProcessMessageFrames_TagKeyGrowthIsCapped is a regression test: the
// stream-wide tag-key union used to grow unboundedly for the life of a
// stream. It must now stop growing once flattenFieldCap distinct tag keys
// have been seen.
func TestProcessMessageFrames_TagKeyGrowthIsCapped(t *testing.T) {
	const tagCap = 3
	sm := NewStreamManager(&mockStreamClient{}, 5, tagCap)
	cfg := &StreamConfig{MessageFormat: "lineprotocol", TimestampMode: "message", LineProtocolTimestampPrecision: "s"}

	// Each message introduces a brand-new tag key so the union would grow
	// unboundedly without a cap.
	for i := 0; i < tagCap+5; i++ {
		raw := []byte(fmt.Sprintf("m,tag%d=v f=1 100\n", i))
		_, err := sm.ProcessMessageFrames(
			kafka_client.KafkaMessage{RawValue: raw, Offset: int64(i), Timestamp: time.Now()},
			0, []int32{0}, cfg, "topic",
		)
		if err != nil {
			t.Fatalf("message %d err: %v", i, err)
		}
	}

	if got := len(sm.lpTagKeyOrder); got != tagCap {
		t.Errorf("tag-key union should be capped at %d, got %d", tagCap, got)
	}
}
