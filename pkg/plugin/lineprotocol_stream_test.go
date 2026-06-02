package plugin

import (
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
