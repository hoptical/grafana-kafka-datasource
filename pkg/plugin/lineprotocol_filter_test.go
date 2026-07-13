package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
)

// All three filters are ANDed. Empty filter on an axis = no constraint.

func TestLineProtocolFilter_NoFiltersIncludesEverything(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
	})
	if got := fieldByName(frames[0], "_field").Len(); got != 4 {
		t.Errorf("no filter should yield 4 rows, got %d", got)
	}
}

func TestLineProtocolFilter_MeasurementWhitelist(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "Breaker Data",
	})
	f := frames[0]
	m := fieldByName(f, "_measurement")
	if m.Len() != 2 {
		t.Fatalf("want 2 rows from Breaker Data, got %d", m.Len())
	}
	for i := 0; i < m.Len(); i++ {
		if strAt(m, i) != "Breaker Data" {
			t.Errorf("row %d _measurement: %q", i, strAt(m, i))
		}
	}
}

func TestLineProtocolFilter_MeasurementWhitelistAcceptsCommaList(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "Breaker Data, Last Trip",
	})
	if got := fieldByName(frames[0], "_measurement").Len(); got != 4 {
		t.Errorf("want 4 rows (both measurements), got %d", got)
	}
}

func TestLineProtocolFilter_FieldWhitelist(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolFields:             "PT Primary",
	})
	f := frames[0]
	fk := fieldByName(f, "_field")
	if fk.Len() != 1 {
		t.Fatalf("want 1 row matching PT Primary, got %d", fk.Len())
	}
	if strAt(fk, 0) != "PT Primary" {
		t.Errorf("_field: %q", strAt(fk, 0))
	}
}

func TestLineProtocolFilter_TagEquals(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolTags:               "Building=DCM102, Device_tag=-XQ002",
	})
	// Only the 'Breaker Data' line has both Building=DCM102 AND Device_tag=-XQ002.
	if got := fieldByName(frames[0], "_field").Len(); got != 2 {
		t.Errorf("tag filter: want 2 rows, got %d", got)
	}
}

func TestLineProtocolFilter_TagEqualsNonMatchDropsLine(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolTags:               "Building=NoSuch",
	})
	if len(frames) != 0 {
		t.Errorf("tag filter with no matches should produce 0 frames, got %d", len(frames))
	}
}

func TestLineProtocolFilter_AllAxesAndedTogether(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "Breaker Data",
		LineProtocolFields:             "PT Primary",
		LineProtocolTags:               "Device_tag=-XQ002",
	})
	f := frames[0]
	if got := fieldByName(f, "_field").Len(); got != 1 {
		t.Fatalf("AND filter: want 1 row, got %d", got)
	}
	if strAt(fieldByName(f, "_measurement"), 0) != "Breaker Data" {
		t.Errorf("_measurement wrong")
	}
	if strAt(fieldByName(f, "_field"), 0) != "PT Primary" {
		t.Errorf("_field wrong")
	}
}

func TestLineProtocolFilter_EmptyFieldFilterIgnored(t *testing.T) {
	// Whitespace-only filter shouldn't drop everything.
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "   ",
		LineProtocolFields:             ", ,",
		LineProtocolTags:               "",
	})
	if got := fieldByName(frames[0], "_field").Len(); got != 4 {
		t.Errorf("whitespace-only filters should be inert, got %d rows", got)
	}
}

func TestLineProtocolFilter_MeasurementRegex(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "Breaker.*",
	})
	m := fieldByName(frames[0], "_measurement")
	if m.Len() != 2 {
		t.Fatalf("regex Breaker.* should match 'Breaker Data' (2 fields), got %d rows", m.Len())
	}
	for i := 0; i < m.Len(); i++ {
		if strAt(m, i) != "Breaker Data" {
			t.Errorf("row %d _measurement: %q", i, strAt(m, i))
		}
	}
}

func TestLineProtocolFilter_FieldRegex(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolFields:             "PT.*",
	})
	fk := fieldByName(frames[0], "_field")
	if fk.Len() != 1 {
		t.Fatalf("regex PT.* should match 'PT Primary' only, got %d rows", fk.Len())
	}
	if strAt(fk, 0) != "PT Primary" {
		t.Errorf("_field: %q", strAt(fk, 0))
	}
}

func TestLineProtocolFilter_TagValueRegex(t *testing.T) {
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolTags:               "Device_tag=-XQ00[12]",
	})
	// Both lines match: one has -XQ001, the other -XQ002.
	if got := fieldByName(frames[0], "_field").Len(); got != 4 {
		t.Errorf("regex tag filter -XQ00[12]: want 4 rows (both lines), got %d", got)
	}
}

func TestLineProtocolFilter_InvalidRegexSkipped(t *testing.T) {
	// An invalid regex entry should be silently skipped (not crash or block all).
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "[invalid, Breaker Data",
	})
	// The invalid pattern "[invalid" is skipped; "Breaker Data" still matches.
	m := fieldByName(frames[0], "_measurement")
	if m.Len() != 2 {
		t.Fatalf("invalid regex skipped: want 2 rows from 'Breaker Data', got %d", m.Len())
	}
}

func TestLineProtocolFilter_PlainEntryIsExactMatch(t *testing.T) {
	// "Breaker" is a substring of "Breaker Data" but not an exact match, so
	// with anchored patterns it must NOT match (would have matched as a loose
	// substring before anchoring).
	frames := runFilterCase(t, &StreamConfig{
		MessageFormat:                  "lineprotocol",
		TimestampMode:                  "message",
		LineProtocolTimestampPrecision: "s",
		LineProtocolMeasurements:       "Breaker",
	})
	if len(frames) != 0 {
		t.Errorf("plain 'Breaker' must not match 'Breaker Data' (anchored), got %d frames", len(frames))
	}
}

// runFilterCase produces a frame from a fixed two-LP-line payload using the
// given config. The payload has:
//
//	Last Trip,Building=DCM102,Device_tag=-XQ001 a=1,b=2 100
//	Breaker Data,Building=DCM102,Device_tag=-XQ002 PT Primary=46.37,Frequency=50 100
func runFilterCase(t *testing.T, cfg *StreamConfig) []*data.Frame {
	t.Helper()
	raw := []byte("Last\\ Trip,Building=DCM102,Device_tag=-XQ001 a=1,b=2 100\nBreaker\\ Data,Building=DCM102,Device_tag=-XQ002 PT\\ Primary=46.37,Frequency=50 100\n")
	sm := NewStreamManager(&mockStreamClient{}, 5, 1000)
	frames, err := sm.ProcessMessageFrames(
		kafka_client.KafkaMessage{RawValue: raw, Offset: 0, Timestamp: time.Now()},
		0, []int32{0}, cfg, "topic",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return frames
}
