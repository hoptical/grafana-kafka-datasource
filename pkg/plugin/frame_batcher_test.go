package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func TestFrameMicroBatcher_MergesRowsSameSchema(t *testing.T) {
	b := newFrameMicroBatcher(3)

	f1 := testFrame("response", "A", 1)
	f2 := testFrame("response", "A", 2)
	f3 := testFrame("response", "A", 3)

	ready, err := b.AddFrames([]*data.Frame{f1})
	if err != nil {
		t.Fatalf("AddFrames(1) failed: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected no flush after 1 row, got %d frames", len(ready))
	}

	ready, err = b.AddFrames([]*data.Frame{f2})
	if err != nil {
		t.Fatalf("AddFrames(2) failed: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("expected no flush after 2 rows, got %d frames", len(ready))
	}

	ready, err = b.AddFrames([]*data.Frame{f3})
	if err != nil {
		t.Fatalf("AddFrames(3) failed: %v", err)
	}
	if len(ready) != 1 {
		t.Fatalf("expected 1 flushed frame, got %d", len(ready))
	}
	if ready[0].Rows() != 3 {
		t.Fatalf("expected 3 rows, got %d", ready[0].Rows())
	}

	if v, _ := ready[0].ConcreteAt(2, 0); v != int64(1) {
		t.Fatalf("row0 value mismatch: got %v", v)
	}
	if v, _ := ready[0].ConcreteAt(2, 1); v != int64(2) {
		t.Fatalf("row1 value mismatch: got %v", v)
	}
	if v, _ := ready[0].ConcreteAt(2, 2); v != int64(3) {
		t.Fatalf("row2 value mismatch: got %v", v)
	}
}

func TestFrameMicroBatcher_SeparatesDifferentSchemas(t *testing.T) {
	b := newFrameMicroBatcher(10)

	f1 := testFrame("response", "A", 10)
	f2 := testFrameWithDisplay("response", "A", 20, "partition-1")

	if _, err := b.AddFrames([]*data.Frame{f1, f2}); err != nil {
		t.Fatalf("AddFrames failed: %v", err)
	}

	out := b.Flush()
	if len(out) != 2 {
		t.Fatalf("expected 2 flushed frames, got %d", len(out))
	}
	if out[0].Rows() != 1 || out[1].Rows() != 1 {
		t.Fatalf("expected each flushed frame to contain 1 row, got rows=%d,%d", out[0].Rows(), out[1].Rows())
	}
}

func testFrame(name, refID string, value int64) *data.Frame {
	ts := time.Unix(1700000000, 0).UTC()
	return data.NewFrame(name,
		data.NewField("time", nil, []time.Time{ts}),
		data.NewField("partition", nil, []int32{0}),
		data.NewField("value", nil, []int64{value}),
	).SetRefID(refID)
}

func testFrameWithDisplay(name, refID string, value int64, display string) *data.Frame {
	f := testFrame(name, refID, value)
	for _, field := range f.Fields {
		if field.Name == "time" {
			continue
		}
		field.Config = &data.FieldConfig{DisplayNameFromDS: display}
	}
	return f
}
