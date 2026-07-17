package plugin

import (
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// newBenchFrame returns a frame with a pre-allocated Fields slice, matching
// the calling convention documented on FieldBuilder.AddValueToFrame (frame.Fields
// must be pre-allocated to at least fieldIndex+1 length).
func newBenchFrame(n int) *data.Frame {
	frame := data.NewFrame("bench")
	frame.Fields = make([]*data.Field, n)
	return frame
}

func BenchmarkAddValueToFrame_Float64(b *testing.B) {
	fb := NewFieldBuilder()
	frame := newBenchFrame(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fb.AddValueToFrame(frame, "temperature", float64(21.5), 0)
	}
}

func BenchmarkAddValueToFrame_Int64(b *testing.B) {
	fb := NewFieldBuilder()
	frame := newBenchFrame(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fb.AddValueToFrame(frame, "count", int64(42), 0)
	}
}

func BenchmarkAddValueToFrame_String(b *testing.B) {
	fb := NewFieldBuilder()
	frame := newBenchFrame(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fb.AddValueToFrame(frame, "status", "ok", 0)
	}
}

func BenchmarkAddValueToFrame_Bool(b *testing.B) {
	fb := NewFieldBuilder()
	frame := newBenchFrame(1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fb.AddValueToFrame(frame, "active", true, 0)
	}
}

func BenchmarkAddValueToFrame_Nil(b *testing.B) {
	fb := NewFieldBuilder()
	frame := newBenchFrame(1)
	// Prime the type registry so the nil path exercises the "existing type" branch.
	fb.AddValueToFrame(frame, "sensor", float64(1), 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fb.AddValueToFrame(frame, "sensor", nil, 0)
	}
}

// BenchmarkAddValueToFrame_ManyFields simulates a realistic per-message cost:
// n distinct fields added to one frame, as ProcessMessage does in a loop.
func BenchmarkAddValueToFrame_ManyFields(b *testing.B) {
	const numFields = 20
	fb := NewFieldBuilder()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frame := newBenchFrame(numFields)
		for f := 0; f < numFields; f++ {
			fb.AddValueToFrame(frame, fieldNameFor(f), float64(f), f)
		}
	}
}

func fieldNameFor(i int) string {
	names := [...]string{
		"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9",
		"f10", "f11", "f12", "f13", "f14", "f15", "f16", "f17", "f18", "f19",
	}
	return names[i%len(names)]
}
