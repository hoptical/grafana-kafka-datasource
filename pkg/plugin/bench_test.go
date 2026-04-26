package plugin

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// buildNestedMap creates a nested map[string]interface{} of the given depth
// and total leaf count. Each leaf is a float64.
func buildNestedMap(depth, totalLeaves int) map[string]interface{} {
	if depth <= 1 {
		m := make(map[string]interface{}, totalLeaves)
		for i := 0; i < totalLeaves; i++ {
			m[fmt.Sprintf("field_%d", i)] = float64(i) * 1.1
		}
		return m
	}
	leavesPerBranch := totalLeaves / depth
	if leavesPerBranch < 1 {
		leavesPerBranch = 1
	}
	m := make(map[string]interface{}, depth)
	for b := 0; b < depth; b++ {
		m[fmt.Sprintf("branch_%d", b)] = buildNestedMap(depth-1, leavesPerBranch)
	}
	return m
}

// buildJSONBytes returns a flat JSON object with n numeric fields encoded as
// []byte, ready to pass to a decoder. Used for decode benchmarks.
func buildJSONBytes(numFields, targetSizeBytes int) []byte {
	m := make(map[string]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		m[fmt.Sprintf("metric_%d", i)] = float64(i) * 0.123
	}
	if targetSizeBytes > 0 {
		padLen := targetSizeBytes - numFields*20
		if padLen > 0 {
			m["_padding"] = make([]byte, padLen) // marshals as a base64 string
		}
	}
	b, _ := json.Marshal(m)
	return b
}

// newFrameForBench returns a pre-allocated data.Frame with n fields.
func newFrameForBench(n int) *data.Frame {
	f := data.NewFrame("bench")
	f.Fields = make([]*data.Field, n)
	return f
}

// ---------------------------------------------------------------------------
// FlattenJSON benchmarks (Scenario E)
// ---------------------------------------------------------------------------

func BenchmarkFlattenJSON_Depth1_Fields10(b *testing.B) {
	in := buildNestedMap(1, 10)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

func BenchmarkFlattenJSON_Depth3_Fields10(b *testing.B) {
	in := buildNestedMap(3, 10)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

func BenchmarkFlattenJSON_Depth5_Fields10(b *testing.B) {
	in := buildNestedMap(5, 10)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

func BenchmarkFlattenJSON_Depth3_Fields100(b *testing.B) {
	in := buildNestedMap(3, 100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

func BenchmarkFlattenJSON_Depth3_Fields500(b *testing.B) {
	in := buildNestedMap(3, 500)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

func BenchmarkFlattenJSON_Depth3_Fields1000(b *testing.B) {
	in := buildNestedMap(3, 1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

// Cap boundary: produce exactly fieldCap+1 leaves and verify silent drop.
func BenchmarkFlattenJSON_CapBoundary_1001Fields(b *testing.B) {
	in := buildNestedMap(1, 1001)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, 5, 1000)
	}
}

// ---------------------------------------------------------------------------
// JSON decode benchmarks (Scenario C/D)
// ---------------------------------------------------------------------------

func BenchmarkDecodeJSON_Flat10Fields_100B(b *testing.B) {
	raw := buildJSONBytes(10, 100)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		_ = json.Unmarshal(raw, &doc)
	}
}

func BenchmarkDecodeJSON_Flat10Fields_1KB(b *testing.B) {
	raw := buildJSONBytes(10, 1024)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		_ = json.Unmarshal(raw, &doc)
	}
}

func BenchmarkDecodeJSON_Flat10Fields_10KB(b *testing.B) {
	raw := buildJSONBytes(10, 10240)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		_ = json.Unmarshal(raw, &doc)
	}
}

func BenchmarkDecodeJSON_Flat10Fields_100KB(b *testing.B) {
	raw := buildJSONBytes(10, 102400)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		_ = json.Unmarshal(raw, &doc)
	}
}

func BenchmarkDecodeJSON_Flat100Fields_1KB(b *testing.B) {
	raw := buildJSONBytes(100, 1024)
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		_ = json.Unmarshal(raw, &doc)
	}
}

// ---------------------------------------------------------------------------
// FieldBuilder benchmarks (lock overhead analysis)
// ---------------------------------------------------------------------------

// BenchmarkFieldBuilder_10Fields measures AddValueToFrame per-field write-lock
// overhead for a flat 10-field message. This is the per-message hot-path.
func BenchmarkFieldBuilder_10Fields(b *testing.B) {
	const numFields = 10
	keys := make([]string, numFields)
	vals := make([]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		keys[i] = fmt.Sprintf("field_%d", i)
		vals[i] = float64(i) * 1.1
	}

	fb := NewFieldBuilder()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frame := newFrameForBench(numFields)
		for j := 0; j < numFields; j++ {
			fb.AddValueToFrame(frame, keys[j], vals[j], j)
		}
	}
}

func BenchmarkFieldBuilder_100Fields(b *testing.B) {
	const numFields = 100
	keys := make([]string, numFields)
	vals := make([]interface{}, numFields)
	for i := 0; i < numFields; i++ {
		keys[i] = fmt.Sprintf("field_%d", i)
		vals[i] = float64(i) * 1.1
	}

	fb := NewFieldBuilder()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frame := newFrameForBench(numFields)
		for j := 0; j < numFields; j++ {
			fb.AddValueToFrame(frame, keys[j], vals[j], j)
		}
	}
}

// BenchmarkFieldBuilder_MixedTypes exercises the full type-switch in AddValueToFrame.
func BenchmarkFieldBuilder_MixedTypes_10Fields(b *testing.B) {
	keys := []string{"str", "f64", "i64", "bool", "nil", "jsnum_int", "jsnum_flt", "uint", "i32", "f32"}
	vals := []interface{}{
		"hello",
		float64(3.14),
		int64(42),
		true,
		nil,
		json.Number("99"),
		json.Number("1.5"),
		uint64(7),
		int32(8),
		float32(9.9),
	}

	fb := NewFieldBuilder()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		frame := newFrameForBench(len(keys))
		for j, k := range keys {
			fb.AddValueToFrame(frame, k, vals[j], j)
		}
	}
}

// ---------------------------------------------------------------------------
// Full flatten + frame-build pipeline (simulates ProcessMessage hot-path)
// ---------------------------------------------------------------------------

// BenchmarkPipeline_10Fields_1KB simulates the per-message cost at the drain
// goroutine level: JSON unmarshal → FlattenJSON → FieldBuilder × N fields.
func BenchmarkPipeline_10Fields_1KB(b *testing.B) {
	raw := buildJSONBytes(10, 1024)
	fb := NewFieldBuilder()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		if err := json.Unmarshal(raw, &doc); err != nil {
			b.Fatal(err)
		}
		flat := make(map[string]interface{})
		FlattenJSON("", doc, flat, 0, 5, 1000)
		frame := newFrameForBench(len(flat))
		idx := 0
		for k, v := range flat {
			fb.AddValueToFrame(frame, k, v, idx)
			idx++
		}
	}
}

func BenchmarkPipeline_100Fields_1KB(b *testing.B) {
	raw := buildJSONBytes(100, 1024)
	fb := NewFieldBuilder()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		if err := json.Unmarshal(raw, &doc); err != nil {
			b.Fatal(err)
		}
		flat := make(map[string]interface{})
		FlattenJSON("", doc, flat, 0, 5, 1000)
		frame := newFrameForBench(len(flat))
		idx := 0
		for k, v := range flat {
			fb.AddValueToFrame(frame, k, v, idx)
			idx++
		}
	}
}

func BenchmarkPipeline_10Fields_10KB(b *testing.B) {
	raw := buildJSONBytes(10, 10240)
	fb := NewFieldBuilder()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var doc interface{}
		if err := json.Unmarshal(raw, &doc); err != nil {
			b.Fatal(err)
		}
		flat := make(map[string]interface{})
		FlattenJSON("", doc, flat, 0, 5, 1000)
		frame := newFrameForBench(len(flat))
		idx := 0
		for k, v := range flat {
			fb.AddValueToFrame(frame, k, v, idx)
			idx++
		}
	}
}
