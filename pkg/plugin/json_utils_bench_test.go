package plugin

import (
	"encoding/json"
	"fmt"
	"testing"
)

// buildFlatJSON returns a flat (single-level) JSON object with n fields,
// mixing value types the way a typical sensor/metrics payload would.
func buildFlatJSON(n int) map[string]interface{} {
	out := make(map[string]interface{}, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			out[fmt.Sprintf("field_%d", i)] = float64(i) * 1.5
		case 1:
			out[fmt.Sprintf("field_%d", i)] = int64(i)
		case 2:
			out[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value-%d", i)
		case 3:
			out[fmt.Sprintf("field_%d", i)] = i%2 == 0
		}
	}
	return out
}

// buildNestedJSON returns a JSON object nested `depth` levels deep, with
// `width` fields at each level (only the last level holds leaf values).
func buildNestedJSON(depth, width int) map[string]interface{} {
	if depth <= 0 {
		leaf := make(map[string]interface{}, width)
		for i := 0; i < width; i++ {
			leaf[fmt.Sprintf("leaf_%d", i)] = float64(i)
		}
		return leaf
	}
	out := make(map[string]interface{}, width)
	for i := 0; i < width; i++ {
		out[fmt.Sprintf("level_%d", i)] = buildNestedJSON(depth-1, width)
	}
	return out
}

// buildListJSON returns a JSON object with a single field holding a list of n elements.
func buildListJSON(n int) map[string]interface{} {
	list := make([]interface{}, n)
	for i := 0; i < n; i++ {
		list[i] = map[string]interface{}{"idx": i, "name": fmt.Sprintf("item-%d", i)}
	}
	return map[string]interface{}{"items": list}
}

func BenchmarkFlattenJSON_Flat10(b *testing.B) {
	benchmarkFlattenJSON(b, buildFlatJSON(10))
}

func BenchmarkFlattenJSON_Flat50(b *testing.B) {
	benchmarkFlattenJSON(b, buildFlatJSON(50))
}

func BenchmarkFlattenJSON_Nested(b *testing.B) {
	// depth=3, width=5 => 5^3 = 125 leaf-ish keys
	benchmarkFlattenJSON(b, buildNestedJSON(3, 5))
}

func BenchmarkFlattenJSON_List100(b *testing.B) {
	benchmarkFlattenJSON(b, buildListJSON(100))
}

func benchmarkFlattenJSON(b *testing.B, in map[string]interface{}) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := make(map[string]interface{})
		FlattenJSON("", in, out, 0, defaultFlattenMaxDepth, defaultFlattenFieldCap)
	}
}

// BenchmarkDecodeTopLevelJSON measures the cost of the top-level JSON decode
// path used when a message's Value hasn't been pre-decoded (decodeTopLevelJSON
// in stream_manager.go), including json.Number allocation via UseNumber().
func BenchmarkDecodeTopLevelJSON(b *testing.B) {
	payload := buildFlatJSON(20)
	raw, err := json.Marshal(payload)
	if err != nil {
		b.Fatalf("failed to marshal fixture: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeTopLevelJSON(raw); err != nil {
			b.Fatalf("decodeTopLevelJSON failed: %v", err)
		}
	}
}
