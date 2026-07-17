package plugin

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildSyntheticLineProtocol returns raw line-protocol bytes containing
// numLines records, each with numTags tags and numFields fields, matching
// the shape produced by typical IoT/metrics producers.
func buildSyntheticLineProtocol(numLines, numTags, numFields int) []byte {
	var buf bytes.Buffer
	for i := 0; i < numLines; i++ {
		buf.WriteString("cpu")
		for t := 0; t < numTags; t++ {
			fmt.Fprintf(&buf, ",tag%d=value%d", t, i%10)
		}
		buf.WriteByte(' ')
		for f := 0; f < numFields; f++ {
			if f > 0 {
				buf.WriteByte(',')
			}
			fmt.Fprintf(&buf, "field%d=%d", f, i*f)
		}
		fmt.Fprintf(&buf, " %d\n", 1_700_000_000_000_000_000+int64(i))
	}
	return buf.Bytes()
}

func BenchmarkParseLines_Synthetic10Lines(b *testing.B) {
	benchmarkParseLines(b, buildSyntheticLineProtocol(10, 3, 5))
}

func BenchmarkParseLines_Synthetic100Lines(b *testing.B) {
	benchmarkParseLines(b, buildSyntheticLineProtocol(100, 3, 5))
}

func BenchmarkParseLines_RealSample(b *testing.B) {
	raw, err := os.ReadFile(filepath.Join("testdata", "lineprotocol", "real_sample.txt"))
	if err != nil {
		b.Fatalf("failed to read fixture: %v", err)
	}
	benchmarkParseLines(b, raw)
}

func BenchmarkParseLines_FullRealSample(b *testing.B) {
	raw, err := os.ReadFile(filepath.Join("testdata", "lineprotocol", "full_real_sample.txt"))
	if err != nil {
		b.Fatalf("failed to read fixture: %v", err)
	}
	benchmarkParseLines(b, raw)
}

func benchmarkParseLines(b *testing.B, raw []byte) {
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		if _, errs := ParseLines(raw); len(errs) > 0 {
			b.Fatalf("unexpected parse errors: %v", errs)
		}
	}
}
