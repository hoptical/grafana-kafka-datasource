package plugin

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestParseLines_BasicSingleLine covers the simplest valid line.
func TestParseLines_BasicSingleLine(t *testing.T) {
	in := []byte(`weather,location=us field=82 1465839830100400200`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected parse errors: %v", errs)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 line, got %d", len(got))
	}
	line := got[0]
	if line.Measurement != "weather" {
		t.Errorf("Measurement: want weather, got %q", line.Measurement)
	}
	if !tagEqual(line.Tags, []TagKV{{"location", "us"}}) {
		t.Errorf("Tags: want [{location us}], got %v", line.Tags)
	}
	if len(line.Fields) != 1 || line.Fields[0].Key != "field" {
		t.Fatalf("Fields: want one field 'field', got %v", line.Fields)
	}
	if f, ok := line.Fields[0].Value.(float64); !ok || f != 82 {
		t.Errorf("Field value: want float64(82), got %T(%v)", line.Fields[0].Value, line.Fields[0].Value)
	}
	if !line.HasTimestamp || line.Timestamp != 1465839830100400200 {
		t.Errorf("Timestamp: want 1465839830100400200, got %d (hasTs=%v)", line.Timestamp, line.HasTimestamp)
	}
}

// TestParseLines_FieldTypes covers all five field types per the InfluxDB Line Protocol spec.
func TestParseLines_FieldTypes(t *testing.T) {
	in := []byte(`m f1=1.5,f2=42i,f3=42u,f4=t,f5="hello",f6=true,f7=F,f8=-7,f9=-7i,f10=1.2e6`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected parse errors: %v", errs)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 line, got %d", len(got))
	}
	want := map[string]interface{}{
		"f1":  float64(1.5),
		"f2":  int64(42),
		"f3":  uint64(42),
		"f4":  true,
		"f5":  "hello",
		"f6":  true,
		"f7":  false,
		"f8":  float64(-7), // unsuffixed → float64 per spec
		"f9":  int64(-7),
		"f10": float64(1.2e6),
	}
	gotMap := fieldsToMap(got[0].Fields)
	if !reflect.DeepEqual(gotMap, want) {
		t.Errorf("Fields mismatch:\n want %v\n got  %v", want, gotMap)
	}
}

// TestParseLines_Escapes covers all escape sequences from the InfluxDB Line Protocol spec.
func TestParseLines_Escapes(t *testing.T) {
	// Measurement: spaces and commas must be escaped.
	// Tag/Field keys + tag values: spaces, commas, equals must be escaped.
	// Quoted field-string values: only `"` and `\` are escaped.
	in := []byte(`my\ measurement,t\,ag\=k=t\ a\,g\=v field\ key="a \"quoted\" \\value with, and = and \ ok",other=1i`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	line := got[0]
	if line.Measurement != "my measurement" {
		t.Errorf("Measurement: want %q, got %q", "my measurement", line.Measurement)
	}
	if !tagEqual(line.Tags, []TagKV{{"t,ag=k", "t a,g=v"}}) {
		t.Errorf("Tags: got %v", line.Tags)
	}
	wantFields := map[string]interface{}{
		"field key": `a "quoted" \value with, and = and \ ok`,
		"other":     int64(1),
	}
	gotMap := fieldsToMap(line.Fields)
	if !reflect.DeepEqual(gotMap, wantFields) {
		t.Errorf("Fields mismatch:\n want %v\n got  %v", wantFields, gotMap)
	}
}

// TestParseLines_NoTimestamp ensures lines without trailing timestamp are accepted.
func TestParseLines_NoTimestamp(t *testing.T) {
	in := []byte(`m,t=v field=1`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if got[0].HasTimestamp {
		t.Errorf("expected HasTimestamp=false, got true with ts=%d", got[0].Timestamp)
	}
}

// TestParseLines_NoTags ensures lines with measurement+fields only (no tags) parse correctly.
func TestParseLines_NoTags(t *testing.T) {
	in := []byte(`m f=1 100`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(got[0].Tags) != 0 {
		t.Errorf("expected 0 tags, got %v", got[0].Tags)
	}
	if got[0].Measurement != "m" {
		t.Errorf("Measurement: want m, got %q", got[0].Measurement)
	}
	if got[0].Timestamp != 100 {
		t.Errorf("Timestamp: want 100, got %d", got[0].Timestamp)
	}
}

// TestParseLines_MultipleLines covers newline-separated batch messages.
func TestParseLines_MultipleLines(t *testing.T) {
	in := []byte("a f=1 100\nb f=2 200\nc f=3 300\n")

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 lines, got %d", len(got))
	}
	for i, m := range []string{"a", "b", "c"} {
		if got[i].Measurement != m {
			t.Errorf("line %d: want %q, got %q", i, m, got[i].Measurement)
		}
	}
}

// TestParseLines_CommentsAndBlanks ensures `#` comments and blank lines are ignored.
func TestParseLines_CommentsAndBlanks(t *testing.T) {
	in := []byte("# this is a comment\n\na f=1\n   \n# another\nb f=2\n")

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 parsed lines, got %d", len(got))
	}
	if got[0].Measurement != "a" || got[1].Measurement != "b" {
		t.Errorf("want a,b — got %q,%q", got[0].Measurement, got[1].Measurement)
	}
}

// TestParseLines_HashIsValidFieldKey verifies '#' is only a comment at line start, not mid-line.
// The real-world payload contains `#=1` as a legitimate field key.
func TestParseLines_HashIsValidFieldKey(t *testing.T) {
	in := []byte(`m,t=v #=1,Auto\ Mode=0 1779186714`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	want := map[string]interface{}{
		"#":         float64(1),
		"Auto Mode": float64(0),
	}
	if !reflect.DeepEqual(fieldsToMap(got[0].Fields), want) {
		t.Errorf("Fields mismatch: %v", got[0].Fields)
	}
}

// TestParseLines_MalformedDoesNotAbortBatch ensures one bad line still yields others.
func TestParseLines_MalformedDoesNotAbortBatch(t *testing.T) {
	in := []byte("good f=1 100\nbroken\ngood2 f=2 200\n")

	got, errs := ParseLines(in)
	if len(got) != 2 {
		t.Errorf("want 2 good lines, got %d", len(got))
	}
	if len(errs) != 1 {
		t.Errorf("want 1 error for malformed line, got %d: %v", len(errs), errs)
	}
}

// TestParseLines_RealPayload checks a sanitized version of the user's actual Kafka payload.
func TestParseLines_RealPayload(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "lineprotocol", "real_sample.txt"))
	if err != nil {
		t.Fatalf("could not read real_sample.txt: %v", err)
	}

	got, errs := ParseLines(raw)
	if len(errs) != 0 {
		t.Fatalf("unexpected parse errors: %v", errs)
	}
	if len(got) < 6 {
		t.Fatalf("want at least 6 lines from real_sample.txt, got %d", len(got))
	}

	// First line is "Last Trip".
	first := got[0]
	if first.Measurement != "Last Trip" {
		t.Errorf("first measurement: want %q, got %q", "Last Trip", first.Measurement)
	}
	// All lines should share these tags.
	wantTags := map[string]string{
		"Building":          "DCM102",
		"Dashboard":         "HiBreaker",
		"Description":       "White space busbar",
		"Device_tag":        "-XQ202",
		"Equipment-tag":     "N01_DCM102_462_100_XQ202-id3-1",
		"Floor":             "1",
		"Full_tag":          "+N01_DCM102_=462.100_-XQ202",
		"Gapit-product-code": "02/gapit-02-03-std.json",
		"Module":            "207103",
		"POD":               "X",
		"POD_nr":            "POD207103",
		"Site":              "+N01",
		"System":            "=462.100",
		"uid":               "ada72705-d21d-4dd0-aeac-459d47e88365",
	}
	gotTags := tagsToMap(first.Tags)
	if !reflect.DeepEqual(gotTags, wantTags) {
		t.Errorf("Tags mismatch on line 0:\n want %v\n got  %v", wantTags, gotTags)
	}

	// First field on "Last Trip" must be an int (suffix 'i').
	firstField := first.Fields[0]
	if firstField.Key != "Last trip event Timestamp" {
		t.Errorf("first field key: want %q, got %q", "Last trip event Timestamp", firstField.Key)
	}
	if v, ok := firstField.Value.(int64); !ok || v != 4523548585 {
		t.Errorf("first field value: want int64(4523548585), got %T(%v)", firstField.Value, firstField.Value)
	}

	// Every line should have HasTimestamp=true with the same epoch-seconds value.
	for i, l := range got {
		if !l.HasTimestamp || l.Timestamp != 1779186714 {
			t.Errorf("line %d: want ts=1779186714, got %d (has=%v)", i, l.Timestamp, l.HasTimestamp)
		}
	}

	// The "Alarm Counter" line should contain a literal "#" field key.
	var alarmCounter *ParsedLine
	for i := range got {
		if got[i].Measurement == "Alarm Counter" {
			alarmCounter = &got[i]
			break
		}
	}
	if alarmCounter == nil {
		t.Fatalf("could not find 'Alarm Counter' line")
	}
	foundHash := false
	for _, f := range alarmCounter.Fields {
		if f.Key == "#" {
			foundHash = true
			if v, ok := f.Value.(float64); !ok || v != 1 {
				t.Errorf("'#' field: want float64(1), got %T(%v)", f.Value, f.Value)
			}
		}
	}
	if !foundHash {
		t.Errorf("expected '#' field in Alarm Counter line")
	}

	// "Time Synchronization" line has a negative integer SNTP fails count (unsuffixed → float).
	var timeSync *ParsedLine
	for i := range got {
		if got[i].Measurement == "Time Synchronization" {
			timeSync = &got[i]
			break
		}
	}
	if timeSync == nil {
		t.Fatalf("could not find 'Time Synchronization' line")
	}
	for _, f := range timeSync.Fields {
		if f.Key == "SNTP fails count" {
			if v, ok := f.Value.(float64); !ok || v != -24931 {
				t.Errorf("SNTP fails count: want float64(-24931), got %T(%v)", f.Value, f.Value)
			}
		}
	}
}

// TestParseLines_QuotedStringContainingSpaces verifies whitespace inside quoted field values
// doesn't terminate the field set.
func TestParseLines_QuotedStringContainingSpaces(t *testing.T) {
	in := []byte(`m,t=v fkey="hello world with spaces" 100`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if v, ok := got[0].Fields[0].Value.(string); !ok || v != "hello world with spaces" {
		t.Errorf("string value: got %T(%v)", got[0].Fields[0].Value, got[0].Fields[0].Value)
	}
	if got[0].Timestamp != 100 {
		t.Errorf("ts: want 100, got %d", got[0].Timestamp)
	}
}

// TestParseLines_FloatPrecisionPreserved verifies float parsing keeps full float64 precision.
func TestParseLines_FloatPrecisionPreserved(t *testing.T) {
	in := []byte(`m f=14.639791488647461`)

	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	v := got[0].Fields[0].Value.(float64)
	if math.Abs(v-14.639791488647461) > 1e-12 {
		t.Errorf("float precision lost: want 14.639791488647461, got %v", v)
	}
}

// TestParseLines_TombstoneNilInput returns nothing without error.
func TestParseLines_TombstoneNilInput(t *testing.T) {
	got, errs := ParseLines(nil)
	if len(got) != 0 || len(errs) != 0 {
		t.Errorf("nil input should yield empty result: got=%d errs=%d", len(got), len(errs))
	}
	got, errs = ParseLines([]byte(""))
	if len(got) != 0 || len(errs) != 0 {
		t.Errorf("empty input should yield empty result: got=%d errs=%d", len(got), len(errs))
	}
}

// TestParseLines_CRLF handles Windows-style line endings.
func TestParseLines_CRLF(t *testing.T) {
	in := []byte("a f=1 100\r\nb f=2 200\r\n")
	got, errs := ParseLines(in)
	if len(errs) != 0 {
		t.Fatalf("errs: %v", errs)
	}
	if len(got) != 2 {
		t.Errorf("want 2 lines, got %d", len(got))
	}
}

// BenchmarkParseLines measures parser throughput on the real Kafka payload.
// Target: well under 1ms per call so a single Kafka consumer goroutine can
// sustain thousands of messages/sec without becoming the bottleneck.
func BenchmarkParseLines(b *testing.B) {
	raw, err := os.ReadFile(filepath.Join("testdata", "lineprotocol", "real_sample.txt"))
	if err != nil {
		b.Fatalf("could not read real_sample.txt: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		_, _ = ParseLines(raw)
	}
}

// ---- helpers ----

func tagEqual(a, b []TagKV) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fieldsToMap(fs []FieldKV) map[string]interface{} {
	m := make(map[string]interface{}, len(fs))
	for _, f := range fs {
		m[f.Key] = f.Value
	}
	return m
}

func tagsToMap(ts []TagKV) map[string]string {
	m := make(map[string]string, len(ts))
	for _, t := range ts {
		m[t.Key] = t.Value
	}
	return m
}
