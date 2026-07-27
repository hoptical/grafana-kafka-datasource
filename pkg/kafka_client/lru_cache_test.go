package kafka_client

import (
	"testing"

	"github.com/hoptical/grafana-kafka-datasource/pkg/perfflags"
	"github.com/linkedin/goavro/v2"
)

func TestLRUCache_EvictsLeastRecentlyUsed(t *testing.T) {
	c := newLRUCache[int](2)
	c.Add("a", 1)
	c.Add("b", 2)

	if _, ok := c.Get("a"); !ok {
		t.Fatalf("expected key a to exist")
	}

	c.Add("c", 3)

	if _, ok := c.Get("b"); ok {
		t.Fatalf("expected key b to be evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("expected key a to remain after access")
	}
	if v, ok := c.Get("c"); !ok || v != 3 {
		t.Fatalf("expected key c to exist")
	}
}

func TestCacheSizeFromEnv(t *testing.T) {
	t.Setenv("TEST_CACHE_SIZE", "17")
	if got := cacheSizeFromEnv("TEST_CACHE_SIZE", 4); got != 17 {
		t.Fatalf("expected 17, got %d", got)
	}

	t.Setenv("TEST_CACHE_SIZE", "0")
	if got := cacheSizeFromEnv("TEST_CACHE_SIZE", 4); got != 4 {
		t.Fatalf("expected fallback 4 for invalid low value, got %d", got)
	}

	t.Setenv("TEST_CACHE_SIZE", "bad")
	if got := cacheSizeFromEnv("TEST_CACHE_SIZE", 4); got != 4 {
		t.Fatalf("expected fallback 4 for invalid value, got %d", got)
	}
}

func TestAvroCodecCache_BoundedAndLRU(t *testing.T) {
	oldCache := avroCodecCache
	avroCodecCache = newLRUCache[*goavro.Codec](2)
	defer func() {
		avroCodecCache = oldCache
	}()

	wasDisabled := perfflags.AvroCodecCache.Disabled()
	perfflags.AvroCodecCache.SetDisabledForTest(false)
	defer perfflags.AvroCodecCache.SetDisabledForTest(wasDisabled)

	s1 := `{"type":"record","name":"S1","fields":[{"name":"x","type":"long"}]}`
	s2 := `{"type":"record","name":"S2","fields":[{"name":"x","type":"long"}]}`
	s3 := `{"type":"record","name":"S3","fields":[{"name":"x","type":"long"}]}`

	if _, err := getAvroCodec(s1); err != nil {
		t.Fatalf("getAvroCodec(s1): %v", err)
	}
	if _, err := getAvroCodec(s2); err != nil {
		t.Fatalf("getAvroCodec(s2): %v", err)
	}
	if _, ok := avroCodecCache.Get(s1); !ok {
		t.Fatalf("expected s1 cache hit")
	}
	if _, err := getAvroCodec(s3); err != nil {
		t.Fatalf("getAvroCodec(s3): %v", err)
	}

	if got := avroCodecCache.Len(); got != 2 {
		t.Fatalf("expected cache len 2, got %d", got)
	}
	if _, ok := avroCodecCache.Get(s2); ok {
		t.Fatalf("expected s2 to be evicted as LRU")
	}
}

func TestProtobufSchemaCache_BoundedAndLRU(t *testing.T) {
	oldCache := protobufSchemaCache
	protobufSchemaCache = newLRUCache[*ParsedProtobufSchema](2)
	defer func() {
		protobufSchemaCache = oldCache
	}()

	wasDisabled := perfflags.ProtobufSchemaCache.Disabled()
	perfflags.ProtobufSchemaCache.SetDisabledForTest(false)
	defer perfflags.ProtobufSchemaCache.SetDisabledForTest(wasDisabled)

	s1 := "syntax = \"proto3\"; package p1; message M1 { int64 x = 1; }"
	s2 := "syntax = \"proto3\"; package p2; message M2 { int64 x = 1; }"
	s3 := "syntax = \"proto3\"; package p3; message M3 { int64 x = 1; }"

	if _, err := ParseProtobufSchema(s1); err != nil {
		t.Fatalf("ParseProtobufSchema(s1): %v", err)
	}
	if _, err := ParseProtobufSchema(s2); err != nil {
		t.Fatalf("ParseProtobufSchema(s2): %v", err)
	}
	if _, ok := protobufSchemaCache.Get(s1); !ok {
		t.Fatalf("expected s1 cache hit")
	}
	if _, err := ParseProtobufSchema(s3); err != nil {
		t.Fatalf("ParseProtobufSchema(s3): %v", err)
	}

	if got := protobufSchemaCache.Len(); got != 2 {
		t.Fatalf("expected cache len 2, got %d", got)
	}
	if _, ok := protobufSchemaCache.Get(s2); ok {
		t.Fatalf("expected s2 to be evicted as LRU")
	}
}
