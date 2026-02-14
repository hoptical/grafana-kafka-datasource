package kafka_client

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestParseCountPrefixedIndexes_Branches(t *testing.T) {
	if indexes, remaining, ok := parseCountPrefixedIndexes(protowire.AppendVarint(nil, 0)); ok || indexes != nil || remaining != nil {
		t.Fatalf("expected count=0 to be rejected")
	}

	tooLarge := protowire.AppendVarint(nil, 11)
	if indexes, remaining, ok := parseCountPrefixedIndexes(tooLarge); ok || indexes != nil || remaining != nil {
		t.Fatalf("expected count>10 to be rejected")
	}

	data := make([]byte, 0)
	data = protowire.AppendVarint(data, 2)
	data = protowire.AppendVarint(data, 3)
	data = protowire.AppendVarint(data, 4)
	data = append(data, 0xaa, 0xbb)

	indexes, remaining, ok := parseCountPrefixedIndexes(data)
	if !ok {
		t.Fatalf("expected valid count-prefixed indexes")
	}
	if len(indexes) != 2 || indexes[0] != 3 || indexes[1] != 4 {
		t.Fatalf("unexpected indexes: %v", indexes)
	}
	if len(remaining) != 2 || remaining[0] != 0xaa || remaining[1] != 0xbb {
		t.Fatalf("unexpected remaining bytes: %v", remaining)
	}
}

func TestParseTerminatedIndexes_Branches(t *testing.T) {
	data := make([]byte, 0)
	data = protowire.AppendVarint(data, 2)
	data = protowire.AppendVarint(data, 5)
	data = protowire.AppendVarint(data, 0)
	data = append(data, 0xcc)

	indexes, remaining, ok := parseTerminatedIndexes(data)
	if !ok {
		t.Fatalf("expected terminated format to parse")
	}
	if len(indexes) != 2 || indexes[0] != 2 || indexes[1] != 5 {
		t.Fatalf("unexpected indexes: %v", indexes)
	}
	if len(remaining) != 1 || remaining[0] != 0xcc {
		t.Fatalf("unexpected remaining bytes: %v", remaining)
	}

	zeroFirst := protowire.AppendVarint(nil, 0)
	if indexes, remaining, ok := parseTerminatedIndexes(zeroFirst); ok || indexes != nil || remaining != nil {
		t.Fatalf("expected first-zero terminated format to be rejected")
	}
}

func TestParseMessageIndexes_FallbackBranches(t *testing.T) {
	data := make([]byte, 0)
	data = protowire.AppendVarint(data, 3)
	data = append(data, 0xdd)

	indexes, remaining, err := parseMessageIndexes(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(indexes) != 1 || indexes[0] != 3 {
		t.Fatalf("expected single-varint fallback index [3], got %v", indexes)
	}
	if len(remaining) != 1 || remaining[0] != 0xdd {
		t.Fatalf("unexpected remaining bytes: %v", remaining)
	}
}

func TestResolveMessageByIndexPath_Branches(t *testing.T) {
	schema := `syntax = "proto3";
	message Parent {
	  message Child {
	    string value = 1;
	  }
	}
	message Other {
	  string name = 1;
	}`

	parsed, err := ParseProtobufSchema(schema)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	msg, err := resolveMessageByIndexPath(parsed.File, []int{0, 0})
	if err != nil {
		t.Fatalf("expected nested path to resolve: %v", err)
	}
	if string(msg.Name()) != "Child" {
		t.Fatalf("expected Child message, got %s", msg.Name())
	}

	if _, err := resolveMessageByIndexPath(parsed.File, []int{}); err == nil {
		t.Fatalf("expected empty path error")
	}
	if _, err := resolveMessageByIndexPath(parsed.File, []int{99}); err == nil {
		t.Fatalf("expected top-level out-of-range error")
	}
	if _, err := resolveMessageByIndexPath(parsed.File, []int{0, 9}); err == nil {
		t.Fatalf("expected nested out-of-range error")
	}
}
