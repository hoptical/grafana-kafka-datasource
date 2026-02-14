package plugin

import "testing"

func TestUnwrapAvroUnions(t *testing.T) {
	t.Run("null union unwraps to nil", func(t *testing.T) {
		out := UnwrapAvroUnions(map[string]interface{}{"null": nil})
		if out != nil {
			t.Fatalf("expected nil, got %#v", out)
		}
	})

	t.Run("primitive union unwraps value", func(t *testing.T) {
		out := UnwrapAvroUnions(map[string]interface{}{"double": 42.5})
		value, ok := out.(float64)
		if !ok || value != 42.5 {
			t.Fatalf("expected float64 42.5, got %#v", out)
		}
	})

	t.Run("complex union unwraps recursively", func(t *testing.T) {
		out := UnwrapAvroUnions(map[string]interface{}{
			"record": map[string]interface{}{
				"nested": map[string]interface{}{"string": "ok"},
			},
		})
		if out != "ok" {
			t.Fatalf("expected recursive unwrap to collapse to 'ok', got %#v", out)
		}
	})

	t.Run("single-field regular map remains map", func(t *testing.T) {
		out := UnwrapAvroUnions(map[string]interface{}{"value": "x"})
		result, ok := out.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map output, got %#v", out)
		}
		if result["value"] != "x" {
			t.Fatalf("expected regular map to be preserved, got %#v", out)
		}
	})

	t.Run("arrays unwrap recursively", func(t *testing.T) {
		out := UnwrapAvroUnions([]interface{}{
			map[string]interface{}{"string": "a"},
			map[string]interface{}{"null": nil},
			map[string]interface{}{"record": map[string]interface{}{"k": map[string]interface{}{"int": int32(9)}}},
		})

		arr, ok := out.([]interface{})
		if !ok || len(arr) != 3 {
			t.Fatalf("expected 3-element array, got %#v", out)
		}
		if arr[0] != "a" {
			t.Fatalf("expected first element to be 'a', got %#v", arr[0])
		}
		if arr[1] != nil {
			t.Fatalf("expected second element to be nil, got %#v", arr[1])
		}
		third, ok := arr[2].(map[string]interface{})
		if ok {
			t.Fatalf("expected nested recursive unwrap to collapse value, got %#v", third)
		}
		if arr[2] != int32(9) {
			t.Fatalf("expected nested recursive unwrap to return int32(9), got %#v", arr[2])
		}
	})
}
