package plugin

import (
	"encoding/json"
	"fmt"
)

// FlattenJSON flattens a nested JSON-like structure into a flat map with dotted keys.
// It respects a maximum depth and a maximum number of fields to avoid overload.
func FlattenJSON(prefix string, in interface{}, out map[string]interface{}, depth, maxDepth, cap int) int {
	if len(out) >= cap {
		return len(out)
	}
	if depth > maxDepth {
		return len(out)
	}
	switch val := in.(type) {
	case map[string]interface{}:
		for k, v := range val {
			if len(out) >= cap {
				break
			}
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			FlattenJSON(key, v, out, depth+1, maxDepth, cap)
		}
	case []interface{}:
		// Represent arrays as stringified JSON to keep plugin schema-free.
		if prefix != "" && len(out) < cap {
			out[prefix] = fmt.Sprintf("%v", val)
		}
	default:
		if prefix != "" && len(out) < cap {
			out[prefix] = val
		}
	}
	return len(out)
}

// CoerceJSONNumber tries to preserve integer numbers as int64; otherwise returns float64.
// If parsing fails, returns the original json.Number as string.
func CoerceJSONNumber(num json.Number) interface{} {
	if i, err := num.Int64(); err == nil {
		return i
	}
	if f, err := num.Float64(); err == nil {
		return f
	}
	return num.String()
}
