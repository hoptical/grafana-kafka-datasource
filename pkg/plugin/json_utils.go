package plugin

import (
	"encoding/json"
	"fmt"
)

// FlattenJSON flattens a nested JSON-like structure into a flat map with dotted keys.
// It respects a maximum depth and a maximum number of fields to avoid overload.
func FlattenJSON(prefix string, in interface{}, out map[string]interface{}, depth, maxDepth, fieldCap int) int {
	if len(out) >= fieldCap {
		return len(out)
	}
	if depth >= maxDepth {
		return len(out)
	}
	switch val := in.(type) {
	case map[string]interface{}:
		for k, v := range val {
			if len(out) >= fieldCap {
				break
			}
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			FlattenJSON(key, v, out, depth+1, maxDepth, fieldCap)
		}
	case []interface{}:
		// Represent arrays as stringified JSON to keep plugin schema-free.
		if prefix != "" && len(out) < fieldCap {
			out[prefix] = fmt.Sprintf("%v", val)
		}
	default:
		if prefix != "" && len(out) < fieldCap {
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

// toInt64 converts various integer types to int64.
func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	default:
		return 0
	}
}

// toUint64 converts various unsigned integer types to uint64.
func toUint64(v interface{}) uint64 {
	switch n := v.(type) {
	case uint:
		return uint64(n)
	case uint8:
		return uint64(n)
	case uint16:
		return uint64(n)
	case uint32:
		return uint64(n)
	case uint64:
		return n
	default:
		return 0
	}
}
