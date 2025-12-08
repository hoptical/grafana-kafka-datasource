package plugin

// UnwrapAvroUnions recursively unwraps Avro union types in a nested structure.
// goavro represents union types like ["null", "double"] as:
// - null: map[string]interface{}{"null": nil}
// - non-null: map[string]interface{}{"double": 42.0}
// This function detects these patterns and unwraps them to their actual values
// throughout the entire structure before flattening.
func UnwrapAvroUnions(in interface{}) interface{} {
	switch val := in.(type) {
	case map[string]interface{}:
		// Check if this is an Avro union wrapper (single-key map)
		if len(val) == 1 {
			// Check for null union
			if _, hasNull := val["null"]; hasNull {
				return nil
			}

			// Check for other Avro types (primitives and complex types)
			for typeName, typeValue := range val {
				switch typeName {
				case "string", "bytes", "int", "long", "float", "double", "boolean":
					// This is an Avro union wrapper with primitive type, return the unwrapped value
					return typeValue
				default:
					// Check if this is an Avro union wrapper with a complex type (record/array/map/named type)
					// Only unwrap if the value itself is a complex structure
					switch typeValue.(type) {
					case map[string]interface{}, []interface{}:
						return UnwrapAvroUnions(typeValue)
					}
					// If it's neither a known primitive nor a complex structure,
					// this might be a legitimate single-field record, so fall through
					// to normal map handling
				}
			}
		}

		// Not a union wrapper, recursively unwrap all values in the map
		result := make(map[string]interface{}, len(val))
		for k, v := range val {
			result[k] = UnwrapAvroUnions(v)
		}
		return result

	case []interface{}:
		// Recursively unwrap array elements
		result := make([]interface{}, len(val))
		for i, v := range val {
			result[i] = UnwrapAvroUnions(v)
		}
		return result

	default:
		// Primitive value, return as-is
		return val
	}
}
