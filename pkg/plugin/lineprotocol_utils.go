package plugin

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// TagKV is a single key/value pair from a line-protocol tag set.
type TagKV struct {
	Key   string
	Value string
}

// FieldKV is a single key/value pair from a line-protocol field set.
// Value's dynamic type is one of: float64, int64, uint64, bool, string —
// chosen to match what plugin.FieldBuilder.AddValueToFrame already handles.
type FieldKV struct {
	Key   string
	Value interface{}
}

// ParsedLine is a single decoded line-protocol record.
type ParsedLine struct {
	Measurement  string
	Tags         []TagKV
	Fields       []FieldKV
	Timestamp    int64
	HasTimestamp bool
}

// ParseLines decodes one or more newline-delimited InfluxDB line-protocol
// records from raw. Blank lines and lines beginning with '#' are skipped.
// A malformed line is reported in errs but does not abort the batch — well-formed
// peers are still returned.
func ParseLines(raw []byte) (lines []ParsedLine, errs []error) {
	if len(raw) == 0 {
		return nil, nil
	}

	// Strip a trailing newline so the final split doesn't yield an empty record.
	raw = bytes.TrimRight(raw, "\n")
	for lineNum, rawLine := range bytes.Split(raw, []byte{'\n'}) {
		// Tolerate CRLF.
		rawLine = bytes.TrimRight(rawLine, "\r")

		trimmed := bytes.TrimSpace(rawLine)
		if len(trimmed) == 0 {
			continue
		}
		if trimmed[0] == '#' {
			continue
		}

		l, err := parseLine(trimmed)
		if err != nil {
			errs = append(errs, fmt.Errorf("line %d: %w", lineNum+1, err))
			continue
		}
		lines = append(lines, l)
	}
	return lines, errs
}

// parseLine parses a single line-protocol record (no surrounding whitespace,
// no terminating newline).
func parseLine(b []byte) (ParsedLine, error) {
	var out ParsedLine
	pos := 0

	// 1. Measurement: read until first unescaped ',' or ' '.
	measurement, term, end, err := readUntilUnescaped(b, pos, ", ")
	if err != nil {
		return out, fmt.Errorf("measurement: %w", err)
	}
	if measurement == "" {
		return out, fmt.Errorf("empty measurement")
	}
	out.Measurement = unescapeIdent(measurement)
	pos = end + 1

	// 2. Optional tag set (only present when measurement ended on ',').
	if term == ',' {
		for {
			key, kterm, kend, err := readUntilUnescaped(b, pos, "=")
			if err != nil {
				return out, fmt.Errorf("tag key: %w", err)
			}
			if kterm != '=' {
				return out, fmt.Errorf("tag %q missing '='", key)
			}
			pos = kend + 1

			val, vterm, vend, err := readUntilUnescaped(b, pos, ", ")
			if err != nil {
				return out, fmt.Errorf("tag value for %q: %w", key, err)
			}
			out.Tags = append(out.Tags, TagKV{
				Key:   unescapeIdent(key),
				Value: unescapeIdent(val),
			})
			pos = vend + 1
			if vterm == ' ' {
				break
			}
			// vterm == ',' → another tag follows
		}
	}

	// 3. Required field set.
	if pos >= len(b) {
		return out, fmt.Errorf("no fields")
	}
	for {
		key, kterm, kend, err := readUntilUnescaped(b, pos, "=")
		if err != nil {
			return out, fmt.Errorf("field key: %w", err)
		}
		if kterm != '=' {
			return out, fmt.Errorf("field %q missing '='", key)
		}
		pos = kend + 1

		rawVal, vterm, vend, err := readFieldValue(b, pos)
		if err != nil {
			return out, fmt.Errorf("field value for %q: %w", key, err)
		}
		fv, err := classifyFieldValue(rawVal)
		if err != nil {
			return out, fmt.Errorf("field %q: %w", key, err)
		}
		out.Fields = append(out.Fields, FieldKV{
			Key:   unescapeIdent(key),
			Value: fv,
		})
		pos = vend + 1
		if vterm == 0 {
			// EOL → no timestamp.
			return out, nil
		}
		if vterm == ' ' {
			break
		}
		// vterm == ',' → another field follows
	}

	// 4. Optional timestamp.
	rest := bytes.TrimSpace(b[pos:])
	if len(rest) == 0 {
		return out, nil
	}
	ts, err := strconv.ParseInt(string(rest), 10, 64)
	if err != nil {
		return out, fmt.Errorf("timestamp %q: %w", rest, err)
	}
	out.Timestamp = ts
	out.HasTimestamp = true
	return out, nil
}

// readUntilUnescaped scans b starting at start until it finds any unescaped
// byte from delims, returning the token, the terminator byte, and the index of
// the terminator. A backslash escapes the next byte (it is part of the token).
// If end of input is reached before a delimiter, it returns the remaining
// token, terminator 0, and the index = len(b).
func readUntilUnescaped(b []byte, start int, delims string) (token string, term byte, end int, err error) {
	var sb strings.Builder
	for i := start; i < len(b); i++ {
		c := b[i]
		if c == '\\' && i+1 < len(b) {
			// Preserve the escape sequence verbatim; unescaping happens in
			// unescapeIdent so callers can do single-pass scanning here.
			sb.WriteByte(c)
			sb.WriteByte(b[i+1])
			i++
			continue
		}
		if strings.IndexByte(delims, c) >= 0 {
			return sb.String(), c, i, nil
		}
		sb.WriteByte(c)
	}
	return sb.String(), 0, len(b), nil
}

// readFieldValue reads a single field value starting at pos. If the value
// begins with '"', it is a quoted string that may contain spaces and commas;
// only `\"` and `\\` are recognized as escape sequences inside quotes.
// Otherwise the value reads until the next unescaped ',' (more fields)
// or ' ' (timestamp boundary) or end of input.
func readFieldValue(b []byte, pos int) (raw string, term byte, end int, err error) {
	if pos >= len(b) {
		return "", 0, pos, fmt.Errorf("empty field value")
	}
	if b[pos] == '"' {
		var sb strings.Builder
		closed := false
		// Mark this as a string field by re-prepending '"' so the classifier
		// can distinguish "1" (string) from 1 (number).
		sb.WriteByte('"')
		i := pos + 1
		for ; i < len(b); i++ {
			c := b[i]
			if c == '\\' && i+1 < len(b) {
				next := b[i+1]
				if next == '"' || next == '\\' {
					sb.WriteByte(next)
					i++
					continue
				}
				// Unrecognized escape: pass through as-is.
				sb.WriteByte(c)
				continue
			}
			if c == '"' {
				sb.WriteByte('"')
				closed = true
				i++
				break
			}
			sb.WriteByte(c)
		}
		// After the closing quote, expect ',', ' ', or EOL.
		if !closed {
			return "", 0, i, fmt.Errorf("unterminated quoted string")
		}
		if i >= len(b) {
			return sb.String(), 0, i, nil
		}
		switch b[i] {
		case ',', ' ':
			return sb.String(), b[i], i, nil
		default:
			return "", 0, i, fmt.Errorf("unexpected byte %q after quoted string", b[i])
		}
	}
	tok, t, e, err := readUntilUnescaped(b, pos, ", ")
	return tok, t, e, err
}

// classifyFieldValue converts a raw field-value token into one of
// float64/int64/uint64/bool/string per the line-protocol spec.
func classifyFieldValue(raw string) (interface{}, error) {
	if raw == "" {
		return nil, fmt.Errorf("empty value")
	}
	// Quoted string: readFieldValue keeps surrounding quotes to mark the type.
	// Guard against a lone `"` (len 1): an unterminated quoted value at
	// end-of-line would otherwise make raw[1:len(raw)-1] slice with low > high
	// and panic.
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return raw[1 : len(raw)-1], nil
	}
	if raw == `"` {
		return nil, fmt.Errorf("unterminated quoted string %q", raw)
	}
	// Integer suffix.
	if last := raw[len(raw)-1]; last == 'i' {
		v, err := strconv.ParseInt(raw[:len(raw)-1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", raw, err)
		}
		return v, nil
	}
	// Unsigned suffix.
	if last := raw[len(raw)-1]; last == 'u' {
		v, err := strconv.ParseUint(raw[:len(raw)-1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid unsigned %q: %w", raw, err)
		}
		return v, nil
	}
	// Booleans.
	switch raw {
	case "t", "T", "true", "True", "TRUE":
		return true, nil
	case "f", "F", "false", "False", "FALSE":
		return false, nil
	}
	// Default: float64. NB: values exceeding float64 precision (≈16
	// significant digits) lose precision silently — this is spec-compliant.
	// A future opt-in mode can preserve such values as strings.
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number %q: %w", raw, err)
	}
	return v, nil
}

// unescapeIdent processes backslash escapes for measurement / tag / field-key
// contexts: `\ `, `\,`, `\=` collapse to their unescaped form; other backslash
// sequences are preserved verbatim.
func unescapeIdent(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			next := s[i+1]
			if next == ' ' || next == ',' || next == '=' {
				sb.WriteByte(next)
				i++
				continue
			}
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}
