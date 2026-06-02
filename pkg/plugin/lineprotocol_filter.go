package plugin

import (
	"regexp"
	"strings"
)

type tagPattern struct {
	key   string
	value *regexp.Regexp
}

// lineProtocolFilter is the compiled, per-query line-protocol filter. All three
// axes are ANDed; an empty axis matches everything. Each entry is a compiled
// regex, so plain strings like "Breaker Data" match exactly, and patterns like
// "Breaker.*" or "DCM10[12]" work as expected.
type lineProtocolFilter struct {
	measurements []*regexp.Regexp
	fields       []*regexp.Regexp
	tags         []tagPattern
}

func (f *lineProtocolFilter) hasAnyConstraint() bool {
	return len(f.measurements) > 0 || len(f.fields) > 0 || len(f.tags) > 0
}

func buildLineProtocolFilter(config *StreamConfig) *lineProtocolFilter {
	if config == nil {
		return nil
	}
	f := &lineProtocolFilter{
		measurements: parseRegexpSet(config.LineProtocolMeasurements),
		fields:       parseRegexpSet(config.LineProtocolFields),
		tags:         parseTagPatterns(config.LineProtocolTags),
	}
	if !f.hasAnyConstraint() {
		return nil
	}
	return f
}

func applyLineProtocolFilter(lines []ParsedLine, f *lineProtocolFilter) []ParsedLine {
	if f == nil || !f.hasAnyConstraint() {
		return lines
	}
	out := make([]ParsedLine, 0, len(lines))
	for _, line := range lines {
		if len(f.measurements) > 0 && !matchesAnyRegexp(f.measurements, line.Measurement) {
			continue
		}
		if !lineMatchesTagFilter(line, f.tags) {
			continue
		}
		if len(f.fields) == 0 {
			out = append(out, line)
			continue
		}
		kept := make([]FieldKV, 0, len(line.Fields))
		for _, fv := range line.Fields {
			if matchesAnyRegexp(f.fields, fv.Key) {
				kept = append(kept, fv)
			}
		}
		if len(kept) == 0 {
			continue
		}
		line.Fields = kept
		out = append(out, line)
	}
	return out
}

func lineMatchesTagFilter(line ParsedLine, required []tagPattern) bool {
	if len(required) == 0 {
		return true
	}
	lineTags := make(map[string]string, len(line.Tags))
	for _, t := range line.Tags {
		lineTags[t.Key] = t.Value
	}
	// Group patterns by key. Within a key, patterns are ORed (any match passes).
	// Across different keys, groups are ANDed (all keys must pass).
	byKey := make(map[string][]*regexp.Regexp, len(required))
	keyOrder := make([]string, 0, len(required))
	for _, tp := range required {
		if _, seen := byKey[tp.key]; !seen {
			keyOrder = append(keyOrder, tp.key)
		}
		byKey[tp.key] = append(byKey[tp.key], tp.value)
	}
	for _, k := range keyOrder {
		got, ok := lineTags[k]
		if !ok {
			return false
		}
		if !matchesAnyRegexp(byKey[k], got) {
			return false
		}
	}
	return true
}

func matchesAnyRegexp(patterns []*regexp.Regexp, s string) bool {
	for _, p := range patterns {
		if p.MatchString(s) {
			return true
		}
	}
	return false
}

// parseRegexpSet splits a comma-separated string into compiled regex patterns.
// Invalid patterns are silently skipped. Returns nil when nothing useful remains.
func parseRegexpSet(s string) []*regexp.Regexp {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []*regexp.Regexp
	for _, part := range strings.Split(s, ",") {
		p := strings.TrimSpace(part)
		if p == "" {
			continue
		}
		re, err := regexp.Compile(p)
		if err != nil {
			continue
		}
		out = append(out, re)
	}
	return out
}

// parseTagPatterns splits a comma-separated string of `key=value` pairs where
// the value is treated as a regex pattern. Invalid patterns are silently skipped.
func parseTagPatterns(s string) []tagPattern {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []tagPattern
	for _, part := range strings.Split(s, ",") {
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(part[:eq])
		v := strings.TrimSpace(part[eq+1:])
		if k == "" {
			continue
		}
		re, err := regexp.Compile(v)
		if err != nil {
			continue
		}
		out = append(out, tagPattern{key: k, value: re})
	}
	return out
}
