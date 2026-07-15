package plugin

import (
	"regexp"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
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

// compileAnchored compiles pat as a fully-anchored pattern so plain strings
// match exactly (e.g. "Breaker" no longer matches "Breaker Data") while regex
// metacharacters still work within the pattern.
func compileAnchored(pat string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + pat + ")$")
}

// compileAnchoredOrLiteral compiles pat as an anchored regex. If pat isn't a
// valid regex (e.g. "+N01", a literal tag value that happens to be an invalid
// regex fragment), it falls back to an anchored literal exact-match via
// regexp.QuoteMeta instead of silently dropping the entry — otherwise an axis
// with zero surviving patterns matches everything, defeating the filter. A
// warning is logged once per invalid pattern so operators can tell a raw
// string was matched literally rather than as a regex.
func compileAnchoredOrLiteral(pat string) *regexp.Regexp {
	re, err := compileAnchored(pat)
	if err == nil {
		return re
	}
	log.DefaultLogger.Warn("lineprotocol filter pattern is not a valid regex; matching literally",
		"pattern", pat,
		"error", err)
	// QuoteMeta escapes all regex metacharacters, so this compile cannot fail.
	literal, _ := compileAnchored(regexp.QuoteMeta(pat))
	return literal
}

// parseRegexpSet splits a comma-separated string into compiled regex patterns.
// Each pattern is anchored, so plain entries become exact matches. Patterns
// that aren't valid regexes fall back to an anchored literal match (see
// compileAnchoredOrLiteral). Returns nil when nothing useful remains.
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
		out = append(out, compileAnchoredOrLiteral(p))
	}
	return out
}

// parseTagPatterns splits a comma-separated string of `key=value` pairs where
// the value is treated as a regex pattern (falling back to a literal exact
// match when it isn't a valid regex; see compileAnchoredOrLiteral).
// Entries with a blank value (e.g. an interpolated-away `Building=`) are ignored
// rather than compiled into `^(?:)$`, which would otherwise require the tag to be
// empty and filter out every row.
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
		if k == "" || v == "" {
			continue
		}
		out = append(out, tagPattern{key: k, value: compileAnchoredOrLiteral(v)})
	}
	return out
}
