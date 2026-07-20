// Package perfflags provides opt-out "feature flags" for performance
// optimizations added to this plugin's Kafka/decoding hot paths.
//
// Each flag defaults to false, meaning the optimized ("after") behavior is
// used - this is the recommended setting for production. Setting the
// corresponding environment variable to a truthy value (1, t, true, ...)
// reverts that specific code path to its pre-optimization ("before")
// behavior. This exists purely so the exact behavior/performance of the
// plugin prior to each fix can be reproduced on demand - for A/B load
// testing, regression comparisons, or troubleshooting a fix that's
// suspected to have side effects - without checking out an older commit.
//
// Example: to reproduce the plugin's behavior from before the Avro codec
// caching fix, start Grafana with:
//
//	KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE=true
package perfflags

import (
	"os"
	"strconv"
	"sync/atomic"
)

// Flag is a boolean performance feature flag. Its value is seeded once from
// an environment variable at process start, and can additionally be
// overridden in-process (e.g. from a benchmark or test) via SetDisabledForTest,
// without needing to spawn a separate process.
type Flag struct {
	envVar string
	value  atomic.Bool
}

func newFlag(envVar string) *Flag {
	f := &Flag{envVar: envVar}
	f.value.Store(boolEnv(envVar))
	return f
}

// Disabled reports whether this performance optimization has been disabled,
// i.e. whether the plugin should behave as it did before the corresponding
// fix.
func (f *Flag) Disabled() bool { return f.value.Load() }

// EnvVar returns the environment variable name backing this flag.
func (f *Flag) EnvVar() string { return f.envVar }

// SetDisabledForTest overrides the flag's in-process value. Intended for
// benchmarks and tests that want to compare fixed vs. pre-fix behavior
// within a single test binary; production code should rely solely on the
// environment variable.
func (f *Flag) SetDisabledForTest(disabled bool) { f.value.Store(disabled) }

func boolEnv(name string) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

var (
	// AvroCodecCache: when disabled, kafka_client.DecodeAvroMessage
	// recompiles the Avro codec (goavro.NewCodec) on every call instead of
	// reusing a cached compiled codec per schema string. Benchmarks showed
	// recompilation accounted for ~93% of DecodeAvroMessage's cost.
	AvroCodecCache = newFlag("KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE")

	// ProtobufSchemaCache: when disabled, kafka_client.ParseProtobufSchema
	// recompiles the .proto schema (protocompile.Compiler.Compile) on every
	// call instead of reusing a cached compiled descriptor per schema
	// string. Benchmarks showed recompilation accounted for ~95-97% of
	// DecodeProtobufMessage's cost.
	ProtobufSchemaCache = newFlag("KAFKA_DS_PERF_DISABLE_PROTOBUF_SCHEMA_CACHE")

	// FieldOrderCache: when disabled, StreamManager.ProcessMessage collects
	// and sorts the flattened value-field keys from scratch on every
	// message. When enabled (default), the sorted key order is reused as-is
	// whenever a message's key set is identical to the previous message's
	// (the common case for topics with a stable schema), avoiding a
	// sort.Strings call and a fresh key slice per message.
	FieldOrderCache = newFlag("KAFKA_DS_PERF_DISABLE_FIELD_ORDER_CACHE")

	// StreamMicroBatch: when disabled, RunStream sends every produced frame
	// immediately (pre-fix behavior). When enabled (default), compatible
	// frames are micro-batched into small multi-row frames before sending,
	// reducing per-message FrameToJSON and packet overhead.
	StreamMicroBatch = newFlag("KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH")
)
