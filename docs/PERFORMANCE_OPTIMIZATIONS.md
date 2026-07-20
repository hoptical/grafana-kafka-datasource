# Finding and Fixing Performance Bottlenecks in the Grafana Kafka Datasource Plugin

_How profiling, benchmarking, and feature flags turned a per-message decode bottleneck into a 12x throughput win — and how you can reproduce every number in this post yourself._

---

## Why this investigation

The [Grafana Kafka Datasource plugin](https://github.com/hoptical/grafana-kafka-datasource) streams Kafka messages into Grafana in real time, decoding JSON, Avro, Protobuf, InfluxDB Line Protocol, and plaintext payloads on the fly. Before making any performance claims or changes, I wanted answers to two questions:

1. How much load can the plugin's message-processing pipeline actually handle?
2. Where exactly is time and memory being spent, and can it be reduced without changing behavior?

Rather than guessing, this was done the only way that produces trustworthy answers: **write Go benchmarks, profile them with `pprof`, fix what the data says is worth fixing, and prove the improvement with `benchstat`.** Every fix below is also gated behind a runtime feature flag, so anyone can flip it off and reproduce the exact pre-fix behavior — no need to check out an old commit.

All numbers in this post come from `go test -bench` on an Apple M5. Your numbers will differ, but the _relative_ improvements should hold.

---

## Methodology

- **Micro-benchmarks** (`go test -bench=. -benchmem`) isolate individual functions: `DecodeAvroMessage`, `DecodeProtobufMessage`, `ParseProtobufSchema`, and the full per-message pipeline `StreamManager.ProcessMessage` (decode → flatten → build Grafana data frame).
- **CPU/memory profiles** (`-cpuprofile`, `-memprofile`, then `go tool pprof -list`/`-top -alloc_objects`) pinpoint exactly which line of code and which allocation is responsible for the cost — not just which function.
- **`benchstat`** compares before/after benchmark runs statistically (with a p-value), so "faster" means "faster beyond measurement noise," not "faster in one lucky run."
- One infrastructure fix had to come first: production code calls `log.DefaultLogger.Debug(...)` on every message. Left enabled, this alone floods benchmark output and completely distorts timing at high iteration counts. Both benchmark packages now set `log.DefaultLogger = log.NewNullLogger()` in a `TestMain`, purely for benchmark hygiene — but it's worth calling out as a real overhead source in production log volume, independent of everything else below.

---

## Fix #1: The Avro codec was recompiled on every message

**The problem.** `DecodeAvroMessage` called `goavro.NewCodec(schema)` — which parses the Avro schema JSON and builds an internal codec — on _every single message_, even though the schema string never changes between messages on the same topic.

```go
// before
func DecodeAvroMessage(data []byte, schema string) (interface{}, error) {
    codec, err := goavro.NewCodec(schema) // recompiled every call
    ...
}
```

**The fix.** Cache the compiled codec per schema string. A `*goavro.Codec` is immutable once built and safe for concurrent reuse, so a `sync.Map` keyed by the raw schema string is enough:

```go
var avroCodecCache sync.Map // map[string]*goavro.Codec

func getAvroCodec(schema string) (*goavro.Codec, error) {
    if perfflags.AvroCodecCache.Disabled() {
        return goavro.NewCodec(schema) // pre-fix behavior, on demand
    }
    if cached, ok := avroCodecCache.Load(schema); ok {
        return cached.(*goavro.Codec), nil
    }
    codec, err := goavro.NewCodec(schema)
    if err != nil {
        return nil, err
    }
    actual, _ := avroCodecCache.LoadOrStore(schema, codec)
    return actual.(*goavro.Codec), nil
}
```

**Results:**

| Benchmark                             | Before   | After   | Change     |
| ------------------------------------- | -------- | ------- | ---------- |
| `DecodeAvroMessage` (ns/op)           | 4,967 ns | 326 ns  | **-93.5%** |
| `DecodeAvroMessage` (B/op)            | —        | —       | N/A        |
| `DecodeAvroMessage` (allocs/op)       | 185      | 16      | **-91.4%** |
| Full pipeline `ProcessMessage` (Avro) | 6.45 µs  | 1.49 µs | **-77.0%** |

Translated to raw single-threaded, full-pipeline throughput (no network I/O): **~155k msg/s → ~673k msg/s (~4.3x)**.

---

## Fix #2: The Protobuf schema was recompiled on every message too

**The problem.** Same shape of bug, different format: `ParseProtobufSchema` ran `protocompile.Compiler.Compile(...)` — a full `.proto` schema compile — on every call.

**The fix.** A `sync.Map` cache identical in spirit to the Avro one, with the compile logic pulled into a small `compileProtobufSchema` helper so both the cached and the flag-disabled path share one implementation (no duplicated logic to drift out of sync):

```go
func ParseProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
    if perfflags.ProtobufSchemaCache.Disabled() {
        return compileProtobufSchema(schema)
    }
    if cached, ok := protobufSchemaCache.Load(schema); ok {
        return cached.(*ParsedProtobufSchema), nil
    }
    parsed, err := compileProtobufSchema(schema)
    if err != nil {
        return nil, err
    }
    actual, _ := protobufSchemaCache.LoadOrStore(schema, parsed)
    return actual.(*ParsedProtobufSchema), nil
}
```

**Results:**

| Benchmark                                 | Before    | After   | Change                     |
| ----------------------------------------- | --------- | ------- | -------------------------- |
| `ParseProtobufSchema` alone (ns/op)       | 14,514 ns | 8.5 ns  | **-99.94%**                |
| `ParseProtobufSchema` (B/op)              | 34,865 B  | 0 B     | **-100%** (pure cache hit) |
| `DecodeProtobufMessage` (ns/op)           | —         | —       | N/A                        |
| `DecodeProtobufMessage` (allocs/op)       | —         | —       | N/A                        |
| Full pipeline `ProcessMessage` (Protobuf) | 18.6 µs   | 1.49 µs | **-92.0%**                 |

Translated to single-threaded, full-pipeline throughput: **~53.7k msg/s → ~672k msg/s (~12.5x)**. This was the single biggest win in the whole investigation — schema recompilation, not the actual byte decoding, was almost the entire cost.

As a sanity check, JSON and plaintext benchmarks (which share no code with the Avro/Protobuf decode path) were unaffected by these two fixes — a useful control group confirming the fixes are properly isolated.

---

## Fix #3: Re-sorting field names on every message (a smaller, honest win)

**The problem.** For JSON/Avro/Protobuf messages, `ProcessMessage` collects the flattened field keys into a slice and calls `sort.Strings(keys)` on _every_ message, to keep Grafana's frame columns in a stable order. If a topic's schema doesn't change from message to message (the common case), that sort is redundant work.

**The fix.** Cache the sorted key order on the `StreamManager`, and reuse it whenever the current message's key _set_ is identical to the previous message's:

```go
func (sm *StreamManager) sortedFlatKeys(flat map[string]interface{}) []string {
    if !perfflags.FieldOrderCache.Disabled() {
        sm.mu.RLock()
        hit := sm.flatKeySetMatchesLocked(flat)
        var cached []string
        if hit {
            cached = make([]string, len(sm.flatKeyOrder))
            copy(cached, sm.flatKeyOrder)
        }
        sm.mu.RUnlock()
        if hit {
            return cached
        }
    }
    // cache miss (or flag disabled): collect + sort from scratch, then cache
    ...
}
```

**Results — and this is where I want to be honest rather than sell a bigger win than what's real:**

| Field count               | Before   | After    | Change                |
| ------------------------- | -------- | -------- | --------------------- |
| 20 fields (typical topic) | 3.647 µs | 3.640 µs | **~0%, within noise** |
| 100 fields (wide topic)   | 18.64 µs | 15.75 µs | **-15.5% (p=0.002)**  |

At a typical field count, the locking and defensive-copy overhead on a cache hit roughly cancels out the savings from skipping `sort.Strings`. The fix only pays off once a message has a wide schema (tens to hundreds of fields), where sorting cost grows faster than the fixed overhead of a cache check. If your topics have narrow schemas, this fix won't move the needle for you — and that's a useful thing to know before assuming every optimization is universally worth it.

---

## Reproducing every fix's "before" behavior: feature flags

Every fix above is controlled by an environment-variable-backed flag in a new `pkg/perfflags` package. By default, all flags are **off**, meaning you get the optimized (fixed) behavior. Set the corresponding variable to a truthy value to force the plugin back to its pre-fix behavior — no git checkout needed:

| Flag                            | Environment variable                          | Reverts                           |
| ------------------------------- | --------------------------------------------- | --------------------------------- |
| `perfflags.AvroCodecCache`      | `KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE`      | Avro codec caching (Fix #1)       |
| `perfflags.ProtobufSchemaCache` | `KAFKA_DS_PERF_DISABLE_PROTOBUF_SCHEMA_CACHE` | Protobuf schema caching (Fix #2)  |
| `perfflags.FieldOrderCache`     | `KAFKA_DS_PERF_DISABLE_FIELD_ORDER_CACHE`     | Field-order caching (Fix #3)      |
| `perfflags.StreamMicroBatch`    | `KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH`     | RunStream micro-batching (Fix #4) |

```go
type Flag struct {
    envVar string
    value  atomic.Bool
}

func (f *Flag) Disabled() bool                 { return f.value.Load() }
func (f *Flag) SetDisabledForTest(disabled bool) { f.value.Store(disabled) }
```

`Disabled()` is checked at the start of each optimized code path; `SetDisabledForTest` lets benchmarks toggle behavior in-process (used to produce the before/after numbers above without needing two separate benchmark binaries). In production, only the environment variable matters.

Example: reproduce the pre-fix Avro decode behavior yourself:

```bash
# Fixed (default) behavior
go test -bench='^BenchmarkDecodeAvroMessage$' -benchmem -run='^$' ./pkg/kafka_client/...
# BenchmarkDecodeAvroMessage-10   707856   326.2 ns/op   760 B/op   16 allocs/op

# Pre-fix behavior, reproduced on demand
KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE=true \
  go test -bench='^BenchmarkDecodeAvroMessage$' -benchmem -run='^$' ./pkg/kafka_client/...
# BenchmarkDecodeAvroMessage-10   48212   4967 ns/op   8260 B/op   185 allocs/op
```

The same pattern applies to the datasource plugin binary itself in a real Grafana deployment: set the environment variable before starting Grafana, and the plugin's backend process will behave exactly as it did before the fix.

---

## What didn't get "fixed" (and why)

Profiling `ProcessMessage`'s JSON decode path showed that `FieldBuilder.AddValueToFrame` — which builds one Grafana `data.Field` per message field — is the single largest remaining cost (~40% of the function's own time, larger than JSON flattening or key sorting). Allocation profiling traced this to three allocations per field, all structurally required by the current design:

1. A `make([]*float64, 1)` pointer slice built for each field.
2. The Grafana SDK's own internal nullable-vector wrapper (inside `data.NewField`).
3. Boxing the value into a pointer (`SetConcrete`), needed for nullable-field semantics.

The reason this wasn't touched: the plugin deliberately always builds _nullable_ field types, even for non-null values, so that a field's Go type stays consistent across the one-frame-per-message live stream — Grafana Live requires stable schema across appended frames. Switching to non-nullable slices would save an allocation per field, but risks breaking that schema stability if a later message needs to send a null value for the same field. That's a real risk I can't verify without an end-to-end Grafana Live test harness, so it wasn't shipped speculatively.

The higher-leverage architectural lever here is to batch compatible Kafka message frames into small multi-row frames before send, amortizing `FrameToJSON` and packet overhead. That is now implemented behind `KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH` (enabled by default; disable to reproduce pre-fix one-frame-per-message behavior).

---

## A load generator for testing this yourself

The plugin's existing example producer (`example/go/producer.go`) intentionally runs at ~1-2 messages/sec — fine for demos, useless for load testing. To let anyone push real load through the plugin's Kafka topics, there's now a standalone tool at `example/go/loadgen/`:

```bash
cd example/go/loadgen
go run . -broker localhost:9094 -topic loadgen -format json -duration 30s -workers 4
```

It supports all five message formats, concurrent producer goroutines, a target-rate or max-speed mode, and prints live throughput stats:

```text
Starting load generator: format=json topic=loadgen workers=2 rate=max duration=5s async=true
[   2.0s] confirmed=462600 (231295 msg/s, 146.38 MB/s) errors=0
[   4.0s] confirmed=934800 (236100 msg/s, 149.47 MB/s) errors=0
--- summary ---
elapsed:        5.0s
confirmed sent: 1166973
errors:         0
throughput:     233313 msg/s, 147.72 MB/s
```

Useful flags:

- `-format json|avro|protobuf|lineprotocol|plaintext`
- `-rate <msgs/sec>` (0 = max speed) and `-workers <n>` for concurrency
- `-json-fields <n>` to pad JSON payloads with extra fields — set this to 100+ to specifically exercise the field-order-cache fix (#3) at the schema width where it actually matters
- `-duration <dur>` (0 = run until Ctrl+C)

Its Avro/Protobuf schemas (`LoadGenReading{id, seq, value, sent_at_ns}`) are plain inline schemas (no schema registry dependency) — paste them straight from `example/go/loadgen/payload.go` into the plugin's datasource query editor to decode the traffic it produces.

**Important caveat:** the throughput numbers the tool prints (hundreds of thousands to over a million messages/sec, depending on format) measure how fast it can _publish_ messages to Kafka — not how fast the plugin can _consume and decode_ them end-to-end through Grafana Live.

---

## Deterministic full-workflow benchmark (Layer A)

To benchmark the plugin's full per-message backend workflow deterministically (without Kafka/Grafana network effects), use:

```bash
go test -run '^$' -bench BenchmarkWorkflow -benchmem ./pkg/plugin/...
```

This benchmark executes `ProcessMessageFrames` and then routes each resulting frame through one of two sink modes:

- `noop`: processing-only baseline (decode + flatten + frame building)
- `sendframe_json`: processing + real `backend.StreamSender.SendFrame` JSON serialization cost

By default this benchmark also uses the same micro-batching combiner as `RunStream`. To reproduce pre-fix one-frame-per-message behavior, run with:

```bash
KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH=true \
    go test -run '^$' -bench BenchmarkWorkflow -benchmem ./pkg/plugin/...
```

Current matrix includes `plaintext`, `lineprotocol`, `avro`, `protobuf`, `json_20fields`, and `json_100fields`.

Metrics include:

- `ns/op`, `B/op`, `allocs/op` (standard Go benchmark outputs)
- `msg/s` (reported by the benchmark)
- `packets/s` and `out_B/s` in `sendframe_json` mode

Generate profiles and open flame charts:

```bash
go test -run '^$' -bench BenchmarkWorkflow -benchmem \
    -cpuprofile workflow.cpu.prof \
    -memprofile workflow.mem.prof \
    ./pkg/plugin/...

# Text summaries
go tool pprof -top workflow.cpu.prof
go tool pprof -top -alloc_objects workflow.mem.prof

# Flame graph / interactive call graph UI
go tool pprof -http=:0 workflow.cpu.prof
go tool pprof -http=:0 -alloc_space workflow.mem.prof
```

For stable comparisons across iterations, repeat with `-count=6` and compare with `benchstat`.

### Whole-workflow before vs after (all fixes combined)

To measure the full impact of all implemented fixes together, I compared:

- **Before**: all perf flags disabled (`AVRO_CODEC_CACHE`, `PROTOBUF_SCHEMA_CACHE`, `FIELD_ORDER_CACHE`, `STREAM_MICROBATCH`)
- **After**: default settings (all fixes enabled)

Command pattern used:

```bash
# After (all fixes enabled)
go test -run '^$' -bench '^BenchmarkWorkflow/sendframe_json/' -benchmem -count=6 ./pkg/plugin/... > /tmp/workflow-after.txt

# Before (all fixes disabled)
KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE=true \
KAFKA_DS_PERF_DISABLE_PROTOBUF_SCHEMA_CACHE=true \
KAFKA_DS_PERF_DISABLE_FIELD_ORDER_CACHE=true \
KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH=true \
go test -run '^$' -bench '^BenchmarkWorkflow/sendframe_json/' -benchmem -count=6 ./pkg/plugin/... > /tmp/workflow-before.txt

# Keep only benchmark result lines for benchstat parsing
rg '^Benchmark' /tmp/workflow-before.txt > /tmp/workflow-before-clean.txt
rg '^Benchmark' /tmp/workflow-after.txt > /tmp/workflow-after-clean.txt

benchstat /tmp/workflow-before-clean.txt /tmp/workflow-after-clean.txt
```

`msg/s` results (`sendframe_json` mode, n=6):

| Case             |     Before |      After |       Change |
| ---------------- | ---------: | ---------: | -----------: |
| `plaintext`      |     558.2k |     813.2k |  **+45.69%** |
| `lineprotocol`   |     25.51k |     27.01k |   **+5.87%** |
| `avro`           |     100.9k |     427.8k | **+323.83%** |
| `protobuf`       |     47.33k |    405.07k | **+755.91%** |
| `json_20fields`  |     115.7k |     156.7k |  **+35.41%** |
| `json_100fields` |     24.27k |     34.03k |  **+40.26%** |
| **geomean**      | **75.89k** | **165.2k** | **+117.64%** |

Interpretation:

- The combined fixes more than **double geomean throughput** for the measured whole-workflow matrix.
- Biggest relative wins are Avro/Protobuf, where schema/cache fixes remove heavy per-message compile overhead.
- JSON gains come from the field-order and micro-batch optimizations; `json_100fields` benefits strongly from packet/serialization amortization.

### Profile delta (before vs after)

Using paired profiles on `BenchmarkWorkflow/sendframe_json/json_100fields`:

- CPU cumulative in `data.FrameToJSON`: **11.58% -> 2.06%**.
- CPU cost in `json-iterator` string/object writing dropped out of top hotspots (`WriteString`/`WriteObjectField` were significant before).
- Allocation cumulative in `data.FrameToJSON`: **13.73% -> 6.07%**.
- Dominant allocator remains `FieldBuilder.AddValueToFrame` (still the largest single hotspot), but per-message serialization overhead is substantially reduced.
- New expected overhead appears in batching helpers (`frameMicroBatcher.AddFrames`, `appendFrameRows`, `Frame.RowCopy`) — this is the trade-off that buys the large packet-rate reduction and throughput gain.

### On-paper catch-up estimate for loadgen rates

From the same `sendframe_json` benchmark medians above, the plugin's backend processing+serialization capacity (single benchmark worker, no external network/UI rendering overhead) is approximately:

- `plaintext`: ~813k msg/s
- `avro`: ~428k msg/s
- `protobuf`: ~405k msg/s
- `json` (default loadgen shape, `-json-fields=20`): ~157k msg/s
- `json` wide (`-json-fields=100`): ~34k msg/s
- `lineprotocol`: ~27k msg/s

So, **on paper**, for the loadgen default (`-format json -json-fields=20`), the plugin can catch up at about **157k msg/s** in this Layer A setup after fixes.

For practical sustained budgeting with headroom (GC jitter, Kafka/network variance), a conservative target is ~70% of those values:

- default JSON20: **~110k msg/s**
- JSON100: **~24k msg/s**
- line protocol: **~19k msg/s**

These are backend-only capacity estimates; end-to-end Grafana Live behavior (network, UI subscribers, dashboard query load) can lower sustained catch-up in real deployments.

---

## Reproduce everything

```bash
# Clone and set up
git clone https://github.com/hoptical/grafana-kafka-datasource.git
cd grafana-kafka-datasource

# Run the micro-benchmarks (fixed/default behavior)
go test -bench=. -benchmem -run='^$' ./pkg/plugin/... ./pkg/kafka_client/...

# Reproduce pre-fix behavior for any single fix
KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE=true go test -bench='^BenchmarkDecodeAvroMessage$' -benchmem -run='^$' ./pkg/kafka_client/...
KAFKA_DS_PERF_DISABLE_PROTOBUF_SCHEMA_CACHE=true go test -bench='^BenchmarkParseProtobufSchema$' -benchmem -run='^$' ./pkg/kafka_client/...
KAFKA_DS_PERF_DISABLE_FIELD_ORDER_CACHE=true go test -bench='^BenchmarkProcessMessage_JSON_Wide100$' -benchmem -run='^$' ./pkg/plugin/...
KAFKA_DS_PERF_DISABLE_STREAM_MICROBATCH=true go test -run '^$' -bench BenchmarkWorkflow -benchmem ./pkg/plugin/...

# Compare statistically (install benchstat once: go install golang.org/x/perf/cmd/benchstat@latest)
# Use the same -bench filter for both runs so before/after only differ in the
# one flag under comparison - mixing in other benchmarks (e.g. Protobuf's,
# which stay optimized in both runs) would make the aggregate comparison
# misleading rather than a like-for-like measurement of this one fix.
go test -bench='^BenchmarkDecodeAvroMessage$' -benchmem -run='^$' -count=6 ./pkg/kafka_client/... > after.txt
KAFKA_DS_PERF_DISABLE_AVRO_CODEC_CACHE=true go test -bench='^BenchmarkDecodeAvroMessage$' -benchmem -run='^$' -count=6 ./pkg/kafka_client/... > before.txt
benchstat before.txt after.txt

# Generate load and inspect it in Grafana
cd example/go/loadgen
go run . -broker localhost:9094 -topic loadgen -format protobuf -duration 30s -workers 4
```

---

## Takeaways

- Two schema-recompilation bugs (Avro, Protobuf) accounted for the overwhelming majority of decode cost — cheap to fix, huge payoff (4.3x-12.5x on the affected paths), and they were only findable by profiling, not by reading the code and guessing.
- Not every "obvious" optimization pays off equally: the field-order cache is real but genuinely schema-width-dependent — reporting that honestly is more useful than rounding up.
- Making every fix reversible via a feature flag turned "trust me, it's faster" into "here's the exact command to prove it yourself," which is a much better place to leave both a user's audience and a plugin's contributors.

If you maintain a Grafana datasource plugin (or any hot-path message-processing service), the same recipe applies: benchmark before touching anything, profile to find the _actual_ line costing time, fix only what the data justifies, and keep a way to switch back.
