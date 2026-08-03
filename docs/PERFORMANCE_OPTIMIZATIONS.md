# Finding and Fixing Performance Bottlenecks in the Grafana Kafka Datasource Plugin

_How profiling, benchmarking, and explicit benchmark variants turned a per-message decode bottleneck into a 12x throughput win — and how you can reproduce every number in this post yourself._

---

## Why this investigation

The [Grafana Kafka Datasource plugin](https://github.com/hoptical/grafana-kafka-datasource) streams Kafka messages into Grafana in real time, decoding JSON, Avro, Protobuf, InfluxDB Line Protocol, and plaintext payloads on the fly. Before making any performance claims or changes, I wanted answers to two questions:

1. How much load can the plugin's message-processing pipeline actually handle?
2. Where exactly is time and memory being spent, and can it be reduced without changing behavior?

Rather than guessing, this was done the only way that produces trustworthy answers: **write Go benchmarks, profile them with `pprof`, fix what the data says is worth fixing, and prove the improvement with `benchstat`.** Every fix below also has an explicit benchmark path that reproduces the pre-fix behavior without shipping runtime toggles in the plugin.

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

**The fix.** Cache the compiled codec per schema string. A `*goavro.Codec` is immutable once built and safe for concurrent reuse, so the hot-path helper now uses a bounded LRU cache keyed by raw schema string (default 256 entries):

```go
type MessageDecoder struct {
    disableAvroCodecCache bool
    avroCodecCache        *lruCache[*goavro.Codec]
}

func (d *MessageDecoder) getAvroCodec(schema string) (*goavro.Codec, error) {
    if d.disableAvroCodecCache {
        return goavro.NewCodec(schema)
    }
    if cached, ok := d.avroCodecCache.Get(schema); ok {
        return cached, nil
    }
    codec, err := goavro.NewCodec(schema)
    if err != nil {
        return nil, err
    }
    d.avroCodecCache.Add(schema, codec)
    return codec, nil
}
```

**Results:**

| Benchmark                             | Before   | After   | Change     |
| ------------------------------------- | -------- | ------- | ---------- |
| `DecodeAvroMessage` (ns/op)           | 5,166 ns | 341 ns  | **-93.4%** |
| `DecodeAvroMessage` (B/op)            | 8,260 B  | 760 B   | **-90.8%** |
| `DecodeAvroMessage` (allocs/op)       | 185      | 16      | **-91.4%** |
| Full pipeline `ProcessMessage` (Avro) | 6.60 µs  | 1.53 µs | **-76.8%** |

Translated to raw single-threaded, full-pipeline throughput (no network I/O): **~152k msg/s -> ~654k msg/s (~4.3x)**.

---

## Fix #2: The Protobuf schema was recompiled on every message too

**The problem.** Same shape of bug, different format: `ParseProtobufSchema` ran `protocompile.Compiler.Compile(...)` — a full `.proto` schema compile — on every call.

**The fix.** A bounded LRU cache identical in spirit to the Avro one, with compile logic in a small `compileProtobufSchema` helper so both cached and no-cache benchmark paths share one implementation:

```go
func (d *MessageDecoder) ParseProtobufSchema(schema string) (*ParsedProtobufSchema, error) {
    if d.disableProtobufSchemaCache {
        return compileProtobufSchema(schema)
    }
    if cached, ok := d.protobufSchemaCache.Get(schema); ok {
        return cached, nil
    }
    parsed, err := compileProtobufSchema(schema)
    if err != nil {
        return nil, err
    }
    d.protobufSchemaCache.Add(schema, parsed)
    return parsed, nil
}
```

**Results:**

| Benchmark                                 | Before    | After   | Change     |
| ----------------------------------------- | --------- | ------- | ---------- |
| `DecodeProtobufMessage_Plain` (ns/op)     | 15,653 ns | 574 ns  | **-96.3%** |
| `DecodeProtobufMessage_Plain` (B/op)      | 35,862 B  | 997 B   | **-97.2%** |
| `DecodeProtobufMessage_Plain` (allocs/op) | 250       | 14      | **-94.4%** |
| Full pipeline `ProcessMessage` (Protobuf) | 19.07 µs  | 1.53 µs | **-92.0%** |

Translated to single-threaded, full-pipeline throughput: **~52k msg/s -> ~656k msg/s (~12.5x)**. This was the single biggest win in the whole investigation - schema recompilation, not byte decoding itself, was almost the entire cost.

As a sanity check, JSON and plaintext benchmarks (which share no code with the Avro/Protobuf decode path) were unaffected by these two fixes — a useful control group confirming the fixes are properly isolated.

---

## Fix #3: Re-sorting field names on every message (a smaller, honest win)

**The problem.** For JSON/Avro/Protobuf messages, `ProcessMessage` collects the flattened field keys into a slice and calls `sort.Strings(keys)` on _every_ message, to keep Grafana's frame columns in a stable order. If a topic's schema doesn't change from message to message (the common case), that sort is redundant work.

**The fix.** Cache the sorted key order on the `StreamManager`, and reuse it whenever the current message's key _set_ is identical to the previous message's:

```go
func (sm *StreamManager) sortedFlatKeys(flat map[string]interface{}) []string {
    if sm.fieldOrderCacheEnabled {
        sm.mu.RLock()
        hit := sm.flatKeySetMatchesLocked(flat)
        cached := sm.flatKeyOrder
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

| Field count               | Before   | After    | Change               |
| ------------------------- | -------- | -------- | -------------------- |
| 20 fields (typical topic) | 3.855 µs | 3.543 µs | **-8.1% (p=0.002)**  |
| 100 fields (wide topic)   | 19.15 µs | 16.12 µs | **-15.8% (p=0.002)** |

At typical field counts the win is now measurable, but still smaller than wide-schema cases. As schema width grows, sorting cost grows faster than cache-check overhead, so the relative gain increases.

---

## Reproducing every fix's "before" behavior: benchmark variants

These optimizations are now controlled in code, not by shipped environment variables. Production always uses the optimized path. Benchmarks reproduce the pre-fix behavior by constructing explicit no-cache or no-optimization variants.

Useful benchmark pairs:

| Optimized benchmark                                | Pre-fix benchmark                                            | Reverts                          |
| -------------------------------------------------- | ------------------------------------------------------------ | -------------------------------- |
| `BenchmarkDecodeAvroMessage`                       | `BenchmarkDecodeAvroMessage_NoCache`                         | Avro codec caching              |
| `BenchmarkDecodeProtobufMessage_Plain`             | `BenchmarkDecodeProtobufMessage_Plain_NoCache`               | Protobuf schema caching         |
| `BenchmarkProcessMessage_JSON_Wide100`             | `BenchmarkProcessMessage_JSON_Wide100_FieldOrderCacheDisabled` | Field-order caching             |
| `BenchmarkWorkflow`                                | `BenchmarkWorkflow_NoOptimizations`                          | Combined old path, including micro-batching |

The schema caches remain bounded with hardcoded defaults:

| Constant                                       | Default | Meaning                                                 |
| ---------------------------------------------- | ------- | ------------------------------------------------------- |
| `defaultAvroCodecCacheMaxEntries`              | `256`   | Max compiled Avro codecs retained in LRU cache          |
| `defaultProtobufSchemaCacheMaxEntries`         | `256`   | Max compiled Protobuf descriptors retained in LRU cache |

Example: reproduce the pre-fix Avro decode behavior yourself:

```bash
# Optimized path
go test -run '^$' -bench '^BenchmarkDecodeAvroMessage$' -benchmem ./pkg/kafka_client/...
# BenchmarkDecodeAvroMessage-10   707856   326.2 ns/op   760 B/op   16 allocs/op

# Explicit pre-fix path
go test -run '^$' -bench '^BenchmarkDecodeAvroMessage_NoCache$' -benchmem ./pkg/kafka_client/...
# BenchmarkDecodeAvroMessage_NoCache-10   48212   4967 ns/op   8260 B/op   185 allocs/op
```

When using `benchstat`, normalize the explicit pre-fix benchmark name back to the optimized name so both files contain matching benchmark identifiers:

```bash
go test -run '^$' -bench '^BenchmarkDecodeAvroMessage$' -benchmem -count=6 ./pkg/kafka_client/... > after.txt
go test -run '^$' -bench '^BenchmarkDecodeAvroMessage_NoCache$' -benchmem -count=6 ./pkg/kafka_client/... > before.txt
sed 's/BenchmarkDecodeAvroMessage_NoCache/BenchmarkDecodeAvroMessage/' before.txt > before.norm.txt
benchstat before.norm.txt after.txt
```

---

## What didn't get "fixed" (and why)

Profiling `ProcessMessage`'s JSON decode path showed that `FieldBuilder.AddValueToFrame` — which builds one Grafana `data.Field` per message field — is the single largest remaining cost (~40% of the function's own time, larger than JSON flattening or key sorting). Allocation profiling traced this to three allocations per field, all structurally required by the current design:

1. A `make([]*float64, 1)` pointer slice built for each field.
2. The Grafana SDK's own internal nullable-vector wrapper (inside `data.NewField`).
3. Boxing the value into a pointer (`SetConcrete`), needed for nullable-field semantics.

The reason this wasn't touched: the plugin deliberately always builds _nullable_ field types, even for non-null values, so that a field's Go type stays consistent across the one-frame-per-message live stream — Grafana Live requires stable schema across appended frames. Switching to non-nullable slices would save an allocation per field, but risks breaking that schema stability if a later message needs to send a null value for the same field. That's a real risk I can't verify without an end-to-end Grafana Live test harness, so it wasn't shipped speculatively.

The higher-leverage architectural lever here is to batch compatible Kafka message frames into small multi-row frames before send, amortizing `FrameToJSON` and packet overhead. That optimization is enabled in production and is reproduced in benchmarks with `BenchmarkWorkflow`; `BenchmarkWorkflow_NoOptimizations` exercises the old one-frame-per-message behavior.

### Not shipped in this stage (next candidates)

These are promising but were intentionally left for a later stage:

1. **Optimization #3: remove per-field lock overhead in `FieldBuilder` on single-consumer stream path.**
   Today each field goes through `AddValueToFrame` with a mutex lock/unlock. In `RunStream`, only one goroutine builds frames, so this is mostly uncontended overhead. A safe follow-up is a lock-free fast path for single-stream ownership, while preserving the current locked path for shared/concurrent call sites.
2. **Optimization #4: parallel decode/transform workers with ordering guarantees.**
   Current drain loop is single-threaded by design. A worker pool can raise throughput further, but must preserve partition ordering and controlled backpressure before `SendFrame`.
3. **Schema-registry request de-duplication (`singleflight`) for cold-cache bursts.**
   First hits of same schema/subject from concurrent readers can still duplicate outbound HTTP calls. Collapsing in-flight lookups would reduce startup spikes.
4. **Allocation trimming in flatten/build path (`sync.Pool` for temporary maps/slices).**
   Useful for very wide JSON messages, but needs careful profiling and contention checks to avoid pool churn regressions.

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

By default this benchmark also uses the same micro-batching combiner as `RunStream`. To reproduce the combined pre-fix behavior, run `BenchmarkWorkflow_NoOptimizations`.

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

- **Before**: `BenchmarkWorkflow_NoOptimizations`
- **After**: `BenchmarkWorkflow`

Command pattern used:

```bash
# After (all fixes enabled)
go test -run '^$' -bench '^BenchmarkWorkflow/sendframe_json/' -benchmem -count=6 ./pkg/plugin/... > /tmp/workflow-after.txt

# Before (combined explicit old path)
go test -run '^$' -bench '^BenchmarkWorkflow_NoOptimizations/sendframe_json/' -benchmem -count=6 ./pkg/plugin/... > /tmp/workflow-before.txt

# Keep only benchmark result lines for benchstat parsing
rg '^Benchmark' /tmp/workflow-before.txt > /tmp/workflow-before-clean.txt
rg '^Benchmark' /tmp/workflow-after.txt > /tmp/workflow-after-clean.txt

# Normalize the explicit old-path benchmark name so benchstat can pair rows
sed 's/BenchmarkWorkflow_NoOptimizations/BenchmarkWorkflow/' /tmp/workflow-before-clean.txt > /tmp/workflow-before-norm.txt

benchstat /tmp/workflow-before-norm.txt /tmp/workflow-after-clean.txt
```

`msg/s` results (`sendframe_json` mode, n=6):

| Case             |     Before |      After |       Change |
| ---------------- | ---------: | ---------: | -----------: |
| `plaintext`      |     662.1k |     886.0k |  **+33.81%** |
| `lineprotocol`   |     29.37k |     29.66k |      **~0%** |
| `avro`           |     122.4k |     457.0k | **+273.25%** |
| `protobuf`       |     47.39k |    451.85k | **+853.53%** |
| `json_20fields`  |     115.8k |     172.5k |  **+48.97%** |
| `json_100fields` |     24.21k |     37.62k |  **+55.35%** |
| **geomean**      | **82.55k** | **181.0k** | **+119.32%** |

Interpretation:

- The combined fixes more than **double geomean throughput** for the measured whole-workflow matrix.
- Biggest relative wins are Avro/Protobuf, where schema/cache fixes remove heavy per-message compile overhead.
- JSON gains come from the field-order and micro-batch optimizations; `json_100fields` benefits strongly from packet/serialization amortization.

### Profile delta (before vs after)

Using paired profiles on `BenchmarkWorkflow/sendframe_json/json_100fields`:

- CPU cumulative in `data.FrameToJSON`: **17.19% -> 5.53%**.
- CPU cost in `json-iterator` string/object writing (`WriteString`/`WriteObjectField`) drops noticeably after batching.
- Allocation cumulative in `data.FrameToJSON` (`alloc_objects`): **15.50% -> 5.84%**.
- Dominant allocator remains `FieldBuilder.AddValueToFrame`, but per-message serialization overhead is substantially reduced.
- Expected new overhead appears in batching helpers (`frameMicroBatcher.AddFrames`, `appendFrameRows`, `Frame.RowCopy`) — this is the trade-off that buys the large packet-rate reduction and throughput gain.

### Memory profile result (explicit)

To make memory effects as explicit as the CPU profile, here is one concrete paired run of the same case (`BenchmarkWorkflow/sendframe_json/json_100fields`, n=1):

- `alloc_objects`: `data.FrameToJSON` cumulative **15.10% -> 6.90%**.
- `alloc_space`: `backend.(*StreamSender).SendFrame`/`data.FrameToJSON` path cumulative **40.06% -> 12.48%**.
- Benchmark memory metrics: **40,598 B/op -> 35,839 B/op** (~11.7% less allocated bytes/op), with **834 allocs/op -> 869 allocs/op** (~4.2% more alloc calls/op) due to expected row-copy batching overhead.

Reproduce those memory deltas directly:

```bash
# After (all fixes enabled)
go test -run '^$' -bench '^BenchmarkWorkflow/sendframe_json/json_100fields$' -benchmem \
    -count=1 -memprofile /tmp/workflow-after.mem.prof ./pkg/plugin/...

# Before (combined explicit old path)
go test -run '^$' -bench '^BenchmarkWorkflow_NoOptimizations/sendframe_json/json_100fields$' -benchmem \
    -count=1 -memprofile /tmp/workflow-before.mem.prof ./pkg/plugin/...

go tool pprof -top -alloc_objects /tmp/workflow-before.mem.prof
go tool pprof -top -alloc_objects /tmp/workflow-after.mem.prof
go tool pprof -top -alloc_space /tmp/workflow-before.mem.prof
go tool pprof -top -alloc_space /tmp/workflow-after.mem.prof
```

### On-paper catch-up estimate for loadgen rates

From the same `sendframe_json` benchmark medians above, the plugin's backend processing+serialization capacity (single benchmark worker, no external network/UI rendering overhead) is approximately:

- `plaintext`: ~886k msg/s
- `avro`: ~457k msg/s
- `protobuf`: ~452k msg/s
- `json` (default loadgen shape, `-json-fields=20`): ~173k msg/s
- `json` wide (`-json-fields=100`): ~38k msg/s
- `lineprotocol`: ~30k msg/s

So, **on paper**, for the loadgen default (`-format json -json-fields=20`), the plugin can catch up at about **173k msg/s** in this Layer A setup after fixes.

For practical sustained budgeting with headroom (GC jitter, Kafka/network variance), a conservative target is ~70% of those values:

- default JSON20: **~121k msg/s**
- JSON100: **~26k msg/s**
- line protocol: **~21k msg/s**

These are backend-only capacity estimates; end-to-end Grafana Live behavior (network, UI subscribers, dashboard query load) can lower sustained catch-up in real deployments.

---

## Reproduce everything

```bash
# Clone and set up
git clone https://github.com/hoptical/grafana-kafka-datasource.git
cd grafana-kafka-datasource

# Run the micro-benchmarks (fixed/default behavior)
go test -bench=. -benchmem -run='^$' ./pkg/plugin/... ./pkg/kafka_client/...

# Reproduce the explicit pre-fix benchmark paths
go test -run '^$' -bench='^BenchmarkDecodeAvroMessage_NoCache$' -benchmem ./pkg/kafka_client/...
go test -run '^$' -bench='^BenchmarkDecodeProtobufMessage_Plain_NoCache$' -benchmem ./pkg/kafka_client/...
go test -run '^$' -bench='^BenchmarkProcessMessage_JSON_Wide100_FieldOrderCacheDisabled$' -benchmem ./pkg/plugin/...
go test -run '^$' -bench='^BenchmarkWorkflow_NoOptimizations$' -benchmem ./pkg/plugin/...

# Compare statistically (install benchstat once: go install golang.org/x/perf/cmd/benchstat@latest)
# Normalize the explicit pre-fix benchmark name so benchstat can pair it with
# the optimized benchmark.
go test -run '^$' -bench='^BenchmarkDecodeAvroMessage$' -benchmem -count=6 ./pkg/kafka_client/... > after.txt
go test -run '^$' -bench='^BenchmarkDecodeAvroMessage_NoCache$' -benchmem -count=6 ./pkg/kafka_client/... > before.txt
sed 's/BenchmarkDecodeAvroMessage_NoCache/BenchmarkDecodeAvroMessage/' before.txt > before.norm.txt
benchstat before.norm.txt after.txt

# Generate load and inspect it in Grafana
cd example/go/loadgen
go run . -broker localhost:9094 -topic loadgen -format protobuf -duration 30s -workers 4
```

---

## Takeaways

- Two schema-recompilation bugs (Avro, Protobuf) accounted for the overwhelming majority of decode cost - cheap to fix, huge payoff (4.3x-12.5x on affected paths), and only obvious once profiled.
- Field-order caching helps both typical and wide schemas, but wide schemas still benefit more.
- Bounded caches and explicit benchmark variants make optimization safer operationally: fast by default in production, reproducible pre-fix behavior in tests, and controlled memory growth under schema churn.

If you maintain a Grafana datasource plugin (or any hot-path message-processing service), the same recipe applies: benchmark before touching anything, profile to find the _actual_ line costing time, fix only what the data justifies, and keep a way to switch back.
