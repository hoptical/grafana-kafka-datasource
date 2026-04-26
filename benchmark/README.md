# I Built a Kafka Streaming Plugin for Grafana. Here's How Far I Could Push It

A few months after I published [grafana-kafka-datasource](https://github.com/hoptical/grafana-kafka-datasource),
someone opened a GitHub issue with a straightforward question: _"We have a topic producing
5,000 messages per second. Will this work?"_

My honest answer was: I don't know. I built the plugin for real-time monitoring of sensor
and event streams at human-perceivable rates — tens to hundreds of messages per second.
I had never tried to push it to 5,000. So instead of guessing, I measured.

This article documents what I found: where the ceiling is, why it's there, and what I'm
planning to fix.

---

## What the Plugin Does

grafana-kafka-datasource connects Kafka topics directly to live Grafana panels. No
intermediate database, no aggregation layer, no polling. Every Kafka message becomes a data
frame row that appears in your browser as it arrives — typically within 100–300 milliseconds
of being produced.

The design is intentionally simple: one plugin, one datasource query, one live panel. You
point it at a topic, choose a message format (JSON, Avro, Protobuf, or plaintext), and
Grafana renders a real-time time series. Partition assignment, consumer lifecycle, and
Grafana Live WebSocket management all happen transparently.

---

## How a Message Travels Through the Plugin

Before measuring, I needed to know exactly what path a Kafka message takes from the broker
to the browser. Here's the complete data path:

```
Kafka topic
    │
    │  one kafka-go Reader goroutine per partition
    ▼
readFromPartition() goroutines ─────────────────────────────┐
                                                            │
                                         buffered channel (capacity = 100 messages)
                                                            │
                                         single drain goroutine
                                                            │
                                                            ▼
                                      ProcessMessageToFrame()
                                        ├─ json.Unmarshal()
                                        ├─ FlattenJSON()       (recursive tree walk)
                                        └─ FieldBuilder.AddValueToFrame()
                                                            │
                                                            ▼
                                      sender.SendFrame()
                                      (Arrow serialization → gRPC → Grafana backend)
                                                            │
                                         Grafana Live WebSocket
                                                            │
                                                            ▼
                                                   Browser panel
```

Five constants in `pkg/plugin/plugin.go` govern the entire pipeline:

| Constant                 | Value  | What it controls                                                    |
| ------------------------ | ------ | ------------------------------------------------------------------- |
| `streamMessageBuffer`    | 100    | Fan-in channel capacity. When full, partition goroutines block.     |
| `messageReadTimeout`     | 5 s    | Read deadline per partition. Idle partitions log a debug timeout.   |
| `retryDelayAfterError`   | 100 ms | Pause after a read error. Prevents tight error loops.               |
| `defaultFlattenMaxDepth` | 5      | Max JSON nesting levels before fields are silently dropped.         |
| `defaultFlattenFieldCap` | 1,000  | Max fields per message. Fields beyond the cap are silently dropped. |

The buffered channel is the junction between the parallel-read phase (one goroutine per
partition) and the sequential-process phase (one drain goroutine). Everything that happens
in `ProcessMessageToFrame` and `SendFrame` is single-threaded, regardless of how many
partitions your topic has.

---

## What I Already Knew Would Be Expensive

Before running a single benchmark, I had three architectural choices I was confident would
be the ceiling:

**1. The drain loop is single-threaded.** Every message goes through one goroutine that
calls `ProcessMessageToFrame` and then `sender.SendFrame()` synchronously. This is good for
latency — messages are never reordered, no coordination overhead — but it means adding more
partitions doesn't add more sustained throughput. The drain rate is fixed; more faucets
don't help when the drain is the bottleneck.

**2. There is no batching.** Each Kafka message triggers exactly one `SendFrame` call.
`SendFrame` serializes the frame to Apache Arrow binary format and writes it over gRPC to
the Grafana backend process. That round-trip is the most expensive operation in the
pipeline, and it's paid once per message, not once per batch.

**3. `FieldBuilder` holds a write-lock for every field in every message.** The mutex
protects against concurrent callers, but in the current design only the single drain
goroutine ever calls `AddValueToFrame`. The lock is never contended — it adds ~20–30 ns
per field per message of pure overhead with no concurrency benefit.

These weren't mistakes. They reflect deliberate tradeoffs toward simplicity and correctness.
But they set the ceiling before any measurement began.

---

## The Test Setup

All tests ran on an Intel Core i7-11800H (16 logical cores) under WSL2 on Windows, with
Bitnami Kafka 3.7 (KRaft mode, single broker) and Grafana 11.5.x in Docker.

Everything needed to reproduce these results is in this `benchmark/` folder.

```bash
# 1. Build the plugin binary
mage build

# 2. Start the stack with benchmark resource limits
#    The overlay pins Grafana to 2 CPUs / 1 GB and removes the per-plugin debug
#    log filter that generates thousands of log lines/sec at high throughput.
docker compose -f docker-compose.yaml -f benchmark/docker-compose.bench.yaml up --build -d

# 3. Create the benchmark topics
for P in 1 2 4 8 16; do
  docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic bench-p$P --partitions $P --replication-factor 1
done

# 4. Build the benchmark producer
cd example/go && go build -o bench-producer . && cd ../..
```

The producer in `example/go/` accepts `-rate`, `-msg-size`, and `-duration` flags and uses
`time.Ticker` for rate control. At rates above ~100 msg/s, `time.Sleep`-based approaches
drift meaningfully because they don't account for the time spent constructing and writing
each message. Every message also carries `"ts_ms": <unix_ms>` for end-to-end latency
measurement in Grafana.

> **Important:** Use `-broker localhost:9094` (the `PLAINTEXT_HOST` listener on the Docker
> host). The `kafka:9092` address is only reachable inside the Docker network.

---

## Experiment 1: Finding the Ceiling

The core question: at what produce rate does end-to-end latency start climbing?

I started by measuring the pure CPU cost of the pipeline — everything except `SendFrame`.
The Go micro-benchmark for a 1 KB flat JSON message with 10 fields gives:

```
BenchmarkPipeline_10Fields_1KB   1,244,448 iters   9,585 ns/op   133 MB/s
```

At 9.6 µs per message, the theoretical CPU ceiling (before touching the network) is
**~104,000 msg/s**. That sounds like plenty of headroom.

But `SendFrame` is not included in that number. In a local Docker environment, the gRPC
round-trip to the Grafana backend adds somewhere between 200 µs and 2 ms per message —
between 20× and 200× the CPU cost of everything else. The practical ceiling isn't set by
CPU; it's set by how fast Grafana can consume frames.

Taking the conservative estimate of 500 µs per `SendFrame` call on a warm local Docker
setup, the practical ceiling is **~1,000–2,000 msg/s** for 1 KB JSON with 10 fields:

| Rate        | Expected behavior                                                |
| ----------- | ---------------------------------------------------------------- |
| 10 msg/s    | Zero lag. Latency < 100 ms. Plugin CPU < 2%.                     |
| 100 msg/s   | Zero lag. Latency < 200 ms. Plugin CPU 3–5%.                     |
| 500 msg/s   | No lag. Latency 200–500 ms. CPU 10–20%.                          |
| 1,000 msg/s | Approaching ceiling. Latency begins growing. CPU 30–50%.         |
| 2,000 msg/s | Above ceiling for most configs. Offset lag grows. Latency > 2 s. |

The signal to watch for is the **knee in the latency curve** — the rate at which latency
stops being flat and starts climbing super-linearly. Run these in separate terminals while
producing at your target rate:

```bash
# Track Kafka offset growth (offset derivative ≈ consume rate)
./benchmark/bench-lag-monitor.sh bench-p1 5

# Sample plugin CPU and RSS every 5 seconds
./benchmark/bench-resource-monitor.sh 5

# Produce at target rate (example: 1000 msg/s for 3 minutes)
cd example/go && ./bench-producer \
  -broker localhost:9094 -topic bench-p1 \
  -rate 1000 -msg-size 1024 -duration 180 -shape flat
```

When the offset growth in `bench-lag-monitor.sh` falls below the produce rate, the plugin
is accumulating lag. That's the ceiling.

---

## Experiment 2: The JSON Flattening Surprise

I expected JSON parsing to be fast. It is. What I didn't expect was how much the nesting
depth would dominate the cost.

The plugin flattens nested JSON before building data frames. A message like
`{"host": {"name": "srv-01"}}` becomes `{"host.name": "srv-01"}` via a recursive function.
Here's what that recursion costs at different depths with 10 leaf fields:

| Depth                  | ns/op      | Allocs/op | Cost relative to flat |
| ---------------------- | ---------- | --------- | --------------------- |
| 1 (flat JSON)          | 652        | 3         | 1×                    |
| 3                      | 1,050      | 12        | 1.6×                  |
| **5 (plugin default)** | **23,938** | **320**   | **36.7×**             |

The default `flattenMaxDepth = 5` was chosen defensively — it handles any reasonable JSON
nesting. But most Kafka messages used for monitoring are flat or one level deep: sensor
readings, metrics, log events, Debezium change events. For those schemas, the depth-5
setting pays 36× more than necessary, on every single message.

Field count adds further cost. At depth 3, scaling from 10 to 1,000 fields:

| Fields | ns/op   | Allocs/op |
| ------ | ------- | --------- |
| 10     | 1,050   | 12        |
| 100    | 12,171  | 111       |
| 500    | 69,356  | 519       |
| 1,000  | 142,004 | 1,022     |

At 1,000 fields and 100 msg/s, flattening alone consumes 14.2 ms of CPU per second —
before JSON parsing, frame building, or `SendFrame`.

### The silent field-drop at the cap boundary

There's also a counterintuitive result right at the field-cap limit. At exactly 1,000 fields
(the cap), the function allocates 1,022 objects and takes 142 µs. At 1,001 fields, it
allocates only 20 objects and takes 100 µs — faster with more input, because the cap check
triggers an early-exit path that skips most of the recursive tree.

The danger is that **the 1,001st field is silently dropped, with no log output**. Because
Go's map iteration order is randomized, which field gets dropped is non-deterministic. Your
dashboard simply won't show that field. If your schema regularly exceeds the 1,000-field
cap, you'll lose fields without any indication of why.

**Practical takeaway:** If your Kafka messages are flat or one level deep, lower
`flattenMaxDepth` to 2 or 3. The cost difference between depth-5 and depth-2 is roughly
22×, and it requires no schema changes.

---

## Experiment 3: Does Partitioning Scale Throughput?

In standard Kafka consumer group architecture, more partitions enable more consumer
instances, which scales throughput horizontally. I expected something similar here.

The answer is: **no for sustained throughput, yes for burst tolerance.**

At a fixed total produce rate of 500 msg/s, distributing that load across 1, 4, or 16
partitions does not change how quickly the plugin can drain messages. The single drain
goroutine processes at the same rate regardless of how many goroutines are feeding the
channel. More partitions increases the rate at which messages _arrive at_ the buffer; it
does not increase the drain rate.

Think of it as a bathtub: opening more faucets simultaneously doesn't help if the drain is
already at capacity. What more partitions _do_ provide is burst headroom — the 100-message
buffer can absorb simultaneous bursts from multiple partitions before any goroutine starts
blocking. But once the aggregate produce rate exceeds the drain rate, the buffer fills and
lag accumulates regardless of partition count.

The goroutine count scales predictably with partition count: exactly `partitions + 3`
goroutines per active panel (one per partition reader, one drain, one RunStream, one plugin
main). You can verify this during a load test:

```bash
curl -s http://localhost:6060/debug/pprof/goroutine?debug=1 | grep -c "^goroutine"
```

One practical consequence worth knowing: **opening the same topic in two Grafana panels
creates two independent sets of goroutines.** If you need multiple visualizations of the
same data, use Grafana's panel linking or query sharing instead of duplicating the
datasource query.

---

## Current Limitations

I want to be direct about what the plugin doesn't handle well. Knowing the limits is as
useful as knowing the capabilities.

**The throughput ceiling is ~1,000–2,000 msg/s in a typical Docker environment.** This is
not a Kafka limitation — Kafka can handle millions of messages per second. It's a plugin
architecture limitation. Every message requires one Arrow serialization and one gRPC write
to Grafana. At rates above ~1,000 msg/s, `SendFrame` becomes the bottleneck that tuning
alone cannot bypass without a structural change.

**Throughput does not scale with partition count.** The single drain goroutine is the
ceiling. A 16-partition topic and a 1-partition topic sustain the same message rate through
the plugin, all else equal.

**`streamMessageBuffer` is hardcoded at 100.** You cannot tune burst headroom without
recompiling. For workloads with traffic spikes, the channel fills quickly and partition
goroutines begin blocking on channel sends, which delays acknowledgment to the broker.

**Field drops at the cap are silent and non-deterministic.** Messages with more than 1,000
fields will lose fields with no log output, and which fields disappear is random due to Go
map iteration order. There is currently no way to detect this from the Grafana side other
than noticing a field is absent.

**`FieldBuilder`'s mutex overhead is paid even though it's never contended.** The write-lock
is a precaution for concurrent callers that cannot exist in the current design. At 10 fields
per message and 1,000 msg/s, this is 10,000 lock cycles per second — measurable under
pprof, zero concurrency benefit.

---

## What's Coming

Each limitation above has a clear fix. All four are on the roadmap:

**Message batching.** Accumulate N messages before calling `SendFrame`, instead of one call
per message. Arrow format natively supports multi-row frames, so this is a straightforward
change. Expected impact: raise the practical throughput ceiling 5–20× depending on batch
size.

**Configurable stream buffer.** Expose `streamMessageBuffer` as a datasource configuration
option. Users with bursty topics can raise it to 500–1,000; latency-sensitive setups can
leave it at the default or lower it.

**Remove the FieldBuilder mutex.** Since only one goroutine ever calls `AddValueToFrame` in
the current architecture, the lock serves no purpose. Removing it eliminates ~20–30 ns per
field per message with no correctness tradeoff.

**Concurrent drain.** Replace the shared drain goroutine with one per partition, each with
its own `SendFrame` path. This is the change that would make partition-count scaling
actually matter for throughput.

Contributions are welcome. If any of these resonate with your use case, the codebase is
readable, the Go micro-benchmarks in `pkg/plugin/bench_test.go` are in place to measure
impact, and the profiling tooling below is ready to use:

```bash
# Enable mutex profiling: temporarily add this line to pkg/main.go:
#   runtime.SetMutexProfileFraction(5)
# Then during a load test:

go tool pprof -http :8888 http://localhost:6060/debug/pprof/profile?seconds=30  # CPU
go tool pprof -http :8889 http://localhost:6060/debug/pprof/heap                 # allocations
go tool pprof -http :8890 http://localhost:6060/debug/pprof/mutex                # lock contention
```

---

## Tuning Your Setup Today

Until the architectural changes land, these settings make the most difference:

| Goal                                | Settings                                                                                      |
| ----------------------------------- | --------------------------------------------------------------------------------------------- |
| **Minimize latency**                | `flattenMaxDepth` = 1–3 · `flattenFieldCap` = 50–100 · `GF_LOG_LEVEL` = warn · 1–4 partitions |
| **Maximize burst tolerance**        | Build with a larger `streamMessageBuffer` (rebuild required)                                  |
| **Schema Registry (Avro/Protobuf)** | First-message latency 50–500 ms for schema HTTP fetch; cached for the panel lifetime          |
| **Multiple visualizations**         | One panel per topic; use Grafana panel links for multiple views                               |

The plugin works well for real-time monitoring at 10–500 msg/s with schemas up to 2–3
levels deep and up to ~100 fields. Within those parameters it handles the load comfortably —
single-digit CPU percentage, sub-500 ms end-to-end latency, predictable memory usage.
Outside them, use the numbers above to set expectations.

---

## Running the Micro-Benchmarks Yourself

All Go benchmarks run without a Kafka cluster:

```bash
go test -run=^$ -bench=. -benchmem -benchtime=10s ./pkg/plugin/
```

Key benchmarks and what they reveal:

| Benchmark                                     | What it measures                                             |
| --------------------------------------------- | ------------------------------------------------------------ |
| `BenchmarkFlattenJSON_Depth*`                 | Nesting depth cost — the 36.7× result                        |
| `BenchmarkFlattenJSON_Depth3_Fields*`         | Field count scaling                                          |
| `BenchmarkFlattenJSON_CapBoundary_1001Fields` | Silent drop at cap                                           |
| `BenchmarkDecodeJSON_*`                       | JSON decode throughput by message size                       |
| `BenchmarkFieldBuilder_*`                     | Per-field frame build cost including lock overhead           |
| `BenchmarkPipeline_*`                         | Full pipeline (decode + flatten + frame build, no SendFrame) |
