// Command loadgen is a high-throughput synthetic Kafka producer used to load
// test the plugin's consume-and-decode path (throughput/latency), separate
// from ../producer.go which is intentionally low-rate (~1-2 msg/s) and meant
// for demoing the plugin in Grafana, not for load testing.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type stats struct {
	attempted uint64
	confirmed uint64
	errors    uint64
	bytes     uint64
}

func main() {
	broker := flag.String("broker", "localhost:9094", "Kafka broker address")
	topic := flag.String("topic", "loadgen", "Kafka topic name")
	format := flag.String("format", "json", "Message format: json, avro, protobuf, lineprotocol, plaintext")
	partitions := flag.Int("num-partitions", 3, "Number of partitions to create the topic with if it does not exist")
	workers := flag.Int("workers", 4, "Number of concurrent producer goroutines")
	rate := flag.Float64("rate", 0, "Target aggregate messages/sec across all workers (0 = max speed)")
	duration := flag.Duration("duration", 30*time.Second, "How long to run (0 = run until Ctrl+C)")
	jsonFields := flag.Int("json-fields", 20, "Extra numeric fields to pad JSON payloads with (format=json only)")
	keyFormat := flag.String("key-format", "string", "Message key format: none or string")
	async := flag.Bool("async", true, "Fire-and-forget async writes; delivery is still confirmed via the Completion callback")
	batchSize := flag.Int("batch-size", 200, "Writer BatchSize (messages per batch)")
	batchTimeout := flag.Duration("batch-timeout", 10*time.Millisecond, "Writer BatchTimeout (max time an incomplete batch waits before flushing)")
	requiredAcksFlag := flag.String("required-acks", "one", "Required broker acks: none, one, or all")
	statsInterval := flag.Duration("stats-interval", 2*time.Second, "Interval between stdout throughput reports")
	connectTimeout := flag.Duration("connect-timeout", 5*time.Second, "Broker connect timeout")
	leaderWaitTimeout := flag.Duration("leader-wait-timeout", 30*time.Second, "Timeout waiting for topic leader election after topic creation")
	flag.Parse()

	switch *format {
	case "json", "avro", "protobuf", "lineprotocol", "plaintext":
	default:
		fmt.Printf("Error: invalid -format %q. Valid options: json, avro, protobuf, lineprotocol, plaintext\n", *format)
		os.Exit(1)
	}
	if *keyFormat != "none" && *keyFormat != "string" {
		fmt.Printf("Error: invalid -key-format %q. Valid options: none, string\n", *keyFormat)
		os.Exit(1)
	}
	var acks kafka.RequiredAcks
	switch *requiredAcksFlag {
	case "none":
		acks = kafka.RequireNone
	case "one":
		acks = kafka.RequireOne
	case "all":
		acks = kafka.RequireAll
	default:
		fmt.Printf("Error: invalid -required-acks %q. Valid options: none, one, all\n", *requiredAcksFlag)
		os.Exit(1)
	}
	if *workers < 1 {
		fmt.Println("Error: -workers must be >= 1")
		os.Exit(1)
	}
	if *statsInterval <= 0 {
		fmt.Println("Error: -stats-interval must be > 0")
		os.Exit(1)
	}

	actualPartitions, err := createTopicIfNotExists(*broker, *topic, *partitions, *connectTimeout)
	if err != nil {
		fmt.Printf("Error: failed to create/verify topic: %v\n", err)
		os.Exit(1)
	}
	if err := waitForTopicLeaders(*broker, *topic, actualPartitions, *leaderWaitTimeout); err != nil {
		fmt.Printf("Error: topic leader not ready: %v\n", err)
		os.Exit(1)
	}

	build, err := newPayloadBuilder(*format, *jsonFields)
	if err != nil {
		fmt.Printf("Error: failed to prepare %s payload builder: %v\n", *format, err)
		os.Exit(1)
	}

	var st stats
	writer := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     &kafka.RoundRobin{},
		Async:        *async,
		BatchSize:    *batchSize,
		BatchTimeout: *batchTimeout,
		RequiredAcks: acks,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				atomic.AddUint64(&st.errors, uint64(len(messages)))
				return
			}
			atomic.AddUint64(&st.confirmed, uint64(len(messages)))
			var n uint64
			for _, m := range messages {
				n += uint64(len(m.Value))
			}
			atomic.AddUint64(&st.bytes, n)
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	fmt.Printf("Starting load generator: format=%s topic=%s workers=%d rate=%s duration=%s async=%v\n",
		*format, *topic, *workers, rateLabel(*rate), durationLabel(*duration), *async)

	var perWorkerInterval time.Duration
	if *rate > 0 {
		perWorkerInterval = time.Duration(float64(time.Second) * float64(*workers) / *rate)
	}

	start := time.Now()
	var seq int64
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runWorker(ctx, writer, build, keyFormat, &seq, &st, perWorkerInterval)
		}()
	}

	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		reportStats(ctx, &st, start, *statsInterval)
	}()

	wg.Wait()
	if err := writer.Close(); err != nil {
		// Close blocks until all pending async Completion callbacks have
		// fired, so confirmed/errors below reflect every attempted send.
		fmt.Printf("failed to close writer: %v\n", err)
	}
	<-statsDone

	elapsed := time.Since(start).Seconds()
	confirmed := atomic.LoadUint64(&st.confirmed)
	errs := atomic.LoadUint64(&st.errors)
	bytesSent := atomic.LoadUint64(&st.bytes)
	fmt.Println("--- summary ---")
	fmt.Printf("elapsed:        %.1fs\n", elapsed)
	fmt.Printf("confirmed sent: %d\n", confirmed)
	fmt.Printf("errors:         %d\n", errs)
	fmt.Printf("throughput:     %.0f msg/s, %.2f MB/s\n", float64(confirmed)/elapsed, float64(bytesSent)/elapsed/(1024*1024))
}

func runWorker(ctx context.Context, writer *kafka.Writer, build payloadFunc, keyFormat *string, seq *int64, st *stats, interval time.Duration) {
	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}
	for {
		if ticker != nil {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		n := atomic.AddInt64(seq, 1)
		payload, err := build(n)
		if err != nil {
			atomic.AddUint64(&st.errors, 1)
			continue
		}
		msg := kafka.Message{Value: payload}
		if *keyFormat == "string" {
			msg.Key = []byte(fmt.Sprintf("key-%d", n))
		}
		atomic.AddUint64(&st.attempted, 1)
		// Delivery errors are counted via the Writer's Completion callback,
		// which fires for every batch regardless of Async mode - counting
		// the error here too would double-count it when WriteMessages also
		// returns it directly (as it does in synchronous mode).
		_ = writer.WriteMessages(ctx, msg)
	}
}

func reportStats(ctx context.Context, st *stats, start time.Time, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastTime := start
	var lastConfirmed, lastBytes uint64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			confirmed := atomic.LoadUint64(&st.confirmed)
			bytesSent := atomic.LoadUint64(&st.bytes)
			errs := atomic.LoadUint64(&st.errors)
			elapsed := now.Sub(lastTime).Seconds()
			rate := float64(confirmed-lastConfirmed) / elapsed
			mbps := float64(bytesSent-lastBytes) / elapsed / (1024 * 1024)
			fmt.Printf("[%6.1fs] confirmed=%d (%.0f msg/s, %.2f MB/s) errors=%d\n",
				now.Sub(start).Seconds(), confirmed, rate, mbps, errs)
			lastConfirmed, lastBytes, lastTime = confirmed, bytesSent, now
		}
	}
}

func rateLabel(rate float64) string {
	if rate <= 0 {
		return "max"
	}
	return fmt.Sprintf("%.0f msg/s", rate)
}

func durationLabel(d time.Duration) string {
	if d <= 0 {
		return "until Ctrl+C"
	}
	return d.String()
}
