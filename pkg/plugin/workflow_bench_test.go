package plugin

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/hoptical/grafana-kafka-datasource/pkg/kafka_client"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// frameSink abstracts the benchmark's "send" stage. Different
// implementations let us isolate pure processing throughput (noop sink)
// versus processing+serialization throughput (real StreamSender.SendFrame).
type frameSink interface {
	SendFrame(*data.Frame) error
}

type noopFrameSink struct {
	frames uint64
}

func (s *noopFrameSink) SendFrame(_ *data.Frame) error {
	atomic.AddUint64(&s.frames, 1)
	return nil
}

type benchPacketSender struct {
	packets uint64
	bytes   uint64
}

func (s *benchPacketSender) Send(packet *backend.StreamPacket) error {
	atomic.AddUint64(&s.packets, 1)
	if packet != nil {
		atomic.AddUint64(&s.bytes, uint64(len(packet.Data)))
	}
	return nil
}

type streamSenderFrameSink struct {
	sender *backend.StreamSender
	sink   *benchPacketSender
}

func newStreamSenderFrameSink() *streamSenderFrameSink {
	packetSink := &benchPacketSender{}
	return &streamSenderFrameSink{
		sender: backend.NewStreamSender(packetSink),
		sink:   packetSink,
	}
}

func (s *streamSenderFrameSink) SendFrame(frame *data.Frame) error {
	return s.sender.SendFrame(frame, data.IncludeAll)
}

type workflowFixture struct {
	name        string
	config      *StreamConfig
	message     kafka_client.KafkaMessage
	payloadSize int
}

func buildWorkflowFixtureJSON(fieldCount int) (workflowFixture, error) {
	payload := buildFlatJSON(fieldCount)
	raw, err := json.Marshal(payload)
	if err != nil {
		return workflowFixture{}, err
	}
	return workflowFixture{
		name: fmt.Sprintf("json_%dfields", fieldCount),
		config: &StreamConfig{
			MessageFormat: "json",
			TimestampMode: "message",
		},
		// Match production flow where ConsumerPull pre-decodes JSON.
		message:     benchKafkaMessage(payload, nil),
		payloadSize: len(raw),
	}, nil
}

func buildWorkflowFixturePlaintext() workflowFixture {
	raw := []byte("2024-01-01T00:00:00Z sensor-01 value=21.5")
	return workflowFixture{
		name: "plaintext",
		config: &StreamConfig{
			MessageFormat: "plaintext",
			TimestampMode: "message",
		},
		message:     benchKafkaMessage(nil, raw),
		payloadSize: len(raw),
	}
}

func buildWorkflowFixtureAvro(b *testing.B) workflowFixture {
	b.Helper()
	codec, err := goavro.NewCodec(benchAvroSchema)
	if err != nil {
		b.Fatalf("failed to build avro codec: %v", err)
	}
	native := map[string]interface{}{"id": "sensor-01", "value": 21.5, "count": int64(42)}
	raw, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		b.Fatalf("failed to encode avro fixture: %v", err)
	}
	return workflowFixture{
		name: "avro",
		config: &StreamConfig{
			MessageFormat:    "avro",
			AvroSchemaSource: "inlineSchema",
			AvroSchema:       benchAvroSchema,
			TimestampMode:    "message",
		},
		message:     benchKafkaMessage(nil, raw),
		payloadSize: len(raw),
	}
}

func buildWorkflowFixtureProtobuf(b *testing.B) workflowFixture {
	b.Helper()
	parsed, err := kafka_client.ParseProtobufSchema(benchProtoSchema)
	if err != nil {
		b.Fatalf("failed to parse protobuf schema: %v", err)
	}
	dm := dynamicpb.NewMessage(parsed.Message)
	dm.Set(parsed.Message.Fields().ByName("id"), protoreflect.ValueOfString("sensor-01"))
	dm.Set(parsed.Message.Fields().ByName("value"), protoreflect.ValueOfFloat64(21.5))
	dm.Set(parsed.Message.Fields().ByName("count"), protoreflect.ValueOfInt64(42))
	raw, err := proto.Marshal(dm)
	if err != nil {
		b.Fatalf("failed to marshal protobuf fixture: %v", err)
	}
	return workflowFixture{
		name: "protobuf",
		config: &StreamConfig{
			MessageFormat:        "protobuf",
			ProtobufSchemaSource: "inlineSchema",
			ProtobufSchema:       benchProtoSchema,
			TimestampMode:        "message",
		},
		message:     benchKafkaMessage(nil, raw),
		payloadSize: len(raw),
	}
}

func buildWorkflowFixtureLineProtocol() workflowFixture {
	raw := buildSyntheticLineProtocol(10, 3, 5)
	return workflowFixture{
		name: "lineprotocol",
		config: &StreamConfig{
			MessageFormat: "lineprotocol",
			TimestampMode: "message",
		},
		message:     benchKafkaMessage(nil, raw),
		payloadSize: len(raw),
	}
}

type workflowBenchOptions struct {
	streamManagerOptions []StreamManagerOption
	microBatchEnabled    bool
}

func BenchmarkWorkflow(b *testing.B) {
	runWorkflowBenchmark(b, workflowBenchOptions{microBatchEnabled: true})
}

func BenchmarkWorkflow_NoOptimizations(b *testing.B) {
	decoder := kafka_client.NewMessageDecoder(kafka_client.MessageDecoderOptions{
		DisableAvroCodecCache:      true,
		DisableProtobufSchemaCache: true,
	})
	runWorkflowBenchmark(b, workflowBenchOptions{
		streamManagerOptions: []StreamManagerOption{
			WithFieldOrderCacheDisabled(),
			WithMessageDecoder(decoder),
		},
		microBatchEnabled: false,
	})
}

func runWorkflowBenchmark(b *testing.B, options workflowBenchOptions) {
	fixtures := []workflowFixture{
		buildWorkflowFixturePlaintext(),
		buildWorkflowFixtureLineProtocol(),
		buildWorkflowFixtureAvro(b),
		buildWorkflowFixtureProtobuf(b),
	}
	json20, err := buildWorkflowFixtureJSON(20)
	if err != nil {
		b.Fatalf("failed to build json20 fixture: %v", err)
	}
	json100, err := buildWorkflowFixtureJSON(100)
	if err != nil {
		b.Fatalf("failed to build json100 fixture: %v", err)
	}
	fixtures = append(fixtures, json20, json100)

	sinkModes := []struct {
		name    string
		newSink func() frameSink
	}{
		{name: "noop", newSink: func() frameSink { return &noopFrameSink{} }},
		{name: "sendframe_json", newSink: func() frameSink { return newStreamSenderFrameSink() }},
	}

	for _, mode := range sinkModes {
		mode := mode
		b.Run(mode.name, func(b *testing.B) {
			for _, fixture := range fixtures {
				fixture := fixture
				b.Run(fixture.name, func(b *testing.B) {
					runWorkflowBenchCase(b, fixture, mode.newSink(), options)
				})
			}
		})
	}
}

func runWorkflowBenchCase(b *testing.B, fixture workflowFixture, sink frameSink, options workflowBenchOptions) {
	sm := newBenchStreamManager(options.streamManagerOptions...)
	partitions := []int32{0}
	msgWithPartition := messageWithPartition{msg: fixture.message, partition: 0}
	messagesCh := make(chan messageWithPartition, streamMessageBuffer)

	var batcher *frameMicroBatcher
	if options.microBatchEnabled {
		if _, ok := sink.(*streamSenderFrameSink); !ok || fixture.config.MessageFormat == "lineprotocol" {
			batcher = nil
		} else {
			batcher = newFrameMicroBatcher(defaultMicroBatchMaxRows)
		}
	}

	b.SetBytes(int64(fixture.payloadSize))
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		messagesCh <- msgWithPartition
		in := <-messagesCh
		frames, err := sm.ProcessMessageFrames(in.msg, in.partition, partitions, fixture.config, "bench-topic")
		if err != nil {
			b.Fatalf("ProcessMessageFrames failed: %v", err)
		}

		if batcher == nil {
			for _, f := range frames {
				if err := sink.SendFrame(f); err != nil {
					b.Fatalf("sink SendFrame failed: %v", err)
				}
			}
			continue
		}

		ready, err := batcher.AddFrames(frames)
		if err != nil {
			b.Fatalf("micro-batcher AddFrames failed: %v", err)
		}
		for _, f := range ready {
			if err := sink.SendFrame(f); err != nil {
				b.Fatalf("sink SendFrame failed after batch flush: %v", err)
			}
		}
	}
	if batcher != nil {
		for _, f := range batcher.Flush() {
			if err := sink.SendFrame(f); err != nil {
				b.Fatalf("sink SendFrame failed during final flush: %v", err)
			}
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
		switch s := sink.(type) {
		case *streamSenderFrameSink:
			packets := atomic.LoadUint64(&s.sink.packets)
			bytes := atomic.LoadUint64(&s.sink.bytes)
			b.ReportMetric(float64(packets)/elapsed.Seconds(), "packets/s")
			b.ReportMetric(float64(bytes)/elapsed.Seconds(), "out_B/s")
		case *noopFrameSink:
			frames := atomic.LoadUint64(&s.frames)
			b.ReportMetric(float64(frames)/elapsed.Seconds(), "frames/s")
		}
	}
}
