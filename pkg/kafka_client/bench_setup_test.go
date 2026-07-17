package kafka_client

import (
	"os"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// TestMain silences the SDK's default logger for the whole test binary.
// See pkg/plugin/bench_setup_test.go for rationale: without this, per-message
// Debug log calls in production code flood stdout during benchmark runs
// (b.N can reach into the millions for fast operations), which both distorts
// timing and produces unusably large output.
func TestMain(m *testing.M) {
	log.DefaultLogger = log.NewNullLogger()
	os.Exit(m.Run())
}
