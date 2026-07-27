package kafka_client

import (
	"os"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// TestMain silences the SDK's default logger when benchmarks are running.
// See pkg/plugin/bench_setup_test.go for rationale: without this, per-message
// Debug log calls in production code flood stdout during benchmark runs
// (b.N can reach into the millions for fast operations), which both distorts
// timing and produces unusably large output. Scoped to benchmark runs only
// (detected from raw os.Args) so plain `go test` output for unit tests isn't
// silently dropped.
func TestMain(m *testing.M) {
	if benchmarksRequested() {
		log.DefaultLogger = log.NewNullLogger()
	}
	os.Exit(m.Run())
}

// benchmarksRequested reports whether the test binary was invoked with
// -test.bench (i.e. `go test -bench=...`).
func benchmarksRequested() bool {
	for _, arg := range os.Args[1:] {
		if arg == "-test.bench" || strings.HasPrefix(arg, "-test.bench=") {
			return true
		}
	}
	return false
}
