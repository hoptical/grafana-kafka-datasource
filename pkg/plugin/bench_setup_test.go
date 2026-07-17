package plugin

import (
	"os"
	"strings"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// TestMain silences the SDK's default logger when benchmarks are running.
// Without this, every log.DefaultLogger.Debug(...) call in the production
// code (there are several per processed message) writes a JSON line to
// stdout. That's harmless for normal unit tests, but it makes benchmark
// runs enormously slower and noisier (multi-million-line output for
// benchmarks with a high b.N) since logging I/O then dominates the
// measured cost instead of the code under test. It also demonstrates, as a
// benchmarking side-finding, that per-message Debug logging is itself
// non-trivial overhead in a high-throughput streaming path.
//
// This is scoped to benchmark runs only (detected from raw os.Args, since
// flag.Parse hasn't run yet at this point) so plain `go test` output for
// unit tests - useful when diagnosing a failing test - isn't silently
// dropped.
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
