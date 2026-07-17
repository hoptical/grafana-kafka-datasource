package plugin

import (
	"os"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// TestMain silences the SDK's default logger for the whole test binary.
// Without this, every log.DefaultLogger.Debug(...) call in the production
// code (there are several per processed message) writes a JSON line to
// stdout. That's harmless for normal unit tests, but it makes benchmark
// runs enormously slower and noisier (multi-million-line output for
// benchmarks with a high b.N) since logging I/O then dominates the
// measured cost instead of the code under test. It also demonstrates, as a
// benchmarking side-finding, that per-message Debug logging is itself
// non-trivial overhead in a high-throughput streaming path.
func TestMain(m *testing.M) {
	log.DefaultLogger = log.NewNullLogger()
	os.Exit(m.Run())
}
