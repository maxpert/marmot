package coordinator

import (
	"os"
	"testing"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol"
)

// TestMain initializes the query pipeline before any test runs.
//
// Without it, statements are parsed in a degraded path: BEGIN never opens a
// transaction and MySQL-dialect syntax is not transpiled, so tests that mean to
// exercise the explicit-transaction path silently run in autocommit and pass
// for the wrong reason.
func TestMain(m *testing.M) {
	if err := protocol.InitializePipeline(10000, id.NewCompactGenerator(1)); err != nil {
		panic("failed to initialize query pipeline for tests: " + err.Error())
	}
	os.Exit(m.Run())
}
