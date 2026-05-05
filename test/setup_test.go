package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestMain initializes the query pipeline before running any tests
func TestMain(m *testing.M) {
	if os.Getenv("MARMOT_RUN_CLUSTER_INTEGRATION_TESTS") != "1" {
		fmt.Fprintln(os.Stderr, "skipping external cluster integration tests; set MARMOT_RUN_CLUSTER_INTEGRATION_TESTS=1 to run")
		os.Exit(0)
	}

	// Initialize pipeline with default test values (nil ID generator for tests)
	if err := protocol.InitializePipeline(10000, nil); err != nil {
		panic("Failed to initialize query pipeline for tests: " + err.Error())
	}

	// Run all tests
	code := m.Run()

	// Exit with test result code
	os.Exit(code)
}
