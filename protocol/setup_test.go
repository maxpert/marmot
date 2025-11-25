package protocol

import (
	"os"
	"testing"
)

// TestMain initializes the query pipeline before running any tests
func TestMain(m *testing.M) {
	// Initialize pipeline with default test values
	if err := InitializePipeline(10000, 8); err != nil {
		panic("Failed to initialize query pipeline for tests: " + err.Error())
	}

	// Run all tests
	code := m.Run()

	// Exit with test result code
	os.Exit(code)
}
