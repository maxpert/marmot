package transformer

import (
	"testing"

	"github.com/maxpert/marmot/publisher"
)

// TestDebeziumTransformerImplementsInterface verifies that DebeziumTransformer
// implements the publisher.Transformer interface at compile time
func TestDebeziumTransformerImplementsInterface(t *testing.T) {
	var _ publisher.Transformer = (*DebeziumTransformer)(nil)
}
