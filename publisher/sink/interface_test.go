package sink

import "github.com/maxpert/marmot/publisher"

// Compile-time interface verification
var (
	_ publisher.Sink = (*KafkaSink)(nil)
	_ publisher.Sink = (*MockSink)(nil)
)
