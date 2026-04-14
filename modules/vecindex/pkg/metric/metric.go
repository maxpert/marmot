package metric

// Metric identifies the distance function used for vector comparisons.
type Metric uint8

const (
	// MetricL2 is squared Euclidean distance.
	MetricL2 Metric = iota
	// MetricDot is negative inner product (higher dot = closer).
	MetricDot
	// MetricCosine is cosine distance (1 - cosine similarity).
	MetricCosine
)
