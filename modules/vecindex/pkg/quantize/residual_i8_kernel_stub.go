//go:build !cgo || (!amd64 && !arm64) || (!darwin && !linux)

package quantize

func scoreResidualInt8SpanSIMD(_ *ResidualInt8Scorer, _ []byte, _ int, _ []float32) bool {
	return false
}
