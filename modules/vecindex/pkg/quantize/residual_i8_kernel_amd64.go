//go:build cgo && amd64 && (darwin || linux)

package quantize

import "golang.org/x/sys/cpu"

type residualKernelKind uint8

const (
	residualKernelNone residualKernelKind = iota
	residualKernelAMD64AVX2
	residualKernelAMD64VNNI
	residualKernelARM64Dot
)

var residualSpanKernel = detectResidualSpanKernel()

func detectResidualSpanKernel() residualKernelKind {
	if cpu.X86.HasAVXVNNIInt8 {
		return residualKernelAMD64VNNI
	}
	if cpu.X86.HasAVX2 {
		return residualKernelAMD64AVX2
	}
	return residualKernelNone
}
