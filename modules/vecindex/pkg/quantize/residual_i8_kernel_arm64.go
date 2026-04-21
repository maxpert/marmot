//go:build cgo && arm64 && (darwin || linux)

package quantize

import (
	"runtime"

	"golang.org/x/sys/cpu"
)

type residualKernelKind uint8

const (
	residualKernelNone residualKernelKind = iota
	residualKernelAMD64AVX2
	residualKernelAMD64VNNI
	residualKernelARM64Dot
)

var residualSpanKernel = detectResidualSpanKernel()

func detectResidualSpanKernel() residualKernelKind {
	if runtime.GOOS == "darwin" {
		return residualKernelARM64Dot
	}
	if cpu.ARM64.HasASIMDDP || cpu.ARM64.HasI8MM {
		return residualKernelARM64Dot
	}
	return residualKernelNone
}
