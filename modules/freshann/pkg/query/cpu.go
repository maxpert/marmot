package query

import "github.com/klauspost/cpuid/v2"

// SIMDInfo reports runtime CPU capabilities for future optimized kernels.
type SIMDInfo struct {
	AVX2 bool
	SSE2 bool
	NEON bool
}

func DetectSIMD() SIMDInfo {
	cpu := cpuid.CPU
	return SIMDInfo{
		AVX2: cpu.Supports(cpuid.AVX2),
		SSE2: cpu.Supports(cpuid.SSE2),
		NEON: cpu.Supports(cpuid.ASIMD),
	}
}
