//go:build cgo && (amd64 || arm64) && (darwin || linux)

package quantize

/*
#cgo CFLAGS: -O3
#include "residual_i8_kernel.h"
*/
import "C"

import (
	"unsafe"
)

func scoreResidualInt8SpanSIMD(s *ResidualInt8Scorer, rows []byte, entrySize int, out []float32) bool {
	if s == nil || len(out) == 0 || len(rows) < len(out)*entrySize || residualSpanKernel == residualKernelNone {
		return false
	}
	queryScales := unsafe.Pointer(unsafe.SliceData(s.queryScales))
	queryCodes := unsafe.Pointer(unsafe.SliceData(s.queryCodes))
	rowPtr := unsafe.Pointer(unsafe.SliceData(rows))
	outPtr := unsafe.Pointer(unsafe.SliceData(out))

	switch residualSpanKernel {
	case residualKernelAMD64VNNI:
		if C.marmot_residual_score_span_vnni(
			C.int(s.rankMetric),
			C.float(s.queryNorm2),
			C.float(s.baseDot),
			C.int(s.dim),
			C.int(s.blocks),
			C.int(s.blockSize),
			(*C.float)(queryScales),
			(*C.int8_t)(queryCodes),
			(*C.uint8_t)(rowPtr),
			C.int(entrySize),
			C.int(len(out)),
			(*C.float)(outPtr),
		) != 0 {
			return true
		}
		return C.marmot_residual_score_span_avx2(
			C.int(s.rankMetric),
			C.float(s.queryNorm2),
			C.float(s.baseDot),
			C.int(s.dim),
			C.int(s.blocks),
			C.int(s.blockSize),
			(*C.float)(queryScales),
			(*C.int8_t)(queryCodes),
			(*C.uint8_t)(rowPtr),
			C.int(entrySize),
			C.int(len(out)),
			(*C.float)(outPtr),
		) != 0
	case residualKernelAMD64AVX2:
		return C.marmot_residual_score_span_avx2(
			C.int(s.rankMetric),
			C.float(s.queryNorm2),
			C.float(s.baseDot),
			C.int(s.dim),
			C.int(s.blocks),
			C.int(s.blockSize),
			(*C.float)(queryScales),
			(*C.int8_t)(queryCodes),
			(*C.uint8_t)(rowPtr),
			C.int(entrySize),
			C.int(len(out)),
			(*C.float)(outPtr),
		) != 0
	case residualKernelARM64Dot:
		return C.marmot_residual_score_span_arm64(
			C.int(s.rankMetric),
			C.float(s.queryNorm2),
			C.float(s.baseDot),
			C.int(s.dim),
			C.int(s.blocks),
			C.int(s.blockSize),
			(*C.float)(queryScales),
			(*C.int8_t)(queryCodes),
			(*C.uint8_t)(rowPtr),
			C.int(entrySize),
			C.int(len(out)),
			(*C.float)(outPtr),
		) != 0
	default:
		return false
	}
}
