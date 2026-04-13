package hilbert

// Encode computes the Hilbert curve index for the given coordinates using the
// Skilling (2004) transposed-bits algorithm. Each coordinate must be in
// [0, 2^order - 1]. Returns a big-endian byte slice of length
// ceil(numDims * order / 8) suitable for lexicographic comparison.
func Encode(coords []uint32, order int) []byte {
	n := len(coords)
	if n == 0 || order <= 0 {
		return []byte{}
	}

	// Work on a local copy to avoid mutating caller's slice.
	x := make([]uint32, n)
	copy(x, coords)

	axesToTranspose(x, order)

	return transposeToBytes(x, n, order)
}

// Decode reverses the Hilbert encoding, returning the original coordinates.
// numDims specifies the number of dimensions.
func Decode(key []byte, numDims, order int) []uint32 {
	if numDims == 0 || order <= 0 {
		return []uint32{}
	}

	x := bytesToTranspose(key, numDims, order)
	transposeToAxes(x, order)
	return x
}

// axesToTranspose converts coordinates to the transposed Hilbert representation
// in-place, following the Skilling (2004) algorithm.
func axesToTranspose(x []uint32, order int) {
	n := len(x)
	M := uint32(1) << (order - 1)

	// Inverse undo excess work.
	for Q := M; Q > 1; Q >>= 1 {
		P := Q - 1
		for i := range n {
			if x[i]&Q != 0 {
				x[0] ^= P // invert
			} else {
				t := (x[0] ^ x[i]) & P
				x[0] ^= t
				x[i] ^= t // exchange
			}
		}
	}

	// Gray encode.
	for i := 1; i < n; i++ {
		x[i] ^= x[i-1]
	}
	t := uint32(0)
	for Q := M; Q > 1; Q >>= 1 {
		if x[n-1]&Q != 0 {
			t ^= Q - 1
		}
	}
	for i := range n {
		x[i] ^= t
	}
}

// transposeToAxes converts the transposed Hilbert representation back to
// coordinates in-place; it is the inverse of axesToTranspose.
func transposeToAxes(x []uint32, order int) {
	n := len(x)
	M := uint32(1) << (order - 1)

	// Undo Gray encoding: t XOR (t >> 1) ... inverse is cumulative XOR from MSB.
	t := x[n-1] >> 1
	for i := n - 1; i > 0; i-- {
		x[i] ^= x[i-1]
	}
	x[0] ^= t

	// Undo the inverse-undo-excess work: iterate Q from 2 up to M.
	for Q := uint32(2); Q <= M; Q <<= 1 {
		P := Q - 1
		for i := n - 1; i >= 0; i-- {
			if x[i]&Q != 0 {
				x[0] ^= P // invert
			} else {
				t := (x[0] ^ x[i]) & P
				x[0] ^= t
				x[i] ^= t // exchange
			}
		}
	}
}

// transposeToBytes converts the transposed representation into a big-endian
// byte slice. Bit b of dimension d maps to bit position
// (order-1-b)*numDims + d in the Hilbert key.
func transposeToBytes(x []uint32, numDims, order int) []byte {
	totalBits := numDims * order
	out := make([]byte, (totalBits+7)/8)

	for d := range numDims {
		for b := range order {
			// Bit b of dimension d -> bit position (order-1-b)*numDims + d
			bitPos := (order-1-b)*numDims + d
			if (x[d]>>uint(b))&1 == 1 {
				byteIdx := bitPos / 8
				bitIdx := 7 - (bitPos % 8)
				out[byteIdx] |= 1 << uint(bitIdx)
			}
		}
	}
	return out
}

// bytesToTranspose is the inverse of transposeToBytes: it reconstructs the
// transposed uint32 array from a big-endian byte slice.
func bytesToTranspose(key []byte, numDims, order int) []uint32 {
	x := make([]uint32, numDims)
	for d := range numDims {
		for b := range order {
			bitPos := (order-1-b)*numDims + d
			byteIdx := bitPos / 8
			bitIdx := 7 - (bitPos % 8)
			if byteIdx < len(key) && (key[byteIdx]>>uint(bitIdx))&1 == 1 {
				x[d] |= 1 << uint(b)
			}
		}
	}
	return x
}
