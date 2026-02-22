package quant

import "math"

// FP16Encode encodes float32 vector to IEEE-754 binary16.
func FP16Encode(v []float32) []uint16 {
	out := make([]uint16, len(v))
	for i, f := range v {
		out[i] = f32ToF16(f)
	}
	return out
}

// FP16Decode decodes binary16 vector to float32.
func FP16Decode(v []uint16) []float32 {
	out := make([]float32, len(v))
	for i, h := range v {
		out[i] = f16ToF32(h)
	}
	return out
}

func f32ToF16(f float32) uint16 {
	bits := math.Float32bits(f)
	sign := uint16((bits >> 16) & 0x8000)
	exp := int((bits>>23)&0xff) - 127 + 15
	mant := uint16((bits >> 13) & 0x3ff)

	if exp <= 0 {
		if exp < -10 {
			return sign
		}
		mant = mant | 0x400
		return sign | (mant >> uint16(1-exp))
	}
	if exp >= 31 {
		return sign | 0x7c00
	}
	return sign | uint16(exp<<10) | mant
}

func f16ToF32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := int((h >> 10) & 0x1f)
	mant := uint32(h & 0x3ff)

	if exp == 0 {
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		for (mant & 0x400) == 0 {
			mant <<= 1
			exp--
		}
		exp++
		mant &= 0x3ff
	} else if exp == 31 {
		return math.Float32frombits(sign | 0x7f800000 | (mant << 13))
	}

	exp = exp + (127 - 15)
	bits := sign | (uint32(exp) << 23) | (mant << 13)
	return math.Float32frombits(bits)
}
