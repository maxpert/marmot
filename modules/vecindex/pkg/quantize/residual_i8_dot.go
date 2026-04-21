package quantize

func dotInt8(queryCodes []int8, residualCodes []byte) int32 {
	sum := int32(0)
	for i := range queryCodes {
		sum += int32(queryCodes[i]) * int32(int8(residualCodes[i]))
	}
	return sum
}
