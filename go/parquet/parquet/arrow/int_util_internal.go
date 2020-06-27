package arrow

import "math"

// Detect multiplication overflow between *positive* int64s
func HasMultiplyOverflowInt64(value, multiplicand int64) bool {
	return multiplicand != 0 &&
		value > math.MaxInt64/multiplicand
}

// Detect addition overflow between *positive* int64s
func HasAdditionOverflow(value, addend int64) bool {
	return value > math.MaxInt64-addend
}
