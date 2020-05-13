package arrow

import "testing"

func TestScalarEquals(t *testing.T) {
	var a Scalar
	var b Scalar
	a = NewInt64ScalarPrimitive(1)
	b = NewInt64ScalarPrimitive(2)

	a.Equals(b)
}
