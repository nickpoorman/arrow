package arrow

import "testing"

func TestScalarEquals(t *testing.T) {
	var a EqualityComparable
	var b EqualityComparable
	a = ScalarInt123(1)
	b = ScalarInt123(2)

	a.Equals(b)
}
