package util

import (
	"math/rand"
	"testing"

	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow"
)

type unorderedSet map[int64]struct{}

func MakeDistinctInt64s(nValues int) unorderedSet {
	rd := rand.New(rand.NewSource(42))
	valuesSet := make(unorderedSet)
	for len(valuesSet) < nValues {
		valuesSet[rd.Int63()] = struct{}{}
	}
	return valuesSet
	// valuesArr := make([]int64, nValues)
	// i := 0
	// for k := range valuesSet {
	// 	valuesArr[i] = k
	// 	i++
	// }
	// return valuesArr
}

func MakeSequentialInt64s(nValues int) unorderedSet {
	values := make(unorderedSet)
	for i := 0; i < nValues; i++ {
		values[int64(i)] = struct{}{}
	}
	return values
}

// TODO(nickpoorman): Generate this for all the Scalar types
func CheckScalarHashQualityInt64(t *testing.T, distinctValues unorderedSet) {
	t.Helper()

	hashes := make(map[uint64]struct{})
	for k := range distinctValues {
		h, err := ScalarComputeHash(arrow.NewInt64Scalar(k, nil))
		testutil.AssertNil(t, err)
		hashes[h] = struct{}{}
	}
	testutil.AssertGT(t, len(hashes), int(float64(0.96)*float64(len(distinctValues))))
}

// TODO(nickpoorman): Generate this for all the Scalar types
func TestHashingQualityInt64(t *testing.T) {
	nValues := 10000
	{
		values := MakeDistinctInt64s(nValues)
		CheckScalarHashQualityInt64(t, values)
	}
	{
		values := MakeSequentialInt64s(nValues)
		CheckScalarHashQualityInt64(t, values)
	}
}
