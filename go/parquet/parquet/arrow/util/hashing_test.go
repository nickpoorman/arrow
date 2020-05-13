package util

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
	"github.com/nickpoorman/arrow-parquet-go/parquet/arrow"
)

type unorderedSetInt64 map[int64]struct{}
type unorderedSetString map[string]struct{}
type unorderedSetHash map[uint64]struct{}

func MakeDistinctInt64s(nValues int) unorderedSetInt64 {
	rd := rand.New(rand.NewSource(42))
	valuesSet := make(unorderedSetInt64)
	for len(valuesSet) < nValues {
		valuesSet[rd.Int63()] = struct{}{}
	}
	return valuesSet
}

func MakeDistinctStrings(nValues int) unorderedSetString {
	const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	randString := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return string(b)
	}
	rdLength := rand.New(rand.NewSource(42))

	valuesSet := make(unorderedSetString)
	for len(valuesSet) < nValues {
		length := rdLength.Intn(25)
		valuesSet[randString(length)] = struct{}{}
	}
	return valuesSet
}

func MakeSequentialInt64s(nValues int) unorderedSetInt64 {
	values := make(unorderedSetInt64)
	for i := 0; i < nValues; i++ {
		values[int64(i)] = struct{}{}
	}
	return values
}

// TODO(nickpoorman): Generate this for all the Scalar types
func CheckScalarHashQualityInt64(t *testing.T, distinctValues unorderedSetInt64) {
	t.Helper()

	hashes := make(unorderedSetHash)
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

func TestHashingConsistent(t *testing.T) {
	for i := int64(0); i < 1000; i++ {
		h1, err := ScalarComputeHash(arrow.NewInt64Scalar(i, nil))
		testutil.AssertNil(t, err)

		h2, err := ScalarComputeHash(arrow.NewInt64Scalar(i, nil))
		testutil.AssertNil(t, err)

		// Hash should be the same when the inputs are the same
		testutil.AssertDeepEq(t, h1, h2)

		h3, err := ScalarComputeHash(arrow.NewInt64Scalar(i+1, nil))
		testutil.AssertNil(t, err)

		// Hash should not be the same when the inputs are different
		testutil.AssertNotDeepEq(t, h1, h3)
	}
}

func TestHashingQualityStrings(t *testing.T) {
	nValues := 10000
	values := MakeDistinctStrings(nValues)

	hashes := make(unorderedSetHash)
	for v := range values {
		h, err := ScalarComputeStringHash([]byte(v))
		testutil.AssertNil(t, err)
		hashes[h] = struct{}{}
	}
	testutil.AssertGT(t, len(hashes), int(float64(0.96)*float64(len(values))))
}

func AssertGet(t *testing.T, table MemoTable, v arrow.Scalar, expected int32) {
	result, err := table.Get(v)
	testutil.AssertNil(t, err)
	testutil.AssertDeepEq(t, result, expected)
}

func AssertGetOrInsert(t *testing.T, table MemoTable, v arrow.Scalar, expected int32) {
	memoIndex, err := table.GetOrInsert(v, nil, nil)
	testutil.AssertNil(t, err)
	testutil.AssertDeepEq(t, memoIndex, expected)
}

func AssertGetNull(t *testing.T, table MemoTable, expected int32) {
	testutil.AssertDeepEq(t, table.GetNull(), expected)
}

func AssertGetOrInsertNull(t *testing.T, table MemoTable, expected int32) {
	testutil.AssertDeepEq(t, table.GetOrInsertNull(nil, nil), expected)
}

// TODO(nickpoorman): Generate this for all the Scalar types
func TestScalarMemoTableInt64(t *testing.T) {
	a := toScalar(1234)
	b := toScalar(0)
	c := toScalar(-98765321)
	d := toScalar(int64(12345678901234))
	e := toScalar(-1)
	f := toScalar(1)
	g := toScalar(int64(9223372036854775807))
	h := toScalar(int64(-9223372036854775807) - 1)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewScalarMemoTable(pool, 0)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	fmt.Println("Insert A")
	AssertGetOrInsert(t, table, a, 0)
	fmt.Println("Done insert")
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsertNull(t, table, 5)

	fmt.Println("Get A")
	AssertGet(t, table, a, 0)
	fmt.Println("Done Get A")
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, e, 4)
	AssertGetOrInsert(t, table, e, 4)

	AssertGetOrInsert(t, table, f, 6)
	AssertGetOrInsert(t, table, g, 7)
	AssertGetOrInsert(t, table, h, 8)

	AssertGetOrInsert(t, table, g, 7)
	AssertGetOrInsert(t, table, f, 6)
	AssertGetOrInsertNull(t, table, 5)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, a, 0)

	size := 9
	testutil.AssertEqInt(t, int(table.Size()), size)
	{
		values := make([]arrow.Scalar, size)
		table.CopyValues(0, -1, values)
		want := []arrow.Scalar{a, b, c, d, e, toScalar(0), f, g, h}
		testutil.AssertDeepEq(t, values, want)
	}
	{
		startOffset := 3
		values := make([]arrow.Scalar, size-startOffset)
		table.CopyValues(int32(startOffset), -1, values)
		want := []arrow.Scalar{d, e, toScalar(0), f, g, h}
		testutil.AssertDeepEq(t, values, want)
	}
}

func toScalar(v interface{}) arrow.Scalar {
	switch v := v.(type) {
	// case int:
	// 	return arrow.NewInt64Scalar(int64(v), nil)
	// case int64:
	// 	return arrow.NewInt64Scalar(int64(v), nil)
	// case int32:
	// 	return arrow.NewInt64Scalar(int64(v), nil)
	case int:
		return arrow.NewInt64Scalar(int64(v), nil)
	case int64:
		return arrow.NewInt64Scalar(v, nil)
	case int32:
		return arrow.NewInt32Scalar(v, nil)
	default:
		panic(fmt.Sprintf("toScalar not implemented for type: %T", v))
	}
}
