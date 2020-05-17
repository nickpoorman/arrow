package util

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/nickpoorman/arrow-parquet-go/internal/testutil"
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

func TestScalarMemoTableInt64(t *testing.T) {
	a := toScalar(int64(1234))
	b := toScalar(int64(0))
	c := toScalar(int64(-98765321))
	d := toScalar(int64(12345678901234))
	e := toScalar(int64(-1))
	f := toScalar(int64(1))
	g := toScalar(int64(9223372036854775807))
	h := toScalar(int64(-9223372036854775807) - 1)

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewScalarMemoTable(pool, 0)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsertNull(t, table, 5)

	AssertGet(t, table, a, 0)
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
		want := []arrow.Scalar{a, b, c, d, e, nil, f, g, h}
		assertScalarElementsEq(t, values, want)
	}
	{
		values := make([]arrow.Scalar, size)
		table.CopyValues(0, -1, values)
		want := []arrow.Scalar{a, b, c, d, e, arrow.NewNullScalar(nil), f, g, h}
		assertScalarElementsEq(t, values, want)
	}
	{
		startOffset := 3
		values := make([]arrow.Scalar, size-startOffset)
		table.CopyValues(int32(startOffset), -1, values)
		want := []arrow.Scalar{d, e, nil, f, g, h}
		assertScalarElementsEq(t, values, want)
	}
}

func TestScalarMemoTableUint16(t *testing.T) {
	a := toScalar(uint16(1236))
	b := toScalar(uint16(0))
	c := toScalar(uint16(65535))
	d := toScalar(uint16(32767))
	e := toScalar(uint16(1))

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewScalarMemoTable(pool, 0)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)

	{
		testutil.AssertEqInt(t, int(table.Size()), 4)
		values := make([]arrow.Scalar, table.Size())
		table.CopyValues(0, -1, values)
		want := []arrow.Scalar{a, b, c, d}
		assertScalarElementsEq(t, values, want)
	}

	AssertGetOrInsertNull(t, table, 4)
	AssertGetOrInsert(t, table, e, 5)

	AssertGet(t, table, a, 0)
	AssertGetOrInsert(t, table, a, 0)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetNull(t, table, 4)
	AssertGet(t, table, e, 5)
	AssertGetOrInsert(t, table, e, 5)

	testutil.AssertEqInt(t, int(table.Size()), 6)
	values := make([]arrow.Scalar, table.Size())
	table.CopyValues(0, -1, values)
	want := []arrow.Scalar{a, b, c, d, nil, e}
	assertScalarElementsEq(t, values, want)
}

func TestScalarMemoTableInt8(t *testing.T) {
	a := toScalar(int8(1))
	b := toScalar(int8(0))
	c := toScalar(int8(-1))
	d := toScalar(int8(-128))
	e := toScalar(int8(127))

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewScalarMemoTable(pool, 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsertNull(t, table, 5)

	AssertGet(t, table, a, 0)
	AssertGetOrInsert(t, table, a, 0)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGet(t, table, e, 4)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetNull(t, table, 5)
	AssertGetOrInsertNull(t, table, 5)

	testutil.AssertEqInt(t, int(table.Size()), 6)
	values := make([]arrow.Scalar, table.Size())
	table.CopyValues(0, -1, values)
	want := []arrow.Scalar{a, b, c, d, e, nil}
	assertScalarElementsEq(t, values, want)
}

// TODO(nickpoorman): Benchmark this to see if it's actually any faster
func TestSmallScalarMemoTableBool(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewSmallScalarMemoTable(pool, 0)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, toScalar(true), kKeyNotFound)
	AssertGetOrInsert(t, table, toScalar(true), 0)
	AssertGetOrInsertNull(t, table, 1)
	AssertGetOrInsert(t, table, toScalar(false), 2)

	AssertGet(t, table, toScalar(true), 0)
	AssertGetOrInsert(t, table, toScalar(true), 0)
	AssertGetNull(t, table, 1)
	AssertGetOrInsertNull(t, table, 1)
	AssertGet(t, table, toScalar(false), 2)
	AssertGetOrInsert(t, table, toScalar(false), 2)

	testutil.AssertEqInt(t, int(table.Size()), 3)
	want := []arrow.Scalar{toScalar(true), nil, toScalar(false)}
	values := make([]arrow.Scalar, table.Size())
	table.CopyValues(0, -1, values)
	assertScalarElementsEq(t, values, want)
}

func TestScalarMemoTableFloat64(t *testing.T) {
	a := toScalar(float64(0.0))
	b := toScalar(float64(1.5))
	c := toScalar(float64(-0.1)) // different than C++ as -0.0 isn't a thing
	d := toScalar(float64(math.Inf(1)))
	e := toScalar(float64(math.Inf(-1)))
	f := toScalar(float64(math.NaN()))

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	table := NewScalarMemoTable(pool, 0)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsert(t, table, f, 5)

	AssertGet(t, table, a, 0)
	AssertGetOrInsert(t, table, a, 0)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGet(t, table, e, 4)
	AssertGetOrInsert(t, table, e, 4)
	AssertGet(t, table, f, 5)
	AssertGetOrInsert(t, table, f, 5)

	testutil.AssertEqInt(t, int(table.Size()), 6)
	expected := []arrow.Scalar{a, b, c, d, e, f}
	values := make([]arrow.Scalar, table.Size())
	table.CopyValues(0, -1, values)
	for i := 0; i < len(expected); i++ {
		u := expected[i]
		v := values[i]
		if arrow.ScalarIsNaN(u) {
			testutil.AssertTrue(t, arrow.ScalarIsNaN(v))
		} else {
			testutil.AssertDeepEq(t, u, v)
		}
	}
}

func TestBinaryMemoTableBasics(t *testing.T) {
	a := toScalar("")
	b := toScalar("a")
	c := toScalar("foo")
	d := toScalar("bar")
	e := toScalar("\x00")
	f := toScalar("\x00trailing")

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	// defer pool.AssertSize(t, 0) // Whatever uses this data will release the memory?

	table := NewBinaryMemoTable(pool, 0, -1, arrow.BinaryTypes.String)
	testutil.AssertEqInt(t, int(table.Size()), 0)
	AssertGet(t, table, a, kKeyNotFound)
	AssertGetNull(t, table, kKeyNotFound)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, kKeyNotFound)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGetOrInsert(t, table, f, 5)
	AssertGetOrInsertNull(t, table, 6)

	AssertGet(t, table, a, 0)
	AssertGetOrInsert(t, table, a, 0)
	AssertGet(t, table, b, 1)
	AssertGetOrInsert(t, table, b, 1)
	AssertGetOrInsert(t, table, c, 2)
	AssertGetOrInsert(t, table, d, 3)
	AssertGetOrInsert(t, table, e, 4)
	AssertGet(t, table, f, 5)
	AssertGetOrInsert(t, table, f, 5)
	AssertGetNull(t, table, 6)
	AssertGetOrInsertNull(t, table, 6)

	testutil.AssertEqInt(t, int(table.Size()), 7)
	testutil.AssertEqInt(t, int(table.ValuesSize()), 17)

	size := table.Size()
	{
		offsets := make([]int32, size+1)
		table.CopyOffsets(0, -1, offsets)
		testutil.AssertDeepEq(t, offsets, []int32{0, 0, 1, 4, 7, 8, 17, 17})

		expectedValues := ""
		expectedValues += "afoobar"
		expectedValues += "\x00"
		expectedValues += "\x00"
		expectedValues += "trailing"
		values := []byte(strings.Repeat("X", 17))
		table.CopyValues(0, -1, values)
		testutil.AssertDeepEq(t, values, []byte(expectedValues))
	}
	{
		startOffset := int32(4)
		offsets := make([]int32, size+1-startOffset)
		table.CopyOffsets(startOffset, -1, offsets)
		testutil.AssertDeepEq(t, offsets, []int32{0, 1, 10, 10})

		expectedValues := ""
		expectedValues += "\x00"
		expectedValues += "\x00"
		expectedValues += "trailing"
		values := []byte(strings.Repeat("X", 10))
		table.CopyValues(4 /* start offset */, -1, values)
		testutil.AssertDeepEq(t, values, []byte(expectedValues))
	}
	{
		startOffset := int32(1)
		var actual [][]byte
		table.VisitValues(startOffset, func(v []byte) {
			actual = append(actual, v)
		})
		testutil.AssertDeepEq(t, actual, [][]byte{
			[]byte("a"),
			[]byte("foo"),
			[]byte("bar"),
			[]byte("\x00"),
			[]byte("\x00trailing"),
			[]byte(""),
		})
	}
}

func toScalar(v interface{}) arrow.Scalar {
	switch v := v.(type) {
	case int:
		return arrow.NewInt64Scalar(int64(v), nil)
	case int64:
		return arrow.NewInt64Scalar(v, nil)
	case int32:
		return arrow.NewInt32Scalar(v, nil)
	case uint16:
		return arrow.NewUint16Scalar(v, nil)
	case int8:
		return arrow.NewInt8Scalar(v, nil)
	case bool:
		return arrow.NewBooleanScalar(v, nil)
	case float64:
		return arrow.NewFloat64Scalar(v, nil)
	case string:
		return arrow.NewStringScalarBytes([]byte(v), nil)
	default:
		panic(fmt.Sprintf("toScalar not implemented for type: %T", v))
	}
}

func assertScalarElementsEq(t *testing.T, got, want []arrow.Scalar) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("assertElementsEq: len(got)=%d; len(want)=%d\n", len(got), len(want))
	}
	for i := range got {
		eq, err := arrow.ScalarEquals(got[i], want[i])
		testutil.AssertNil(t, err)
		if !eq {
			t.Errorf("assertScalarElementsEq: got=\n%+v\nwant=\n%+v\n", got, want)
		}
	}
}

func TestNewMemoTables(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

	scalarMemoTable := "*util.ScalarMemoTable"
	smallScalarMemoTable := "*util.SmallScalarMemoTable"
	binaryMemoTable := "*util.BinaryMemoTable"

	cases := []struct {
		dataType arrow.DataType
		want     string
	}{
		{
			arrow.PrimitiveTypes.Int8,
			smallScalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Int16,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Int32,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Int64,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Uint8,
			smallScalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Uint16,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Uint32,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Uint64,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Float32,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Float64,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Date32,
			scalarMemoTable,
		},
		{
			arrow.PrimitiveTypes.Date64,
			scalarMemoTable,
		},
		{
			arrow.Null,
			scalarMemoTable,
		},
		{
			arrow.BinaryTypes.Binary,
			binaryMemoTable,
		},
		{
			arrow.BinaryTypes.String,
			binaryMemoTable,
		},
		{
			arrow.FixedWidthTypes.Boolean,
			smallScalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Date32,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Date64,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.DayTimeInterval,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Duration_s,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Duration_ms,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Duration_us,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Duration_ns,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Float16,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.MonthInterval,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Time32s,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Time32ms,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Time64us,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Time64ns,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Timestamp_s,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Timestamp_ms,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Timestamp_us,
			scalarMemoTable,
		},
		{
			arrow.FixedWidthTypes.Timestamp_ns,
			scalarMemoTable,
		},
		{
			(*arrow.ListType)(nil),
			scalarMemoTable,
		},
		{
			(*arrow.StructType)(nil),
			scalarMemoTable,
		},
	}

	for i, c := range cases {
		table := NewMemoTable(pool, 0, c.dataType)
		got := reflect.TypeOf(table).String()
		if got != c.want {
			t.Errorf("got=%s; want=%s | case[%d]: %#v\n", got, c.want, i, c)
		}
	}
}
